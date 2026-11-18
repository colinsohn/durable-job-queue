-- PostgreSQL 13+. A job can be claimed at least once; its external side effects
-- must be idempotent because the lease can expire while a worker is still alive.
CREATE TABLE queue_jobs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    job_type TEXT NOT NULL CHECK (length(job_type) > 0),
    payload JSONB NOT NULL,
    state TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'leased', 'succeeded', 'failed')),
    priority INTEGER NOT NULL DEFAULT 0,
    run_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    attempts_started INTEGER NOT NULL DEFAULT 0 CHECK (attempts_started >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
    lease_token UUID,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    result JSONB,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    CONSTRAINT attempts_within_limit CHECK (attempts_started <= max_attempts),
    CONSTRAINT lease_fields_together CHECK (
        (state = 'leased' AND lease_token IS NOT NULL
            AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (state <> 'leased' AND lease_token IS NULL
            AND lease_owner IS NULL AND lease_expires_at IS NULL)
    )
);

CREATE INDEX queue_jobs_ready_idx
    ON queue_jobs (priority DESC, run_at, id)
    WHERE state = 'queued';
CREATE INDEX queue_jobs_expiring_idx
    ON queue_jobs (lease_expires_at, id)
    WHERE state = 'leased';

CREATE TABLE queue_attempts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES queue_jobs(id),
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    claim_token UUID NOT NULL UNIQUE,
    worker_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'running'
        CHECK (state IN ('running', 'succeeded', 'failed', 'lease_expired')),
    claimed_at TIMESTAMPTZ NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    last_error TEXT,
    UNIQUE (job_id, attempt_no),
    CONSTRAINT attempt_finished_check CHECK (
        (state = 'running' AND finished_at IS NULL)
        OR (state <> 'running' AND finished_at IS NOT NULL)
    )
);

CREATE INDEX queue_attempts_job_idx ON queue_attempts (job_id, attempt_no DESC);

-- The entire function executes as one transaction, even when called as a
-- single SELECT. FOR UPDATE SKIP LOCKED avoids duplicate claims under races.
-- An expired final attempt becomes terminal even if no other job is ready.
CREATE FUNCTION queue_claim(p_worker TEXT, p_lease INTERVAL)
RETURNS TABLE (
    job_id BIGINT,
    job_type TEXT,
    payload JSONB,
    attempt_no INTEGER,
    claim_token UUID,
    lease_until TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := clock_timestamp();
    v_job queue_jobs%ROWTYPE;
    v_token UUID;
BEGIN
    IF p_worker IS NULL OR btrim(p_worker) = '' THEN
        RAISE EXCEPTION 'worker ID must be nonempty';
    END IF;
    IF p_lease IS NULL OR p_lease <= INTERVAL '0 seconds' THEN
        RAISE EXCEPTION 'lease must be positive';
    END IF;

    -- Sweep a bounded batch of final expired leases to avoid stranded jobs.
    -- Locked rows are skipped so multiple sweepers can safely coexist.
    WITH expired AS (
        SELECT j.id, j.attempts_started
        FROM queue_jobs AS j
        WHERE j.state = 'leased'
          AND j.lease_expires_at <= v_now
          AND j.attempts_started >= j.max_attempts
        ORDER BY j.lease_expires_at, j.id
        LIMIT 100
        FOR UPDATE SKIP LOCKED
    ), terminal AS (
        UPDATE queue_jobs AS j
        SET state = 'failed', lease_token = NULL, lease_owner = NULL,
            lease_expires_at = NULL, finished_at = v_now,
            last_error = 'lease expired after final attempt', updated_at = v_now
        FROM expired AS e
        WHERE j.id = e.id
        RETURNING j.id, e.attempts_started
    )
    UPDATE queue_attempts AS a
    SET state = 'lease_expired', finished_at = v_now,
        last_error = 'worker lease expired'
    FROM terminal AS t
    WHERE a.job_id = t.id AND a.attempt_no = t.attempts_started
      AND a.state = 'running';

    SELECT j.* INTO v_job
    FROM queue_jobs AS j
    WHERE (j.state = 'queued' AND j.run_at <= v_now)
       OR (j.state = 'leased' AND j.lease_expires_at <= v_now
           AND j.attempts_started < j.max_attempts)
    ORDER BY j.priority DESC, j.run_at ASC, j.id ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Close the old attempt only after locking its parent job.
    IF v_job.state = 'leased' THEN
        UPDATE queue_attempts AS a
        SET state = 'lease_expired', finished_at = v_now,
            last_error = 'worker lease expired'
        WHERE a.job_id = v_job.id
          AND a.attempt_no = v_job.attempts_started
          AND a.state = 'running';
    END IF;

    v_token := gen_random_uuid();
    UPDATE queue_jobs AS j
    SET state = 'leased', attempts_started = v_job.attempts_started + 1,
        lease_token = v_token, lease_owner = p_worker,
        lease_expires_at = v_now + p_lease, updated_at = v_now
    WHERE j.id = v_job.id;

    INSERT INTO queue_attempts (
        job_id, attempt_no, claim_token, worker_id, claimed_at, lease_expires_at
    ) VALUES (
        v_job.id, v_job.attempts_started + 1, v_token,
        p_worker, v_now, v_now + p_lease
    );

    RETURN QUERY SELECT v_job.id, v_job.job_type, v_job.payload,
                        v_job.attempts_started + 1, v_token, v_now + p_lease;
END;
$$;
