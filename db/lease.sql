-- Apply after schema.sql (also upgrades a database created before lease methods).
-- All lifecycle writers lock the parent job before its attempt, like queue_claim.
-- Read the database clock AFTER acquiring the lock; a waiting request cannot
-- resurrect a lease that expires while another transaction holds the row.
CREATE OR REPLACE FUNCTION queue_renew(
    p_job BIGINT, p_token UUID, p_worker TEXT, p_lease INTERVAL
) RETURNS TIMESTAMPTZ LANGUAGE plpgsql AS $$
DECLARE
    v_job queue_jobs%ROWTYPE;
    v_now TIMESTAMPTZ;
    v_until TIMESTAMPTZ;
BEGIN
    IF p_lease IS NULL OR p_lease < INTERVAL '1 millisecond' THEN
        RAISE EXCEPTION 'lease must be at least one millisecond';
    END IF;
    SELECT * INTO v_job FROM queue_jobs WHERE id = p_job FOR UPDATE;
    IF NOT FOUND THEN RETURN NULL; END IF;
    v_now := clock_timestamp();
    IF v_job.state <> 'leased' OR p_token IS NULL OR p_worker IS NULL
       OR v_job.lease_token <> p_token OR v_job.lease_owner <> p_worker
       OR v_job.lease_expires_at <= v_now THEN
        RETURN NULL;
    END IF;
    -- Heartbeats extend from now but never shorten an existing lease.
    v_until := greatest(v_job.lease_expires_at, v_now + p_lease);
    UPDATE queue_jobs SET lease_expires_at = v_until, updated_at = v_now
    WHERE id = p_job;
    UPDATE queue_attempts SET lease_expires_at = v_until
    WHERE job_id = p_job AND claim_token = p_token AND state = 'running';
    IF NOT FOUND THEN RAISE EXCEPTION 'running attempt missing for leased job'; END IF;
    RETURN v_until;
END;
$$;

CREATE OR REPLACE FUNCTION queue_complete(
    p_job BIGINT, p_token UUID, p_worker TEXT, p_result JSONB
) RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE
    v_job queue_jobs%ROWTYPE;
    v_now TIMESTAMPTZ;
BEGIN
    IF p_result IS NULL THEN RAISE EXCEPTION 'result must be JSON (JSON null is allowed)'; END IF;
    SELECT * INTO v_job FROM queue_jobs WHERE id = p_job FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    v_now := clock_timestamp();
    IF v_job.state <> 'leased' OR p_token IS NULL OR p_worker IS NULL
       OR v_job.lease_token <> p_token OR v_job.lease_owner <> p_worker
       OR v_job.lease_expires_at <= v_now THEN
        RETURN false;
    END IF;
    UPDATE queue_attempts SET state = 'succeeded', finished_at = v_now
    WHERE job_id = p_job AND claim_token = p_token AND state = 'running';
    IF NOT FOUND THEN RAISE EXCEPTION 'running attempt missing for leased job'; END IF;
    UPDATE queue_jobs
    SET state = 'succeeded', result = p_result, finished_at = v_now,
        updated_at = v_now, lease_token = NULL, lease_owner = NULL,
        lease_expires_at = NULL, last_error = NULL
    WHERE id = p_job;
    RETURN true;
END;
$$;
