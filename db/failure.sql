-- Apply after schema.sql and lease.sql; safe to reapply to existing tables.
CREATE OR REPLACE FUNCTION queue_fail(
    p_job BIGINT, p_token UUID, p_worker TEXT, p_error TEXT,
    p_base INTERVAL, p_cap INTERVAL, p_permanent BOOLEAN
) RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE
    v_job queue_jobs%ROWTYPE;
    v_now TIMESTAMPTZ;
    v_delay INTERVAL;
    v_state TEXT;
    v_error TEXT;
    v_i INTEGER;
BEGIN
    IF p_base IS NULL OR p_base < INTERVAL '1 millisecond'
       OR p_cap IS NULL OR p_cap < p_base THEN
        RAISE EXCEPTION 'retry delays require 1 millisecond <= base <= cap';
    END IF;
    IF p_error IS NULL OR btrim(p_error) = '' OR p_permanent IS NULL THEN
        RAISE EXCEPTION 'failure reason and permanent flag required';
    END IF;
    v_error := left(p_error, 2000);
    -- Lock ordering and clock fencing match renewal/completion.
    SELECT * INTO v_job FROM queue_jobs WHERE id = p_job FOR UPDATE;
    IF NOT FOUND THEN RETURN NULL; END IF;
    v_now := clock_timestamp();
    IF v_job.state <> 'leased' OR p_token IS NULL OR p_worker IS NULL
       OR v_job.lease_token <> p_token OR v_job.lease_owner <> p_worker
       OR v_job.lease_expires_at <= v_now THEN
        RETURN NULL;
    END IF;
    v_state := 'failed';
    v_delay := p_base;
    IF NOT p_permanent AND v_job.attempts_started < v_job.max_attempts THEN
        v_state := 'queued';
        -- Failed attempt n waits min(cap, base * 2^(n-1)). Exit at the
        -- cap instead of looping through a potentially huge attempt number.
        FOR v_i IN 1..(v_job.attempts_started - 1) LOOP
            EXIT WHEN v_delay >= p_cap;
            IF v_delay >= p_cap / 2 THEN v_delay := p_cap;
            ELSE v_delay := v_delay * 2; END IF;
        END LOOP;
    END IF;
    UPDATE queue_attempts SET state = 'failed', finished_at = v_now, last_error = v_error
    WHERE job_id = p_job AND claim_token = p_token AND state = 'running';
    IF NOT FOUND THEN RAISE EXCEPTION 'running attempt missing for leased job'; END IF;
    UPDATE queue_jobs SET state = v_state, last_error = v_error, result = NULL,
        updated_at = v_now, finished_at = CASE WHEN v_state = 'failed' THEN v_now ELSE NULL END,
        run_at = CASE WHEN v_state = 'queued' THEN v_now + v_delay ELSE run_at END,
        lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
    WHERE id = p_job;
    RETURN v_state;
END;
$$;
