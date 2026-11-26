package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Diagnostics summarizes queue backlog, active/expired leases, terminal jobs,
// and attempt history from a single database snapshot without exposing tokens.
type Diagnostics struct {
	CheckedAt                time.Time `json:"checked_at"`
	TotalJobs                int       `json:"total_jobs"`
	QueuedReady              int       `json:"queued_ready"`
	QueuedDelayed            int       `json:"queued_delayed"`
	LeasedLive               int       `json:"leased_live"`
	LeasedExpiredReclaimable int       `json:"leased_expired_reclaimable"`
	LeasedExpiredExhausted   int       `json:"leased_expired_exhausted"`
	Succeeded                int       `json:"succeeded"`
	Failed                   int       `json:"failed"`
	RetriedJobs              int       `json:"retried_jobs"`
	OldestReadyAgeSeconds    *float64  `json:"oldest_ready_age_seconds"`
	TotalAttempts            int       `json:"total_attempts"`
	RunningAttempts          int       `json:"running_attempts"`
	SucceededAttempts        int       `json:"succeeded_attempts"`
	FailedAttempts           int       `json:"failed_attempts"`
	ExpiredAttempts          int       `json:"expired_attempts"`
}

const diagnosticsQuery = `
WITH clock AS (
    SELECT clock_timestamp() AS now
),
job_stats AS (
    SELECT
        count(j.id) AS total_jobs,
        count(*) FILTER (WHERE j.state = 'queued' AND j.run_at <= c.now) AS queued_ready,
        count(*) FILTER (WHERE j.state = 'queued' AND j.run_at > c.now) AS queued_delayed,
        count(*) FILTER (WHERE j.state = 'leased' AND j.lease_expires_at > c.now) AS leased_live,
        count(*) FILTER (WHERE j.state = 'leased' AND j.lease_expires_at <= c.now AND j.attempts_started < j.max_attempts) AS leased_expired_reclaimable,
        count(*) FILTER (WHERE j.state = 'leased' AND j.lease_expires_at <= c.now AND j.attempts_started >= j.max_attempts) AS leased_expired_exhausted,
        count(*) FILTER (WHERE j.state = 'succeeded') AS succeeded,
        count(*) FILTER (WHERE j.state = 'failed') AS failed,
        count(*) FILTER (WHERE j.attempts_started > 1) AS retried_jobs,
        max(
            CASE
                WHEN j.state = 'queued' AND j.run_at <= c.now
                    THEN GREATEST(0.0, extract(epoch FROM (c.now - j.run_at))::double precision)
                WHEN j.state = 'leased' AND j.lease_expires_at <= c.now AND j.attempts_started < j.max_attempts
                    THEN GREATEST(0.0, extract(epoch FROM (c.now - j.lease_expires_at))::double precision)
                ELSE NULL
            END
        ) AS oldest_ready_age_seconds
    FROM clock AS c
    LEFT JOIN queue_jobs AS j ON TRUE
),
attempt_stats AS (
    SELECT
        count(*) AS total_attempts,
        count(*) FILTER (WHERE a.state = 'running') AS running_attempts,
        count(*) FILTER (WHERE a.state = 'succeeded') AS succeeded_attempts,
        count(*) FILTER (WHERE a.state = 'failed') AS failed_attempts,
        count(*) FILTER (WHERE a.state = 'lease_expired') AS expired_attempts
    FROM queue_attempts AS a
)
SELECT
    c.now,
    js.total_jobs,
    js.queued_ready,
    js.queued_delayed,
    js.leased_live,
    js.leased_expired_reclaimable,
    js.leased_expired_exhausted,
    js.succeeded,
    js.failed,
    js.retried_jobs,
    js.oldest_ready_age_seconds,
    ats.total_attempts,
    ats.running_attempts,
    ats.succeeded_attempts,
    ats.failed_attempts,
    ats.expired_attempts
FROM clock AS c
CROSS JOIN job_stats AS js
CROSS JOIN attempt_stats AS ats`

func (s Store) Diagnostics(ctx context.Context) (Diagnostics, error) {
	if s.DB == nil {
		return Diagnostics{}, errors.New("database connection is nil")
	}
	var d Diagnostics
	var oldest sql.NullFloat64
	err := s.DB.QueryRowContext(ctx, diagnosticsQuery).Scan(
		&d.CheckedAt,
		&d.TotalJobs,
		&d.QueuedReady,
		&d.QueuedDelayed,
		&d.LeasedLive,
		&d.LeasedExpiredReclaimable,
		&d.LeasedExpiredExhausted,
		&d.Succeeded,
		&d.Failed,
		&d.RetriedJobs,
		&oldest,
		&d.TotalAttempts,
		&d.RunningAttempts,
		&d.SucceededAttempts,
		&d.FailedAttempts,
		&d.ExpiredAttempts,
	)
	if err != nil {
		return Diagnostics{}, fmt.Errorf("diagnostics query: %w", err)
	}
	if oldest.Valid {
		v := oldest.Float64
		d.OldestReadyAgeSeconds = &v
	}
	return d, nil
}
