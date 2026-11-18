package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Claim is one execution attempt, not a promise of exactly-once delivery.
// A recovered job receives a new Token; old workers must not complete it.
type Claim struct {
	JobID      int64
	Type       string
	Payload    json.RawMessage
	Attempt    int
	Token      string
	LeaseUntil time.Time
}

// ClaimNext atomically claims a due job, or returns (nil, nil) when none exists.
// The caller supplies a database/sql connection with a PostgreSQL driver.
// The queue_claim SQL function also marks expired final attempts as failed.
func ClaimNext(ctx context.Context, db *sql.DB, workerID string, lease time.Duration) (*Claim, error) {
	if strings.TrimSpace(workerID) == "" {
		return nil, errors.New("worker ID must be nonempty")
	}
	if lease < time.Millisecond {
		return nil, errors.New("lease must be at least one millisecond")
	}
	if db == nil {
		return nil, errors.New("database connection is nil")
	}

	var claim Claim
	// Passing seconds as a number avoids interval-string parsing ambiguity.
	const query = `SELECT job_id, job_type, payload::text, attempt_no, claim_token::text, lease_until
FROM queue_claim($1, $2::double precision * INTERVAL '1 second')`
	err := db.QueryRowContext(ctx, query, workerID, lease.Seconds()).Scan(
		&claim.JobID, &claim.Type, &claim.Payload, &claim.Attempt, &claim.Token, &claim.LeaseUntil,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim next job: %w", err)
	}
	return &claim, nil
}
