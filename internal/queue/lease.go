package queue

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrLeaseLost covers unknown jobs, wrong owners/tokens, expired leases, and
// already completed attempts. A caller must stop writing results after this.
var ErrLeaseLost = errors.New("lease is no longer held")

func validateLease(db *sql.DB, id int64, token, worker string) error {
	if db == nil {
		return errors.New("database connection is nil")
	}
	if id <= 0 || strings.TrimSpace(worker) == "" {
		return errors.New("positive job ID and nonempty worker ID required")
	}
	// Accept the canonical UUID form returned by ClaimNext; reject malformed
	// input before sending it to PostgreSQL's UUID cast.
	if len(token) != 36 || token[8] != '-' || token[13] != '-' || token[18] != '-' || token[23] != '-' {
		return errors.New("claim token must be a UUID")
	}
	compact := strings.ReplaceAll(token, "-", "")
	if len(compact) != 32 {
		return errors.New("claim token must be a UUID")
	}
	if _, err := hex.DecodeString(compact); err != nil {
		return errors.New("claim token must be a UUID")
	}
	return nil
}

// Renew extends a live lease from database time, never shortening its current
// expiry. Both the worker ID and claim token must match. The returned deadline
// is authoritative; caller wall clocks must not be used for fencing.
func Renew(ctx context.Context, db *sql.DB, id int64, token, worker string, lease time.Duration) (time.Time, error) {
	if err := validateLease(db, id, token, worker); err != nil {
		return time.Time{}, err
	}
	if lease < time.Millisecond {
		return time.Time{}, errors.New("lease must be at least one millisecond")
	}
	var until sql.NullTime
	err := db.QueryRowContext(ctx, `SELECT queue_renew($1, $2::uuid, $3, $4::double precision * INTERVAL '1 second')`, id, token, worker, lease.Seconds()).Scan(&until)
	if err != nil {
		return time.Time{}, fmt.Errorf("renew lease: %w", err)
	}
	if !until.Valid {
		return time.Time{}, ErrLeaseLost
	}
	return until.Time, nil
}

// Complete records a successful result and closes the attempt atomically. A
// duplicate completion returns ErrLeaseLost and cannot overwrite the result.
// This only fences queue state, not external side effects.
func Complete(ctx context.Context, db *sql.DB, id int64, token, worker string, result json.RawMessage) error {
	if err := validateLease(db, id, token, worker); err != nil {
		return err
	}
	if !json.Valid(result) {
		return errors.New("result must be valid JSON")
	}
	var completed bool
	err := db.QueryRowContext(ctx, `SELECT queue_complete($1, $2::uuid, $3, $4::jsonb)`, id, token, worker, string(result)).Scan(&completed)
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if !completed {
		return ErrLeaseLost
	}
	return nil
}
