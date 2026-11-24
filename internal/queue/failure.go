package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Fail closes an attempt and returns queued or failed. PostgreSQL determines
// exhaustion from the locked job row, not a caller-supplied attempt counter.
func Fail(ctx context.Context, db *sql.DB, id int64, token, worker, reason string, base, cap time.Duration, permanent bool) (string, error) {
	if err := validateLease(db, id, token, worker); err != nil {
		return "", err
	}
	if base < time.Millisecond || cap < base {
		return "", errors.New("retry delays require one millisecond <= base <= cap")
	}
	reason = strings.ToValidUTF8(reason, "�")
	if strings.TrimSpace(reason) == "" {
		return "", errors.New("failure reason is required")
	}
	runes := []rune(reason)
	if len(runes) > 2000 {
		reason = string(runes[:2000])
	}
	var state sql.NullString
	err := db.QueryRowContext(ctx, `SELECT queue_fail($1,$2::uuid,$3,$4,
 $5::double precision * INTERVAL '1 second',$6::double precision * INTERVAL '1 second',$7)`,
		id, token, worker, reason, base.Seconds(), cap.Seconds(), permanent).Scan(&state)
	if err != nil {
		return "", fmt.Errorf("fail job: %w", err)
	}
	if !state.Valid {
		return "", ErrLeaseLost
	}
	return state.String, nil
}
