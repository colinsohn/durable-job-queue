package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var ErrConflict = errors.New("idempotency key belongs to a different request")
var ErrNotFound = errors.New("job not found")

type Submission struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type Job struct {
	ID              int64           `json:"id"`
	Type            string          `json:"type"`
	Payload         json.RawMessage `json:"payload"`
	State           string          `json:"state"`
	AttemptsStarted int             `json:"attempts_started"`
	MaxAttempts     int             `json:"max_attempts"`
	CreatedAt       time.Time       `json:"created_at"`
}

type Store struct{ DB *sql.DB }

const jobColumns = `id, job_type, payload::text, state, attempts_started, max_attempts, created_at`

func scanJob(row *sql.Row) (Job, error) {
	var j Job
	var payload []byte
	err := row.Scan(&j.ID, &j.Type, &payload, &j.State, &j.AttemptsStarted, &j.MaxAttempts, &j.CreatedAt)
	j.Payload = payload
	return j, err
}

// Submit uses PostgreSQL's unique key arbitration, including concurrent inserts.
// The second statement gets a fresh READ COMMITTED snapshot after any insert
// conflict wait, so it sees the winning row. JSONB equality ignores object key
// order and whitespace. A replay never changes the original job or its state.
func (s Store) Submit(ctx context.Context, key string, req Submission) (Job, bool, error) {
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback()
	j, err := scanJob(tx.QueryRowContext(ctx, `INSERT INTO queue_jobs (idempotency_key, job_type, payload)
 VALUES ($1, $2, $3::jsonb) ON CONFLICT (idempotency_key) DO NOTHING RETURNING `+jobColumns, key, req.Type, string(req.Payload)))
	created := err == nil
	if errors.Is(err, sql.ErrNoRows) {
		var equivalent bool
		err = tx.QueryRowContext(ctx, `SELECT job_type = $2 AND payload = $3::jsonb FROM queue_jobs WHERE idempotency_key = $1`, key, req.Type, string(req.Payload)).Scan(&equivalent)
		if err != nil {
			return Job{}, false, fmt.Errorf("find submission: %w", err)
		}
		if !equivalent {
			return Job{}, false, ErrConflict
		}
		j, err = scanJob(tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM queue_jobs WHERE idempotency_key = $1`, key))
	}
	if err != nil {
		return Job{}, false, fmt.Errorf("submit job: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return Job{}, false, err
	}
	return j, created, nil
}

func (s Store) Get(ctx context.Context, id int64) (Job, error) {
	j, err := scanJob(s.DB.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM queue_jobs WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return j, err
}
