package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func leaseTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("QUEUE_TEST_DSN")
	if dsn == "" {
		t.Skip("set QUEUE_TEST_DSN for PostgreSQL lease integration tests")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatal("QUEUE_TEST_DSN must be a PostgreSQL URL")
	}
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("queue_lease_%d", time.Now().UnixNano())
	if _, err = admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer admin.Close()
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Error(err)
		}
	})
	q := u.Query()
	q.Set("search_path", schema)
	q.Set("application_name", schema)
	u.RawQuery = q.Encode()
	db, err := sql.Open("postgres", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, name := range []string{"schema.sql", "lease.sql"} {
		ddl, err := os.ReadFile(filepath.Join("..", "..", "db", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(string(ddl)); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func claimForTest(t *testing.T, db *sql.DB, key string) *Claim {
	t.Helper()
	_, _, err := (Store{DB: db}).Submit(context.Background(), key, Submission{Type: "echo", Payload: json.RawMessage(`{"n":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	c, err := ClaimNext(context.Background(), db, "worker-A", time.Minute)
	if err != nil || c == nil {
		t.Fatalf("claim: %v %v", c, err)
	}
	return c
}

func TestLeaseLifecyclePostgres(t *testing.T) {
	db := leaseTestDB(t)
	ctx := context.Background()
	c := claimForTest(t, db, "lifecycle")
	until, err := Renew(ctx, db, c.JobID, c.Token, "worker-A", 2*time.Minute)
	if err != nil || !until.After(c.LeaseUntil) {
		t.Fatalf("renew: %v %v", until, err)
	}
	shorter, err := Renew(ctx, db, c.JobID, c.Token, "worker-A", time.Millisecond)
	if err != nil || !shorter.Equal(until) {
		t.Fatalf("short renewal shortened lease: %v %v", shorter, err)
	}
	var same bool
	if err = db.QueryRow(`SELECT j.lease_expires_at = a.lease_expires_at FROM queue_jobs j JOIN queue_attempts a ON a.claim_token=j.lease_token WHERE j.id=$1`, c.JobID).Scan(&same); err != nil || !same {
		t.Fatalf("attempt expiry mismatch: %v", err)
	}
	for _, bad := range []struct {
		id            int64
		token, worker string
	}{
		{c.JobID, "00000000-0000-0000-0000-000000000000", "worker-A"},
		{c.JobID, c.Token, "worker-B"},
		{999999, c.Token, "worker-A"},
	} {
		if _, err = Renew(ctx, db, bad.id, bad.token, bad.worker, time.Minute); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("unauthorized renew: %v", err)
		}
		if err = Complete(ctx, db, bad.id, bad.token, bad.worker, json.RawMessage(`null`)); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("unauthorized complete: %v", err)
		}
	}
	// Competing completions serialize on the parent lock: exactly one can win.
	var wg sync.WaitGroup
	results := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- Complete(ctx, db, c.JobID, c.Token, "worker-A", json.RawMessage(`{"ok":true}`))
		}()
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrLeaseLost) {
			t.Fatal(err)
		}
	}
	if winners != 1 {
		t.Fatalf("completion winners: %d", winners)
	}
	if _, err = Renew(ctx, db, c.JobID, c.Token, "worker-A", time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("renew after success: %v", err)
	}
	if err = Complete(ctx, db, c.JobID, c.Token, "worker-A", json.RawMessage(`{"overwrite":true}`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("duplicate completion: %v", err)
	}
	var consistent bool
	err = db.QueryRow(`SELECT j.state='succeeded' AND j.result='{"ok":true}'::jsonb
 AND j.lease_token IS NULL AND j.lease_owner IS NULL AND j.lease_expires_at IS NULL
 AND j.finished_at=a.finished_at AND a.state='succeeded' AND j.attempts_started=1
 FROM queue_jobs j JOIN queue_attempts a ON a.job_id=j.id WHERE j.id=$1`, c.JobID).Scan(&consistent)
	if err != nil || !consistent {
		t.Fatalf("inconsistent success: %v", err)
	}
	if next, err := ClaimNext(ctx, db, "worker-B", time.Minute); err != nil || next != nil {
		t.Fatalf("completed job reclaimed: %v %v", next, err)
	}
}

func TestExpiredAndReclaimedLeasePostgres(t *testing.T) {
	db := leaseTestDB(t)
	ctx := context.Background()
	old := claimForTest(t, db, "expiry")
	// Expire the authoritative job record without sleeping; an expired lease is invalid before
	// any sweeper or reclaimer has touched it.
	if _, err := db.Exec(`UPDATE queue_jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, old.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := Renew(ctx, db, old.JobID, old.Token, "worker-A", time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired renew: %v", err)
	}
	if err := Complete(ctx, db, old.JobID, old.Token, "worker-A", json.RawMessage(`null`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired completion: %v", err)
	}
	fresh, err := ClaimNext(ctx, db, "worker-B", time.Minute)
	if err != nil || fresh == nil || fresh.JobID != old.JobID || fresh.Token == old.Token || fresh.Attempt != 2 {
		t.Fatalf("reclaim: %v %v", fresh, err)
	}
	if _, err = Renew(ctx, db, old.JobID, old.Token, "worker-A", time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale renew: %v", err)
	}
	if err = Complete(ctx, db, old.JobID, old.Token, "worker-A", json.RawMessage(`{"stale":true}`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale completion: %v", err)
	}
	if err = Complete(ctx, db, fresh.JobID, fresh.Token, "worker-B", json.RawMessage(`null`)); err != nil {
		t.Fatal(err)
	}
	var states string
	if err = db.QueryRow(`SELECT string_agg(state,',' ORDER BY attempt_no) FROM queue_attempts WHERE job_id=$1`, old.JobID).Scan(&states); err != nil || states != "lease_expired,succeeded" {
		t.Fatalf("history: %s %v", states, err)
	}
}

func TestExpiryWhileWaitingForLockPostgres(t *testing.T) {
	for _, operation := range []string{"renew", "complete"} {
		t.Run(operation, func(t *testing.T) {
			db := leaseTestDB(t)
			c := claimForTest(t, db, "wait")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			// The row is initially valid. Hold its lock so the lifecycle request waits.
			if _, err = tx.ExecContext(ctx, `SELECT id FROM queue_jobs WHERE id=$1 FOR UPDATE`, c.JobID); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				if operation == "renew" {
					_, err := Renew(ctx, db, c.JobID, c.Token, "worker-A", time.Minute)
					done <- err
				} else {
					done <- Complete(ctx, db, c.JobID, c.Token, "worker-A", json.RawMessage(`null`))
				}
			}()
			// Observe a real lock wait before expiring the row; no scheduling assumption.
			for {
				var waiting bool
				err = db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=current_setting('application_name') AND wait_event_type='Lock' AND pid<>pg_backend_pid())`).Scan(&waiting)
				if err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(5 * time.Millisecond):
				}
			}
			if _, err = tx.ExecContext(ctx, `UPDATE queue_jobs SET lease_expires_at=clock_timestamp() WHERE id=$1`, c.JobID); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err = <-done; !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("expired while waiting: %v", err)
			}
			var state string
			if err = db.QueryRow(`SELECT state FROM queue_jobs WHERE id=$1`, c.JobID).Scan(&state); err != nil || state != "leased" {
				t.Fatalf("unexpected mutation: %s %v", state, err)
			}
		})
	}
}

func TestLeaseAtomicRollbackPostgres(t *testing.T) {
	db := leaseTestDB(t)
	ctx := context.Background()
	c := claimForTest(t, db, "rollback")
	// Deliberately corrupt the fixture to force the second table update to fail.
	if _, err := db.Exec(`DELETE FROM queue_attempts WHERE job_id=$1`, c.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := Renew(ctx, db, c.JobID, c.Token, "worker-A", 2*time.Minute); err == nil {
		t.Fatal("missing attempt was not rejected")
	}
	var until time.Time
	if err := db.QueryRow(`SELECT lease_expires_at FROM queue_jobs WHERE id=$1`, c.JobID).Scan(&until); err != nil || !until.Equal(c.LeaseUntil) {
		t.Fatalf("partial renewal: %v %v", until, err)
	}
	if err := Complete(ctx, db, c.JobID, c.Token, "worker-A", json.RawMessage(`null`)); err == nil {
		t.Fatal("missing attempt was not rejected")
	}
	var state string
	if err := db.QueryRow(`SELECT state FROM queue_jobs WHERE id=$1`, c.JobID).Scan(&state); err != nil || state != "leased" {
		t.Fatalf("partial completion: %s %v", state, err)
	}
}
