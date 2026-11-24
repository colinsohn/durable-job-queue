package queue

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestFailureRetriesAndExhaustionPostgres(t *testing.T) {
	db := leaseTestDB(t)
	ctx := context.Background()
	c := claimForTest(t, db, "failures")
	const base = 100 * time.Millisecond
	const cap = 150 * time.Millisecond
	for attempt := 1; attempt <= 3; attempt++ {
		state, err := Fail(ctx, db, c.JobID, c.Token, "worker-A", "temporary failure", base, cap, false)
		if err != nil {
			t.Fatal(err)
		}
		want := "queued"
		if attempt == 3 {
			want = "failed"
		}
		if state != want {
			t.Fatalf("attempt %d state %s", attempt, state)
		}
		j, err := (Store{DB: db}).Get(ctx, c.JobID)
		if err != nil || j.LastError == nil || *j.LastError != "temporary failure" || j.AttemptsStarted != attempt {
			t.Fatalf("status: %+v %v", j, err)
		}
		if (j.FinishedAt != nil) != (attempt == 3) {
			t.Fatal("wrong terminal timestamp")
		}
		var closed bool
		if err = db.QueryRow(`SELECT state='failed' AND finished_at IS NOT NULL FROM queue_attempts WHERE claim_token=$1`, c.Token).Scan(&closed); err != nil || !closed {
			t.Fatalf("attempt not closed: %v", err)
		}
		if _, err = Fail(ctx, db, c.JobID, c.Token, "worker-A", "overwrite", base, cap, false); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("duplicate failure: %v", err)
		}
		if next, err := ClaimNext(ctx, db, "too-early", time.Minute); err != nil || next != nil {
			t.Fatalf("premature claim: %v %v", next, err)
		}
		if attempt < 3 {
			var delay float64
			if err = db.QueryRow(`SELECT extract(epoch FROM run_at-updated_at) FROM queue_jobs WHERE id=$1`, c.JobID).Scan(&delay); err != nil {
				t.Fatal(err)
			}
			expected := base.Seconds()
			if attempt == 2 {
				expected = cap.Seconds()
			}
			if delay != expected {
				t.Fatalf("delay %.6f want %.6f", delay, expected)
			}
			if _, err = db.Exec(`UPDATE queue_jobs SET run_at=clock_timestamp()-interval '1 second' WHERE id=$1`, c.JobID); err != nil {
				t.Fatal(err)
			}
			old := c
			c, err = ClaimNext(ctx, db, "worker-A", time.Minute)
			if err != nil || c == nil || c.Attempt != attempt+1 || c.Token == old.Token {
				t.Fatalf("retry claim: %v %v", c, err)
			}
			if _, err = Fail(ctx, db, old.JobID, old.Token, "worker-A", "stale", base, cap, false); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("stale failure: %v", err)
			}
		}
	}
	var attempts int
	if err := db.QueryRow(`SELECT count(*) FROM queue_attempts WHERE job_id=$1`, c.JobID).Scan(&attempts); err != nil || attempts != 3 {
		t.Fatalf("history: %d %v", attempts, err)
	}
}

func TestPermanentExpiredAndAtomicFailurePostgres(t *testing.T) {
	db := leaseTestDB(t)
	ctx := context.Background()
	c := claimForTest(t, db, "permanent")
	reason := strings.Repeat("失", 2100)
	state, err := Fail(ctx, db, c.JobID, c.Token, "worker-A", reason, time.Second, time.Minute, true)
	if err != nil || state != "failed" {
		t.Fatalf("permanent failure %s %v", state, err)
	}
	var length, attempts int
	if err = db.QueryRow(`SELECT length(last_error),attempts_started FROM queue_jobs WHERE id=$1`, c.JobID).Scan(&length, &attempts); err != nil || length != 2000 || attempts != 1 {
		t.Fatalf("bounded reason/exhaustion: %d %d %v", length, attempts, err)
	}
	c = claimForTest(t, db, "atomic")
	if _, err = db.Exec(`DELETE FROM queue_attempts WHERE job_id=$1`, c.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err = Fail(ctx, db, c.JobID, c.Token, "worker-A", "failure", time.Second, time.Minute, false); err == nil {
		t.Fatal("missing attempt not rejected")
	}
	var jobState string
	if err = db.QueryRow(`SELECT state FROM queue_jobs WHERE id=$1`, c.JobID).Scan(&jobState); err != nil || jobState != "leased" {
		t.Fatalf("partial failure: %s %v", jobState, err)
	}
	if _, err = db.Exec(`UPDATE queue_jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, c.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err = Fail(ctx, db, c.JobID, c.Token, "worker-A", "expired", time.Second, time.Minute, false); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired failure: %v", err)
	}
}
