package queue

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestFinalExpiryBatchAndReadyWorkPostgres(t *testing.T) {
	db := leaseTestDB(t)
	ctx := context.Background()
	_, err := db.Exec(`WITH jobs AS (
  INSERT INTO queue_jobs(idempotency_key,job_type,payload,state,attempts_started,max_attempts,lease_token,lease_owner,lease_expires_at)
  SELECT 'batch-expired-'||n,'echo','{}'::jsonb,'leased',1,1,gen_random_uuid(),'dead-worker',clock_timestamp()-interval '1 second'
  FROM generate_series(1,105) AS n RETURNING id,lease_token,lease_expires_at
 ) INSERT INTO queue_attempts(job_id,attempt_no,claim_token,worker_id,claimed_at,lease_expires_at)
 SELECT id,1,lease_token,'dead-worker',lease_expires_at-interval '1 second',lease_expires_at FROM jobs`)
	if err != nil {
		t.Fatal(err)
	}
	ready, _, err := (Store{DB: db}).Submit(ctx, "ready-amid-final-expiries", Submission{Type: "echo", Payload: json.RawMessage(`{"ready":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	c, err := ClaimNext(ctx, db, "survivor", time.Minute)
	if err != nil || c == nil || c.JobID != ready.ID {
		t.Fatalf("ready work starved: %v %v", c, err)
	}
	var failed, leased int
	if err = db.QueryRow(`SELECT count(*) FILTER(WHERE state='failed'),count(*) FILTER(WHERE state='leased') FROM queue_jobs WHERE idempotency_key LIKE 'batch-expired-%'`).Scan(&failed, &leased); err != nil || failed != 100 || leased != 5 {
		t.Fatalf("bounded first sweep: %d %d %v", failed, leased, err)
	}
	if err = Complete(ctx, db, c.JobID, c.Token, "survivor", json.RawMessage(`null`)); err != nil {
		t.Fatal(err)
	}
	if c, err = ClaimNext(ctx, db, "survivor", time.Minute); err != nil || c != nil {
		t.Fatalf("extra attempt after final expiry: %v %v", c, err)
	}
	var closed, extra int
	if err = db.QueryRow(`SELECT count(*) FILTER(WHERE state='lease_expired' AND finished_at IS NOT NULL),count(*) FILTER(WHERE attempt_no>1) FROM queue_attempts WHERE worker_id='dead-worker'`).Scan(&closed, &extra); err != nil || closed != 105 || extra != 0 {
		t.Fatalf("batch history: %d %d %v", closed, extra, err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM queue_jobs WHERE idempotency_key LIKE 'batch-expired-%' AND state='failed' AND finished_at IS NOT NULL AND lease_token IS NULL AND lease_owner IS NULL AND lease_expires_at IS NULL AND attempts_started=1 AND max_attempts=1`).Scan(&failed); err != nil || failed != 105 {
		t.Fatalf("stranded exhausted jobs: %d %v", failed, err)
	}
}

func TestFinalExpiryCannotBeResurrectedPostgres(t *testing.T) {
	db := leaseTestDB(t)
	ctx := context.Background()
	c := claimForTest(t, db, "last-attempt")
	if _, err := db.Exec(`UPDATE queue_jobs SET max_attempts=1,lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, c.JobID); err != nil {
		t.Fatal(err)
	}
	reject := func() {
		t.Helper()
		if _, err := Renew(ctx, db, c.JobID, c.Token, "worker-A", time.Minute); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("late renew: %v", err)
		}
		if err := Complete(ctx, db, c.JobID, c.Token, "worker-A", json.RawMessage(`{"late":true}`)); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("late completion: %v", err)
		}
		if _, err := Fail(ctx, db, c.JobID, c.Token, "worker-A", "late retry", time.Millisecond, time.Second, false); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("late retry: %v", err)
		}
	}
	reject()
	for i := 0; i < 3; i++ {
		if next, err := ClaimNext(ctx, db, "recovery-worker", time.Minute); err != nil || next != nil {
			t.Fatalf("exhausted job reclaimed: %v %v", next, err)
		}
	}
	reject()
	j, err := (Store{DB: db}).Get(ctx, c.JobID)
	if err != nil || j.State != "failed" || j.AttemptsStarted != 1 || j.FinishedAt == nil || j.LastError == nil || *j.LastError != "lease expired after final attempt" {
		t.Fatalf("terminal status: %+v %v", j, err)
	}
	var count int
	if err = db.QueryRow(`SELECT count(*) FROM queue_attempts WHERE job_id=$1 AND state='lease_expired' AND finished_at IS NOT NULL`, c.JobID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("terminal history: %d %v", count, err)
	}
}
