package queue

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestDiagnosticsNilDB(t *testing.T) {
	if _, err := (Store{}).Diagnostics(context.Background()); err == nil {
		t.Fatal("expected error for nil database")
	}
}

func TestDiagnosticsAllStatesPostgres(t *testing.T) {
	db := leaseTestDB(t)
	ctx := context.Background()
	store := Store{DB: db}

	empty, err := store.Diagnostics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if empty.CheckedAt.IsZero() || empty.TotalJobs != 0 || empty.TotalAttempts != 0 || empty.OldestReadyAgeSeconds != nil {
		t.Fatalf("unexpected empty diagnostics: %+v", empty)
	}

	// 1. Succeeded on attempt 2 after attempt 1 expired (retried_jobs=1, 1 lease_expired + 1 succeeded attempt).
	cRetried1 := claimForTest(t, db, "diag-retried-success")
	if _, err = db.Exec(`UPDATE queue_jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE id = $1`, cRetried1.JobID); err != nil {
		t.Fatal(err)
	}
	cRetried2, err := ClaimNext(ctx, db, "worker-B", time.Minute)
	if err != nil || cRetried2 == nil || cRetried2.JobID != cRetried1.JobID || cRetried2.Attempt != 2 {
		t.Fatalf("reclaim retried job: %v %v", cRetried2, err)
	}
	if err = Complete(ctx, db, cRetried2.JobID, cRetried2.Token, "worker-B", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}

	// 2. Terminal failed job on attempt 1 (permanent failure).
	cFailed := claimForTest(t, db, "diag-failed")
	if state, err := Fail(ctx, db, cFailed.JobID, cFailed.Token, "worker-A", "permanent", time.Second, time.Minute, true); err != nil || state != "failed" {
		t.Fatalf("permanent fail: %s %v", state, err)
	}

	// 3. Queued but delayed in retry backoff (1 failed attempt, run_at in the future).
	cDelayed := claimForTest(t, db, "diag-delayed")
	if state, err := Fail(ctx, db, cDelayed.JobID, cDelayed.Token, "worker-A", "backoff", time.Hour, 2*time.Hour, false); err != nil || state != "queued" {
		t.Fatalf("fail delayed: %s %v", state, err)
	}

	// 4. Leased and currently live (1 running attempt).
	_ = claimForTest(t, db, "diag-live")

	// 5. Claim two more jobs while no other job is ready, then expire one as reclaimable and one as exhausted.
	cReclaim := claimForTest(t, db, "diag-reclaimable")
	cExhaust := claimForTest(t, db, "diag-exhausted")
	if _, err = db.Exec(`UPDATE queue_jobs SET lease_expires_at = clock_timestamp() - interval '2 seconds' WHERE id = $1`, cReclaim.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE queue_jobs SET max_attempts = 1, lease_expires_at = clock_timestamp() - interval '3 seconds' WHERE id = $1`, cExhaust.JobID); err != nil {
		t.Fatal(err)
	}

	// 6. Queued and immediately ready.
	jReady, _, err := store.Submit(ctx, "diag-ready", Submission{Type: "echo", Payload: json.RawMessage(`{"case":"ready"}`)})
	if err != nil {
		t.Fatal(err)
	}

	d, err := store.Diagnostics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d.TotalJobs != 7 ||
		d.QueuedReady != 1 ||
		d.QueuedDelayed != 1 ||
		d.LeasedLive != 1 ||
		d.LeasedExpiredReclaimable != 1 ||
		d.LeasedExpiredExhausted != 1 ||
		d.Succeeded != 1 ||
		d.Failed != 1 ||
		d.RetriedJobs != 1 {
		t.Fatalf("unexpected job diagnostics: %+v", d)
	}
	if d.OldestReadyAgeSeconds == nil || *d.OldestReadyAgeSeconds < 1.5 {
		t.Fatalf("expected oldest ready age >= 1.5s, got %v", d.OldestReadyAgeSeconds)
	}
	if d.TotalAttempts != 7 ||
		d.RunningAttempts != 3 ||
		d.SucceededAttempts != 1 ||
		d.FailedAttempts != 2 ||
		d.ExpiredAttempts != 1 {
		t.Fatalf("unexpected attempt diagnostics: %+v", d)
	}

	// Next claim sweeps the exhausted lease into terminal failed and claims one ready/reclaimable job;
	// a second claim claims the remaining ready/reclaimable job.
	claim1, err := ClaimNext(ctx, db, "worker-C", time.Minute)
	if err != nil || claim1 == nil {
		t.Fatalf("claim1: %v %v", claim1, err)
	}
	claim2, err := ClaimNext(ctx, db, "worker-C", time.Minute)
	if err != nil || claim2 == nil {
		t.Fatalf("claim2: %v %v", claim2, err)
	}
	claimedIDs := map[int64]bool{claim1.JobID: true, claim2.JobID: true}
	if !claimedIDs[jReady.ID] || !claimedIDs[cReclaim.JobID] {
		t.Fatalf("unexpected claimed IDs: %+v", claimedIDs)
	}
	if err = Complete(ctx, db, claim1.JobID, claim1.Token, "worker-C", json.RawMessage(`null`)); err != nil {
		t.Fatal(err)
	}
	if err = Complete(ctx, db, claim2.JobID, claim2.Token, "worker-C", json.RawMessage(`null`)); err != nil {
		t.Fatal(err)
	}

	after, err := store.Diagnostics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.TotalJobs != 7 ||
		after.QueuedReady != 0 ||
		after.QueuedDelayed != 1 ||
		after.LeasedLive != 1 ||
		after.LeasedExpiredReclaimable != 0 ||
		after.LeasedExpiredExhausted != 0 ||
		after.Succeeded != 3 ||
		after.Failed != 2 ||
		after.RetriedJobs != 2 ||
		after.OldestReadyAgeSeconds != nil {
		t.Fatalf("unexpected post-sweep job diagnostics: %+v", after)
	}
	if after.TotalAttempts != 9 ||
		after.RunningAttempts != 1 ||
		after.SucceededAttempts != 3 ||
		after.FailedAttempts != 2 ||
		after.ExpiredAttempts != 3 {
		t.Fatalf("unexpected post-sweep attempt diagnostics: %+v", after)
	}
}
