package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"durable-job-queue/internal/queue"
	_ "github.com/lib/pq"
)

func workerDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("QUEUE_TEST_DSN")
	if dsn == "" {
		t.Skip("set QUEUE_TEST_DSN for PostgreSQL worker tests")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("queue_worker_%d", time.Now().UnixNano())
	if _, err = admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
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
	u.RawQuery = q.Encode()
	db, err := sql.Open("postgres", u.String())
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { db.Close() })
	for _, name := range []string{"schema.sql", "lease.sql", "failure.sql"} {
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

func testConfig() Config {
	c := DefaultConfig("test-worker")
	c.Lease = 500 * time.Millisecond
	c.Heartbeat = 80 * time.Millisecond
	c.OperationTimeout = 150 * time.Millisecond
	c.ShutdownGrace = 200 * time.Millisecond
	c.Poll = 5 * time.Millisecond
	c.RetryBase = 30 * time.Millisecond
	c.RetryCap = 50 * time.Millisecond
	return c
}

func submit(t *testing.T, db *sql.DB, key string) int64 {
	t.Helper()
	j, _, err := (queue.Store{DB: db}).Submit(context.Background(), key, queue.Submission{Type: "echo", Payload: json.RawMessage(`{"message":"hello"}`)})
	if err != nil {
		t.Fatal(err)
	}
	return j.ID
}

func awaitState(t *testing.T, db *sql.DB, id int64, want string) queue.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, err := (queue.Store{DB: db}).Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if j.State == want {
			return j
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %d did not become %s", id, want)
	return queue.Job{}
}

func start(t *testing.T, w Worker) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Error("worker did not stop")
		}
	})
	return cancel, done
}

func TestWorkerEchoHeartbeatsPostgres(t *testing.T) {
	db := workerDB(t)
	id := submit(t, db, "heartbeat")
	cfg := testConfig()
	w := Worker{DB: db, Config: cfg, Handlers: map[string]Handler{"echo": Echo(900 * time.Millisecond)}}
	cancel, done := start(t, w)
	j := awaitState(t, db, id, "succeeded")
	if j.AttemptsStarted != 1 || string(j.Result) != `{"message": "hello"}` || j.FinishedAt == nil {
		t.Fatalf("echo status: %+v", j)
	}
	var renewed bool
	if err := db.QueryRow(`SELECT lease_expires_at > claimed_at + interval '500 milliseconds' FROM queue_attempts WHERE job_id=$1`, id).Scan(&renewed); err != nil || !renewed {
		t.Fatalf("heartbeat not renewed: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWorkerRetriesPermanentAndPanicPostgres(t *testing.T) {
	for _, mode := range []string{"retry_success", "exhausted", "permanent", "panic", "invalid_json", "unsupported", "empty_reason"} {
		t.Run(mode, func(t *testing.T) {
			db := workerDB(t)
			id := submit(t, db, mode)
			cfg := testConfig()
			handler := func(ctx context.Context, c *queue.Claim) (json.RawMessage, error) {
				switch mode {
				case "retry_success":
					if c.Attempt == 3 {
						return c.Payload, nil
					}
				case "permanent":
					return nil, fmt.Errorf("%w: invalid job", ErrPermanent)
				case "panic":
					panic("do not expose this value")
				case "invalid_json":
					return nil, nil
				case "empty_reason":
					return nil, errors.New("")
				}
				return nil, errors.New("temporary failure")
			}
			handlers := map[string]Handler{"echo": handler}
			if mode == "unsupported" {
				handlers = nil
			}
			cancel, done := start(t, Worker{DB: db, Config: cfg, Handlers: handlers})
			want := "failed"
			if mode == "retry_success" {
				want = "succeeded"
			}
			j := awaitState(t, db, id, want)
			attempts := 3
			if mode == "permanent" || mode == "invalid_json" || mode == "unsupported" {
				attempts = 1
			}
			if j.AttemptsStarted != attempts {
				t.Fatalf("attempts: %d", j.AttemptsStarted)
			}
			if mode == "panic" && (j.LastError == nil || *j.LastError != "handler panicked") {
				t.Fatalf("unsafe panic reason: %+v", j.LastError)
			}
			if mode == "empty_reason" && (j.LastError == nil || *j.LastError != "handler failed without a message") {
				t.Fatal("empty failure reason was not normalized")
			}
			var early int
			if err := db.QueryRow(`SELECT count(*) FROM queue_attempts a JOIN queue_attempts b ON a.job_id=b.job_id AND b.attempt_no=a.attempt_no+1
   WHERE a.job_id=$1 AND b.claimed_at < a.finished_at + CASE WHEN a.attempt_no=1 THEN interval '30 milliseconds' ELSE interval '50 milliseconds' END`, id).Scan(&early); err != nil || early != 0 {
				t.Fatalf("early retry: %d %v", early, err)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkerShutdownDrainPostgres(t *testing.T) {
	db := workerDB(t)
	first := submit(t, db, "drain-first")
	second := submit(t, db, "drain-second")
	cfg := testConfig()
	claimed := make(chan struct{})
	release := make(chan struct{})
	handler := func(ctx context.Context, c *queue.Claim) (json.RawMessage, error) {
		close(claimed)
		select {
		case <-release:
			return c.Payload, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	cancel, done := start(t, Worker{DB: db, Config: cfg, Handlers: map[string]Handler{"echo": handler}})
	select {
	case <-claimed:
	case <-time.After(3 * time.Second):
		t.Fatal("no claim")
	}
	cancel()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if j := awaitState(t, db, first, "succeeded"); j.AttemptsStarted != 1 {
		t.Fatal("extra attempt")
	}
	j, err := (queue.Store{DB: db}).Get(context.Background(), second)
	if err != nil || j.State != "queued" || j.AttemptsStarted != 0 {
		t.Fatalf("claimed during shutdown: %+v %v", j, err)
	}
}

func TestWorkerShutdownTimeoutAndLeaseLossPostgres(t *testing.T) {
	for _, mode := range []string{"timeout", "lease_loss"} {
		t.Run(mode, func(t *testing.T) {
			db := workerDB(t)
			id := submit(t, db, mode)
			cfg := testConfig()
			cfg.ShutdownGrace = 30 * time.Millisecond
			entered := make(chan struct{})
			exited := make(chan struct{})
			handler := func(ctx context.Context, c *queue.Claim) (json.RawMessage, error) {
				close(entered)
				<-ctx.Done()
				close(exited)
				return nil, ctx.Err()
			}
			cancel, done := start(t, Worker{DB: db, Config: cfg, Handlers: map[string]Handler{"echo": handler}})
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("no claim")
			}
			if mode == "timeout" {
				cancel()
				if err := <-done; !errors.Is(err, ErrShutdownTimeout) {
					t.Fatalf("timeout: %v", err)
				}
			} else {
				// Invalidate the token while leaving its deadline live, so the next
				// heartbeat rejects it without immediately reclaiming another attempt.
				if _, err := db.Exec(`UPDATE queue_jobs SET lease_token=gen_random_uuid() WHERE id=$1`, id); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-exited:
			case <-time.After(2 * time.Second):
				t.Fatal("handler not canceled")
			}
			if mode == "lease_loss" {
				cancel()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			j, err := (queue.Store{DB: db}).Get(context.Background(), id)
			if err != nil || j.State != "leased" || j.LastError != nil || j.FinishedAt != nil {
				t.Fatalf("canceled handler persisted an outcome: %+v %v", j, err)
			}
		})
	}
}
