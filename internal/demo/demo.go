package demo

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"durable-job-queue/internal/api"
	"durable-job-queue/internal/queue"
	_ "github.com/lib/pq"
)

type Config struct {
	DSN              string
	RootDir          string
	Lease            time.Duration
	Heartbeat        time.Duration
	Poll             time.Duration
	OperationTimeout time.Duration
	VictimDelay      time.Duration
	SurvivorDelay    time.Duration
	Timeout          time.Duration
}

func DefaultConfig(dsn, rootDir string) Config {
	return Config{
		DSN:              dsn,
		RootDir:          rootDir,
		Lease:            800 * time.Millisecond,
		Heartbeat:        150 * time.Millisecond,
		Poll:             25 * time.Millisecond,
		OperationTimeout: 200 * time.Millisecond,
		VictimDelay:      10 * time.Second,
		SurvivorDelay:    80 * time.Millisecond,
		Timeout:          30 * time.Second,
	}
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.DSN) == "" {
		return errors.New("database URL is required")
	}
	u, err := url.Parse(c.DSN)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return errors.New("database URL must use postgres or postgresql scheme")
	}
	if c.Lease < 200*time.Millisecond || c.Heartbeat < 20*time.Millisecond ||
		c.OperationTimeout < 20*time.Millisecond || c.Heartbeat+c.OperationTimeout >= c.Lease ||
		c.Poll <= 0 || c.VictimDelay <= c.Lease || c.SurvivorDelay < 0 || c.Timeout < 3*time.Second {
		return errors.New("invalid demo timing parameters")
	}
	return nil
}

type JobSnapshot struct {
	ID              int64           `json:"id"`
	IdempotencyKey  string          `json:"idempotency_key"`
	Type            string          `json:"type"`
	State           string          `json:"state"`
	AttemptsStarted int             `json:"attempts_started"`
	MaxAttempts     int             `json:"max_attempts"`
	LeaseOwner      *string         `json:"lease_owner"`
	LeaseTokenSet   bool            `json:"lease_token_set"`
	LeaseExpiresAt  *time.Time      `json:"lease_expires_at"`
	Result          json.RawMessage `json:"result,omitempty"`
	LastError       *string         `json:"last_error,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	FinishedAt      *time.Time      `json:"finished_at,omitempty"`
}

type AttemptSnapshot struct {
	AttemptNo      int        `json:"attempt_no"`
	WorkerID       string     `json:"worker_id"`
	State          string     `json:"state"`
	ClaimedAt      time.Time  `json:"claimed_at"`
	LeaseExpiresAt time.Time  `json:"lease_expires_at"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	LastError      *string    `json:"last_error,omitempty"`
}

type PhaseSnapshot struct {
	Phase       string            `json:"phase"`
	CapturedAt  time.Time         `json:"captured_at"`
	Job         JobSnapshot       `json:"job"`
	Attempts    []AttemptSnapshot `json:"attempts"`
	Diagnostics queue.Diagnostics `json:"diagnostics"`
}

type FencingVerification struct {
	HeartbeatExtensionWitnessed bool `json:"heartbeat_extension_witnessed"`
	LiveCompetitorDidNotSteal   bool `json:"live_competitor_did_not_steal"`
	VictimKilledWithSIGKILL     bool `json:"victim_killed_with_sigkill"`
	TokenRotatedOnReclaim       bool `json:"token_rotated_on_reclaim"`
	ReclaimedNotBeforeExpiry    bool `json:"reclaimed_not_before_expiry"`
	AtomicExpiryTransition      bool `json:"atomic_expiry_transition"`
	StaleRenewRejected          bool `json:"stale_renew_rejected"`
	StaleCompleteRejected       bool `json:"stale_complete_rejected"`
	StaleFailRejected           bool `json:"stale_fail_rejected"`
}

type Report struct {
	FormatVersion     int                 `json:"format_version"`
	MeasuredAtUTC     string              `json:"measured_at_utc"`
	GoVersion         string              `json:"go_version"`
	PostgreSQLVersion string              `json:"postgresql_version"`
	BeforeCrash       PhaseSnapshot       `json:"before_crash"`
	AfterRecovery     PhaseSnapshot       `json:"after_recovery"`
	Verification      FencingVerification `json:"verification"`
}

func FormatTextSummary(r Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "=== DURABLE JOB QUEUE — LOCAL CLI FAILURE & RECOVERY DEMO ===\n")
	fmt.Fprintf(&b, "Environment: %s | PostgreSQL %s\n\n", r.GoVersion, r.PostgreSQLVersion)

	writePhase := func(title string, p PhaseSnapshot) {
		owner := "<none>"
		if p.Job.LeaseOwner != nil {
			owner = *p.Job.LeaseOwner
		}
		fmt.Fprintf(&b, "--- %s ---\n", title)
		fmt.Fprintf(&b, "Job #%d (key=%s, type=%s): state=%s attempts=%d/%d lease_owner=%s token_present=%t\n",
			p.Job.ID, p.Job.IdempotencyKey, p.Job.Type, p.Job.State,
			p.Job.AttemptsStarted, p.Job.MaxAttempts, owner, p.Job.LeaseTokenSet)
		if len(p.Job.Result) > 0 {
			fmt.Fprintf(&b, "Job Result: %s\n", string(p.Job.Result))
		}
		fmt.Fprintf(&b, "Attempt History (%d rows):\n", len(p.Attempts))
		for _, a := range p.Attempts {
			fin := "<active>"
			if a.FinishedAt != nil {
				fin = a.FinishedAt.UTC().Format(time.RFC3339Nano)
			}
			fmt.Fprintf(&b, "  attempt=%d worker=%s state=%-13s claimed_at=%s lease_expires_at=%s finished_at=%s\n",
				a.AttemptNo, a.WorkerID, a.State,
				a.ClaimedAt.UTC().Format(time.RFC3339Nano),
				a.LeaseExpiresAt.UTC().Format(time.RFC3339Nano),
				fin)
		}
		fmt.Fprintf(&b, "Diagnostics: total_jobs=%d leased_live=%d succeeded=%d retried_jobs=%d running_attempts=%d expired_attempts=%d succeeded_attempts=%d\n\n",
			p.Diagnostics.TotalJobs, p.Diagnostics.LeasedLive, p.Diagnostics.Succeeded,
			p.Diagnostics.RetriedJobs, p.Diagnostics.RunningAttempts,
			p.Diagnostics.ExpiredAttempts, p.Diagnostics.SucceededAttempts)
	}

	writePhase("1. BEFORE CRASH (Worker-A heartbeating while Worker-B competes)", r.BeforeCrash)
	writePhase("2. AFTER SIGKILL & LEASE-EXPIRY RECOVERY (Worker-B reclaims & completes)", r.AfterRecovery)

	fmt.Fprintf(&b, "--- 3. INVARIANT & FENCING VERIFICATION ---\n")
	fmt.Fprintf(&b, "  heartbeat_extension_witnessed : %t\n", r.Verification.HeartbeatExtensionWitnessed)
	fmt.Fprintf(&b, "  live_competitor_did_not_steal : %t\n", r.Verification.LiveCompetitorDidNotSteal)
	fmt.Fprintf(&b, "  victim_killed_with_sigkill    : %t\n", r.Verification.VictimKilledWithSIGKILL)
	fmt.Fprintf(&b, "  token_rotated_on_reclaim      : %t\n", r.Verification.TokenRotatedOnReclaim)
	fmt.Fprintf(&b, "  reclaimed_not_before_expiry   : %t\n", r.Verification.ReclaimedNotBeforeExpiry)
	fmt.Fprintf(&b, "  atomic_expiry_transition      : %t\n", r.Verification.AtomicExpiryTransition)
	fmt.Fprintf(&b, "  stale_renew_rejected          : %t\n", r.Verification.StaleRenewRejected)
	fmt.Fprintf(&b, "  stale_complete_rejected       : %t\n", r.Verification.StaleCompleteRejected)
	fmt.Fprintf(&b, "  stale_fail_rejected           : %t\n", r.Verification.StaleFailRejected)
	return b.String()
}

func Run(parent context.Context, cfg Config) (Report, error) {
	if cfg.RootDir == "" {
		cfg.RootDir = "."
	}
	if err := cfg.Validate(); err != nil {
		return Report{}, err
	}
	ctx, cancel := context.WithTimeout(parent, cfg.Timeout)
	defer cancel()

	admin, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		return Report{}, errors.New("failed to open admin connection")
	}
	defer admin.Close()

	schema := fmt.Sprintf("queue_demo_%d", time.Now().UnixNano())
	if _, err = admin.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		return Report{}, errors.New("failed to create disposable demo schema")
	}
	defer func() {
		cleanupCtx, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		_, _ = admin.ExecContext(cleanupCtx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	}()

	u, _ := url.Parse(cfg.DSN)
	q := u.Query()
	q.Set("search_path", schema)
	q.Set("application_name", schema)
	u.RawQuery = q.Encode()
	scopedDSN := u.String()

	db, err := sql.Open("postgres", scopedDSN)
	if err != nil {
		return Report{}, errors.New("failed to open scoped connection")
	}
	defer db.Close()

	for _, name := range []string{"schema.sql", "lease.sql", "failure.sql"} {
		ddl, err := os.ReadFile(filepath.Join(cfg.RootDir, "db", name))
		if err != nil {
			return Report{}, fmt.Errorf("read %s: %w", name, err)
		}
		if _, err = db.ExecContext(ctx, string(ddl)); err != nil {
			return Report{}, fmt.Errorf("apply %s: database execution failed", name)
		}
	}

	var pgVersion string
	if err = db.QueryRowContext(ctx, `SHOW server_version`).Scan(&pgVersion); err != nil {
		return Report{}, errors.New("query server_version failed")
	}

	tmpDir, err := os.MkdirTemp("", "queue-demo-bin-*")
	if err != nil {
		return Report{}, errors.New("failed to create temporary binary directory")
	}
	defer os.RemoveAll(tmpDir)

	workerBin := filepath.Join(tmpDir, "queue-worker")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", workerBin, "./cmd/queue-worker")
	buildCmd.Dir = cfg.RootDir
	if out, err := buildCmd.CombinedOutput(); err != nil {
		return Report{}, fmt.Errorf("failed to build queue-worker binary: %s", strings.TrimSpace(string(out)))
	}

	store := queue.Store{DB: db}
	srv := httptest.NewServer(api.Handler(store))
	defer srv.Close()

	// Submit demo job through the HTTP API with Idempotency-Key.
	reqBody := []byte(`{"type":"echo","payload":{"demo":"worker_crash_recovery","step":"sigkill_failover"}}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/jobs", bytes.NewReader(reqBody))
	if err != nil {
		return Report{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "demo-crash-recovery-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Report{}, errors.New("HTTP job submission failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return Report{}, fmt.Errorf("expected 201 Created from POST /jobs, got %d", resp.StatusCode)
	}
	var created struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return Report{}, err
	}
	jobID := created.ID

	startWorker := func(workerID string, echoDelay time.Duration) (*exec.Cmd, error) {
		cmd := exec.Command(
			workerBin,
			"--id", workerID,
			"--lease", cfg.Lease.String(),
			"--heartbeat", cfg.Heartbeat.String(),
			"--poll", cfg.Poll.String(),
			"--operation-timeout", cfg.OperationTimeout.String(),
			"--shutdown-grace", "300ms",
			"--echo-delay", echoDelay.String(),
		)
		env := make([]string, 0, len(os.Environ())+1)
		for _, kv := range os.Environ() {
			if !strings.HasPrefix(kv, "DATABASE_URL=") && !strings.HasPrefix(kv, "PGOPTIONS=") {
				env = append(env, kv)
			}
		}
		env = append(env, "DATABASE_URL="+scopedDSN)
		cmd.Env = env
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		return cmd, nil
	}

	victimCmd, err := startWorker("worker-A", cfg.VictimDelay)
	if err != nil {
		return Report{}, fmt.Errorf("failed to start worker-A: %w", err)
	}
	defer func() {
		if victimCmd.Process != nil {
			_ = victimCmd.Process.Kill()
			_, _ = victimCmd.Process.Wait()
		}
	}()

	// Wait for worker-A to claim attempt 1.
	oldToken, err := waitForClaim(ctx, db, jobID, "worker-A", 1)
	if err != nil {
		return Report{}, err
	}

	// Start competing worker-B while worker-A holds the live lease.
	survivorCmd, err := startWorker("worker-B", cfg.SurvivorDelay)
	if err != nil {
		return Report{}, fmt.Errorf("failed to start worker-B: %w", err)
	}
	defer func() {
		if survivorCmd.Process != nil {
			_ = survivorCmd.Process.Signal(syscall.SIGTERM)
			_ = survivorCmd.Wait()
		}
	}()

	// Witness at least one live heartbeat extension by worker-A while worker-B is competing.
	if err := waitForHeartbeatExtension(ctx, db, jobID, 1, cfg.Lease); err != nil {
		return Report{}, err
	}

	beforeSnap, err := capturePhase(ctx, db, store, jobID, "before_crash_live_lease")
	if err != nil {
		return Report{}, err
	}
	if beforeSnap.Job.State != "leased" || beforeSnap.Job.AttemptsStarted != 1 ||
		beforeSnap.Job.LeaseOwner == nil || *beforeSnap.Job.LeaseOwner != "worker-A" {
		return Report{}, fmt.Errorf("unexpected before-crash job state: %+v", beforeSnap.Job)
	}

	// Abruptly kill worker-A with SIGKILL after its claim and heartbeat.
	if err := victimCmd.Process.Kill(); err != nil {
		return Report{}, fmt.Errorf("failed to SIGKILL worker-A: %w", err)
	}
	victimState, err := victimCmd.Process.Wait()
	if err != nil {
		return Report{}, fmt.Errorf("wait for killed worker-A failed: %w", err)
	}
	ws, ok := victimState.Sys().(syscall.WaitStatus)
	killedBySIGKILL := ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL

	// Wait for actual lease expiry and worker-B to claim attempt 2 with a new token.
	freshToken, err := waitForClaim(ctx, db, jobID, "worker-B", 2)
	if err != nil {
		return Report{}, err
	}
	tokenRotated := freshToken != "" && freshToken != oldToken

	// Verify stale writes from old owner/token are fenced off while survivor holds or completes attempt 2.
	_, renewErr := queue.Renew(ctx, db, jobID, oldToken, "worker-A", cfg.Lease)
	staleRenewRejected := errors.Is(renewErr, queue.ErrLeaseLost)

	compErr := queue.Complete(ctx, db, jobID, oldToken, "worker-A", json.RawMessage(`{"stale":true}`))
	staleCompleteRejected := errors.Is(compErr, queue.ErrLeaseLost)

	_, failErr := queue.Fail(ctx, db, jobID, oldToken, "worker-A", "stale failure", time.Second, 2*time.Second, false)
	staleFailRejected := errors.Is(failErr, queue.ErrLeaseLost)

	if !staleRenewRejected || !staleCompleteRejected || !staleFailRejected {
		return Report{}, fmt.Errorf("stale token fencing check failed: renew=%v complete=%v fail=%v",
			renewErr, compErr, failErr)
	}

	// Wait for worker-B to finish attempt 2 and mark the job succeeded.
	if err := waitForSucceeded(ctx, db, jobID); err != nil {
		return Report{}, err
	}

	// Gracefully stop survivor worker-B.
	_ = survivorCmd.Process.Signal(syscall.SIGTERM)
	if err := survivorCmd.Wait(); err != nil {
		return Report{}, fmt.Errorf("worker-B did not exit cleanly on SIGTERM: %w", err)
	}

	afterSnap, err := capturePhase(ctx, db, store, jobID, "after_sigkill_recovery")
	if err != nil {
		return Report{}, err
	}
	if len(afterSnap.Attempts) != 2 ||
		afterSnap.Attempts[0].State != "lease_expired" ||
		afterSnap.Attempts[1].State != "succeeded" ||
		afterSnap.Attempts[0].FinishedAt == nil {
		return Report{}, fmt.Errorf("unexpected post-recovery attempts: %+v", afterSnap.Attempts)
	}

	reclaimedNotBeforeExpiry := !afterSnap.Attempts[1].ClaimedAt.Before(afterSnap.Attempts[0].LeaseExpiresAt)
	atomicExpiryTransition := afterSnap.Attempts[0].FinishedAt.Equal(afterSnap.Attempts[1].ClaimedAt)
	if !tokenRotated || !reclaimedNotBeforeExpiry || !atomicExpiryTransition {
		return Report{}, fmt.Errorf("recovery invariant violation: tokenRotated=%t notBeforeExpiry=%t atomic=%t",
			tokenRotated, reclaimedNotBeforeExpiry, atomicExpiryTransition)
	}

	return Report{
		FormatVersion:     1,
		MeasuredAtUTC:     time.Now().UTC().Format(time.RFC3339Nano),
		GoVersion:         runtime.Version(),
		PostgreSQLVersion: pgVersion,
		BeforeCrash:       beforeSnap,
		AfterRecovery:     afterSnap,
		Verification: FencingVerification{
			HeartbeatExtensionWitnessed: true,
			LiveCompetitorDidNotSteal:   true,
			VictimKilledWithSIGKILL:     killedBySIGKILL,
			TokenRotatedOnReclaim:       tokenRotated,
			ReclaimedNotBeforeExpiry:    reclaimedNotBeforeExpiry,
			AtomicExpiryTransition:      atomicExpiryTransition,
			StaleRenewRejected:          staleRenewRejected,
			StaleCompleteRejected:       staleCompleteRejected,
			StaleFailRejected:           staleFailRejected,
		},
	}, nil
}

func waitForClaim(ctx context.Context, db *sql.DB, jobID int64, owner string, attempt int) (string, error) {
	ticker := time.NewTicker(15 * time.Millisecond)
	defer ticker.Stop()
	for {
		var state string
		var started int
		var curOwner, token sql.NullString
		err := db.QueryRowContext(ctx, `
			SELECT state, attempts_started, lease_owner, lease_token::text
			FROM queue_jobs WHERE id = $1`, jobID).Scan(&state, &started, &curOwner, &token)
		if err != nil {
			return "", err
		}
		if state == "leased" && started == attempt && curOwner.Valid && curOwner.String == owner && token.Valid {
			return token.String, nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timed out waiting for %s to claim attempt %d", owner, attempt)
		case <-ticker.C:
		}
	}
}

func waitForHeartbeatExtension(ctx context.Context, db *sql.DB, jobID int64, attemptNo int, lease time.Duration) error {
	threshold := lease + 50*time.Millisecond
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var claimedAt, expiresAt time.Time
		err := db.QueryRowContext(ctx, `
			SELECT claimed_at, lease_expires_at
			FROM queue_attempts
			WHERE job_id = $1 AND attempt_no = $2`, jobID, attemptNo).Scan(&claimedAt, &expiresAt)
		if err != nil {
			return err
		}
		if expiresAt.Sub(claimedAt) >= threshold {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.New("timed out waiting for live heartbeat extension")
		case <-ticker.C:
		}
	}
}

func waitForSucceeded(ctx context.Context, db *sql.DB, jobID int64) error {
	ticker := time.NewTicker(15 * time.Millisecond)
	defer ticker.Stop()
	for {
		var state string
		if err := db.QueryRowContext(ctx, `SELECT state FROM queue_jobs WHERE id = $1`, jobID).Scan(&state); err != nil {
			return err
		}
		if state == "succeeded" {
			return nil
		}
		if state == "failed" {
			return errors.New("demo job unexpectedly entered terminal failed state")
		}
		select {
		case <-ctx.Done():
			return errors.New("timed out waiting for survivor to complete job")
		case <-ticker.C:
		}
	}
}

func capturePhase(ctx context.Context, db *sql.DB, store queue.Store, jobID int64, phase string) (PhaseSnapshot, error) {
	var j JobSnapshot
	var owner, token, lastErr sql.NullString
	var leaseExp, finAt sql.NullTime
	var resultBytes []byte
	err := db.QueryRowContext(ctx, `
		SELECT id, idempotency_key, job_type, state, attempts_started, max_attempts,
		       lease_owner, lease_token::text, lease_expires_at, result, last_error, created_at, finished_at
		FROM queue_jobs WHERE id = $1`, jobID).Scan(
		&j.ID, &j.IdempotencyKey, &j.Type, &j.State, &j.AttemptsStarted, &j.MaxAttempts,
		&owner, &token, &leaseExp, &resultBytes, &lastErr, &j.CreatedAt, &finAt,
	)
	if err != nil {
		return PhaseSnapshot{}, err
	}
	if owner.Valid {
		s := owner.String
		j.LeaseOwner = &s
	}
	j.LeaseTokenSet = token.Valid && token.String != ""
	if leaseExp.Valid {
		t := leaseExp.Time.UTC()
		j.LeaseExpiresAt = &t
	}
	if len(resultBytes) > 0 {
		j.Result = json.RawMessage(resultBytes)
	}
	if lastErr.Valid {
		s := lastErr.String
		j.LastError = &s
	}
	j.CreatedAt = j.CreatedAt.UTC()
	if finAt.Valid {
		t := finAt.Time.UTC()
		j.FinishedAt = &t
	}

	rows, err := db.QueryContext(ctx, `
		SELECT attempt_no, worker_id, state, claimed_at, lease_expires_at, finished_at, last_error
		FROM queue_attempts
		WHERE job_id = $1
		ORDER BY attempt_no`, jobID)
	if err != nil {
		return PhaseSnapshot{}, err
	}
	defer rows.Close()

	var attempts []AttemptSnapshot
	for rows.Next() {
		var a AttemptSnapshot
		var aFin sql.NullTime
		var aErr sql.NullString
		if err := rows.Scan(&a.AttemptNo, &a.WorkerID, &a.State, &a.ClaimedAt, &a.LeaseExpiresAt, &aFin, &aErr); err != nil {
			return PhaseSnapshot{}, err
		}
		a.ClaimedAt = a.ClaimedAt.UTC()
		a.LeaseExpiresAt = a.LeaseExpiresAt.UTC()
		if aFin.Valid {
			t := aFin.Time.UTC()
			a.FinishedAt = &t
		}
		if aErr.Valid {
			s := aErr.String
			a.LastError = &s
		}
		attempts = append(attempts, a)
	}
	if err := rows.Err(); err != nil {
		return PhaseSnapshot{}, err
	}

	diag, err := store.Diagnostics(ctx)
	if err != nil {
		return PhaseSnapshot{}, err
	}
	return PhaseSnapshot{
		Phase:       phase,
		CapturedAt:  time.Now().UTC(),
		Job:         j,
		Attempts:    attempts,
		Diagnostics: diag,
	}, nil
}
