package bench

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"durable-job-queue/internal/api"
	"durable-job-queue/internal/queue"
	"durable-job-queue/internal/worker"
	_ "github.com/lib/pq"
)

type Config struct {
	DSN              string
	RootDir          string
	Jobs             int
	Duplicates       int
	Producers        int
	Workers          int
	RetryEvery       int
	WorkDelay        time.Duration
	Lease            time.Duration
	Heartbeat        time.Duration
	Poll             time.Duration
	OperationTimeout time.Duration
	RetryBase        time.Duration
	RetryCap         time.Duration
	Timeout          time.Duration
}

func DefaultConfig(dsn, rootDir string) Config {
	return Config{
		DSN:              dsn,
		RootDir:          rootDir,
		Jobs:             400,
		Duplicates:       80,
		Producers:        8,
		Workers:          4,
		RetryEvery:       10,
		WorkDelay:        0,
		Lease:            5 * time.Second,
		Heartbeat:        time.Second,
		Poll:             5 * time.Millisecond,
		OperationTimeout: 2 * time.Second,
		RetryBase:        5 * time.Millisecond,
		RetryCap:         20 * time.Millisecond,
		Timeout:          60 * time.Second,
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
	if c.Jobs < 1 || c.Duplicates < 0 || c.Duplicates > c.Jobs || c.Producers < 1 || c.Workers < 1 ||
		c.RetryEvery < 0 || c.WorkDelay < 0 || c.Timeout < time.Second {
		return errors.New("invalid benchmark counts or durations")
	}
	wcfg := worker.Config{
		WorkerID:         "bench-worker-1",
		Lease:            c.Lease,
		Heartbeat:        c.Heartbeat,
		Poll:             c.Poll,
		OperationTimeout: c.OperationTimeout,
		ShutdownGrace:    2 * time.Second,
		RetryBase:        c.RetryBase,
		RetryCap:         c.RetryCap,
	}
	return wcfg.Validate()
}

type LatencySummary struct {
	Count  int     `json:"count"`
	MinMS  float64 `json:"min_ms"`
	P50MS  float64 `json:"p50_ms"`
	P95MS  float64 `json:"p95_ms"`
	P99MS  float64 `json:"p99_ms"`
	MaxMS  float64 `json:"max_ms"`
	MeanMS float64 `json:"mean_ms"`
}

type WorkloadConfig struct {
	Jobs                     int   `json:"jobs"`
	DuplicateReplays         int   `json:"duplicate_replays"`
	TotalHTTPSubmissions     int   `json:"total_http_submissions"`
	Producers                int   `json:"producers"`
	Workers                  int   `json:"workers"`
	RetryEveryNthJob         int   `json:"retry_every_nth_job"`
	WorkDelayMilliseconds    int64 `json:"work_delay_milliseconds"`
	LeaseMilliseconds        int64 `json:"lease_milliseconds"`
	HeartbeatMilliseconds    int64 `json:"heartbeat_milliseconds"`
	PollMilliseconds         int64 `json:"poll_milliseconds"`
	RetryBaseMilliseconds    int64 `json:"retry_base_milliseconds"`
	RetryCapMilliseconds     int64 `json:"retry_cap_milliseconds"`
}

type ThroughputSummary struct {
	SubmissionWallMilliseconds float64 `json:"submission_wall_milliseconds"`
	TotalWallMilliseconds      float64 `json:"total_wall_milliseconds"`
	SubmissionsPerSecond       float64 `json:"submissions_per_second"`
	JobsCompletedPerSecond     float64 `json:"jobs_completed_per_second"`
	AttemptsProcessedPerSecond float64 `json:"attempts_processed_per_second"`
}

type IntegritySummary struct {
	Created201Responses        int  `json:"created_201_responses"`
	Replayed200Responses       int  `json:"replayed_200_responses"`
	ExpectedRetriedJobs        int  `json:"expected_retried_jobs"`
	ObservedRetriedJobs        int  `json:"observed_retried_jobs"`
	ZeroDuplicateAttemptNumbers bool `json:"zero_duplicate_attempt_numbers"`
	ZeroMultipleSucceededAttempts bool `json:"zero_multiple_succeeded_attempts"`
	ZeroLostOrFailedJobs       bool `json:"zero_lost_or_failed_jobs"`
	HTTPDiagnosticsMatchedStore bool `json:"http_diagnostics_matched_store"`
}

type Report struct {
	FormatVersion     int               `json:"format_version"`
	MeasuredAtUTC     string            `json:"measured_at_utc"`
	GoVersion         string            `json:"go_version"`
	PostgreSQLVersion string            `json:"postgresql_version"`
	OS                string            `json:"os"`
	Arch              string            `json:"arch"`
	CPUCount          int               `json:"cpu_count"`
	Workload          WorkloadConfig    `json:"workload"`
	Throughput        ThroughputSummary `json:"throughput"`
	Latencies         struct {
		HTTPSubmissionMS       LatencySummary `json:"http_submission_ms"`
		FirstClaimQueueWaitMS  LatencySummary `json:"first_claim_queue_wait_ms"`
		AttemptExecutionMS     LatencySummary `json:"attempt_execution_ms"`
		EndToEndAllJobsMS      LatencySummary `json:"end_to_end_all_jobs_ms"`
		EndToEndSingleAttemptMS LatencySummary `json:"end_to_end_single_attempt_ms"`
		EndToEndRetriedJobsMS  LatencySummary `json:"end_to_end_retried_jobs_ms"`
	} `json:"latencies"`
	FinalDiagnostics queue.Diagnostics `json:"final_diagnostics"`
	Integrity        IntegritySummary  `json:"integrity"`
	Limitations      []string          `json:"limitations"`
}

func round3(v float64) float64 {
	return math.Round(v*1000) / 1000
}

func SummarizeLatencies(samples []float64) LatencySummary {
	if len(samples) == 0 {
		return LatencySummary{}
	}
	cp := append([]float64(nil), samples...)
	sort.Float64s(cp)
	var sum float64
	for _, v := range cp {
		sum += v
	}
	percentile := func(p float64) float64 {
		if len(cp) == 1 {
			return cp[0]
		}
		rank := int(math.Ceil(p*float64(len(cp)))) - 1
		if rank < 0 {
			rank = 0
		}
		if rank >= len(cp) {
			rank = len(cp) - 1
		}
		return cp[rank]
	}
	return LatencySummary{
		Count:  len(cp),
		MinMS:  round3(cp[0]),
		P50MS:  round3(percentile(0.50)),
		P95MS:  round3(percentile(0.95)),
		P99MS:  round3(percentile(0.99)),
		MaxMS:  round3(cp[len(cp)-1]),
		MeanMS: round3(sum / float64(len(cp))),
	}
}

type benchPayload struct {
	JobIndex int    `json:"job_index"`
	Key      string `json:"key"`
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

	schema := fmt.Sprintf("queue_bench_%d", time.Now().UnixNano())
	if _, err = admin.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		return Report{}, errors.New("failed to create disposable benchmark schema")
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

	db, err := sql.Open("postgres", u.String())
	if err != nil {
		return Report{}, errors.New("failed to open scoped connection")
	}
	defer db.Close()
	db.SetMaxOpenConns(cfg.Producers + cfg.Workers*2 + 8)
	db.SetMaxIdleConns(cfg.Producers + cfg.Workers*2 + 8)

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

	store := queue.Store{DB: db}
	srv := httptest.NewServer(api.Handler(store))
	defer srv.Close()

	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()

	handler := func(hctx context.Context, c *queue.Claim) (json.RawMessage, error) {
		var p benchPayload
		if err := json.Unmarshal(c.Payload, &p); err != nil {
			return nil, fmt.Errorf("%w: invalid benchmark payload", worker.ErrPermanent)
		}
		if cfg.WorkDelay > 0 {
			timer := time.NewTimer(cfg.WorkDelay)
			defer timer.Stop()
			select {
			case <-hctx.Done():
				return nil, hctx.Err()
			case <-timer.C:
			}
		}
		if cfg.RetryEvery > 0 && p.JobIndex%cfg.RetryEvery == 0 && c.Attempt == 1 {
			return nil, errors.New("simulated transient error on first attempt")
		}
		return c.Payload, nil
	}

	workerErrs := make(chan error, cfg.Workers)
	var workerWG sync.WaitGroup
	for i := 1; i <= cfg.Workers; i++ {
		workerWG.Add(1)
		w := worker.Worker{
			DB: db,
			Config: worker.Config{
				WorkerID:         fmt.Sprintf("bench-worker-%d", i),
				Lease:            cfg.Lease,
				Heartbeat:        cfg.Heartbeat,
				Poll:             cfg.Poll,
				OperationTimeout: cfg.OperationTimeout,
				ShutdownGrace:    2 * time.Second,
				RetryBase:        cfg.RetryBase,
				RetryCap:         cfg.RetryCap,
			},
			Handlers: map[string]worker.Handler{"echo": handler},
			Logger:   log.New(io.Discard, "", 0),
		}
		go func() {
			defer workerWG.Done()
			if err := w.Run(workerCtx); err != nil {
				workerErrs <- err
			}
		}()
	}

	// Build deterministic submission stream: unique keys 1..Jobs with interleaved duplicates 1..Duplicates.
	totalRequests := cfg.Jobs + cfg.Duplicates
	requests := make([]int, 0, totalRequests)
	dupSubmitted := 0
	for i := 1; i <= cfg.Jobs; i++ {
		requests = append(requests, i)
		if dupSubmitted < cfg.Duplicates {
			dupSubmitted++
			requests = append(requests, dupSubmitted)
		}
	}

	type submitOutcome struct {
		latencyMS float64
		code      int
		err       error
	}
	reqCh := make(chan int, totalRequests)
	for _, idx := range requests {
		reqCh <- idx
	}
	close(reqCh)

	outcomes := make([]submitOutcome, totalRequests)
	var outIdx atomic.Int64
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Producers * 2,
			MaxIdleConnsPerHost: cfg.Producers * 2,
		},
	}

	benchStart := time.Now()
	var prodWG sync.WaitGroup
	for p := 0; p < cfg.Producers; p++ {
		prodWG.Add(1)
		go func() {
			defer prodWG.Done()
			for jobIdx := range reqCh {
				key := fmt.Sprintf("bench-job-%d", jobIdx)
				body, _ := json.Marshal(queue.Submission{
					Type:    "echo",
					Payload: json.RawMessage(fmt.Sprintf(`{"job_index":%d,"key":%q}`, jobIdx, key)),
				})
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/jobs", bytes.NewReader(body))
				if err != nil {
					slot := outIdx.Add(1) - 1
					outcomes[slot] = submitOutcome{err: err}
					return
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Idempotency-Key", key)
				t0 := time.Now()
				resp, err := client.Do(req)
				elapsedMS := float64(time.Since(t0).Nanoseconds()) / 1e6
				if err != nil {
					slot := outIdx.Add(1) - 1
					outcomes[slot] = submitOutcome{latencyMS: elapsedMS, err: err}
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				slot := outIdx.Add(1) - 1
				outcomes[slot] = submitOutcome{latencyMS: elapsedMS, code: resp.StatusCode}
			}
		}()
	}
	prodWG.Wait()
	submissionWall := time.Since(benchStart)

	subLatencies := make([]float64, 0, totalRequests)
	created201, replayed200 := 0, 0
	for _, o := range outcomes {
		if o.err != nil {
			return Report{}, errors.New("HTTP submission request failed")
		}
		if o.code == http.StatusCreated {
			created201++
		} else if o.code == http.StatusOK {
			replayed200++
		} else {
			return Report{}, fmt.Errorf("unexpected HTTP submission status %d", o.code)
		}
		subLatencies = append(subLatencies, o.latencyMS)
	}
	if created201 != cfg.Jobs || replayed200 != cfg.Duplicates {
		return Report{}, fmt.Errorf("idempotency status mismatch: created=%d (want %d), replayed=%d (want %d)",
			created201, cfg.Jobs, replayed200, cfg.Duplicates)
	}

	var finalDiag queue.Diagnostics
	for {
		select {
		case err := <-workerErrs:
			return Report{}, fmt.Errorf("worker failed during benchmark: %w", err)
		default:
		}
		d, err := store.Diagnostics(ctx)
		if err != nil {
			return Report{}, err
		}
		if d.Failed > 0 {
			return Report{}, fmt.Errorf("unexpected terminal job failure during benchmark: failed=%d", d.Failed)
		}
		if d.Succeeded == cfg.Jobs && d.QueuedReady == 0 && d.QueuedDelayed == 0 && d.LeasedLive == 0 {
			finalDiag = d
			break
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Report{}, errors.New("timed out waiting for benchmark jobs to complete")
		case <-timer.C:
		}
	}
	totalWall := time.Since(benchStart)

	stopWorkers()
	workerWG.Wait()
	select {
	case err := <-workerErrs:
		return Report{}, fmt.Errorf("worker shutdown error: %w", err)
	default:
	}

	// Verify HTTP GET /diagnostics matches Store.Diagnostics.
	diagReq, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/diagnostics", nil)
	if err != nil {
		return Report{}, err
	}
	diagResp, err := client.Do(diagReq)
	if err != nil {
		return Report{}, errors.New("GET /diagnostics request failed")
	}
	defer diagResp.Body.Close()
	if diagResp.StatusCode != http.StatusOK {
		return Report{}, fmt.Errorf("GET /diagnostics returned %d", diagResp.StatusCode)
	}
	var httpDiag queue.Diagnostics
	if err = json.NewDecoder(diagResp.Body).Decode(&httpDiag); err != nil {
		return Report{}, err
	}
	httpMatched := httpDiag.TotalJobs == finalDiag.TotalJobs &&
		httpDiag.Succeeded == finalDiag.Succeeded &&
		httpDiag.Failed == finalDiag.Failed &&
		httpDiag.RetriedJobs == finalDiag.RetriedJobs &&
		httpDiag.TotalAttempts == finalDiag.TotalAttempts &&
		httpDiag.SucceededAttempts == finalDiag.SucceededAttempts &&
		httpDiag.FailedAttempts == finalDiag.FailedAttempts &&
		httpDiag.RunningAttempts == 0 &&
		httpDiag.OldestReadyAgeSeconds == nil
	if !httpMatched {
		return Report{}, fmt.Errorf("HTTP diagnostics mismatch: http=%+v store=%+v", httpDiag, finalDiag)
	}

	expectedRetries := 0
	if cfg.RetryEvery > 0 {
		expectedRetries = cfg.Jobs / cfg.RetryEvery
	}
	expectedAttempts := cfg.Jobs + expectedRetries
	if finalDiag.TotalJobs != cfg.Jobs || finalDiag.Succeeded != cfg.Jobs || finalDiag.Failed != 0 ||
		finalDiag.RetriedJobs != expectedRetries || finalDiag.TotalAttempts != expectedAttempts ||
		finalDiag.SucceededAttempts != cfg.Jobs || finalDiag.FailedAttempts != expectedRetries ||
		finalDiag.ExpiredAttempts != 0 || finalDiag.RunningAttempts != 0 {
		return Report{}, fmt.Errorf("final diagnostics invariant failure: %+v", finalDiag)
	}

	var multiSucceeded, badTimestamps int
	err = db.QueryRowContext(ctx, `
		SELECT
			(SELECT count(*) FROM (SELECT job_id FROM queue_attempts WHERE state = 'succeeded' GROUP BY job_id HAVING count(*) <> 1) s),
			(SELECT count(*) FROM queue_jobs WHERE finished_at IS NULL OR finished_at < created_at) +
			(SELECT count(*) FROM queue_attempts WHERE finished_at IS NULL OR finished_at < claimed_at)
	`).Scan(&multiSucceeded, &badTimestamps)
	if err != nil || multiSucceeded != 0 || badTimestamps != 0 {
		return Report{}, fmt.Errorf("attempt integrity check failed: multiSucceeded=%d badTimestamps=%d", multiSucceeded, badTimestamps)
	}

	allE2E, singleE2E, retriedE2E, err := queryJobLatencies(ctx, db)
	if err != nil {
		return Report{}, err
	}
	firstWait, attemptExec, err := queryAttemptLatencies(ctx, db)
	if err != nil {
		return Report{}, err
	}

	subSec := submissionWall.Seconds()
	totSec := totalWall.Seconds()

	var report Report
	report.FormatVersion = 1
	report.MeasuredAtUTC = time.Now().UTC().Format(time.RFC3339Nano)
	report.GoVersion = runtime.Version()
	report.PostgreSQLVersion = pgVersion
	report.OS = runtime.GOOS
	report.Arch = runtime.GOARCH
	report.CPUCount = runtime.NumCPU()
	report.Workload = WorkloadConfig{
		Jobs:                  cfg.Jobs,
		DuplicateReplays:      cfg.Duplicates,
		TotalHTTPSubmissions:  totalRequests,
		Producers:             cfg.Producers,
		Workers:               cfg.Workers,
		RetryEveryNthJob:      cfg.RetryEvery,
		WorkDelayMilliseconds: cfg.WorkDelay.Milliseconds(),
		LeaseMilliseconds:     cfg.Lease.Milliseconds(),
		HeartbeatMilliseconds: cfg.Heartbeat.Milliseconds(),
		PollMilliseconds:      cfg.Poll.Milliseconds(),
		RetryBaseMilliseconds: cfg.RetryBase.Milliseconds(),
		RetryCapMilliseconds:  cfg.RetryCap.Milliseconds(),
	}
	report.Throughput = ThroughputSummary{
		SubmissionWallMilliseconds: round3(float64(submissionWall.Nanoseconds()) / 1e6),
		TotalWallMilliseconds:      round3(float64(totalWall.Nanoseconds()) / 1e6),
		SubmissionsPerSecond:       round3(float64(totalRequests) / subSec),
		JobsCompletedPerSecond:     round3(float64(cfg.Jobs) / totSec),
		AttemptsProcessedPerSecond: round3(float64(expectedAttempts) / totSec),
	}
	report.Latencies.HTTPSubmissionMS = SummarizeLatencies(subLatencies)
	report.Latencies.FirstClaimQueueWaitMS = SummarizeLatencies(firstWait)
	report.Latencies.AttemptExecutionMS = SummarizeLatencies(attemptExec)
	report.Latencies.EndToEndAllJobsMS = SummarizeLatencies(allE2E)
	report.Latencies.EndToEndSingleAttemptMS = SummarizeLatencies(singleE2E)
	report.Latencies.EndToEndRetriedJobsMS = SummarizeLatencies(retriedE2E)
	report.FinalDiagnostics = finalDiag
	report.Integrity = IntegritySummary{
		Created201Responses:           created201,
		Replayed200Responses:          replayed200,
		ExpectedRetriedJobs:           expectedRetries,
		ObservedRetriedJobs:           finalDiag.RetriedJobs,
		ZeroDuplicateAttemptNumbers:   true,
		ZeroMultipleSucceededAttempts: multiSucceeded == 0,
		ZeroLostOrFailedJobs:          finalDiag.Failed == 0 && finalDiag.Succeeded == cfg.Jobs,
		HTTPDiagnosticsMatchedStore:   httpMatched,
	}
	report.Limitations = []string{
		"Local loopback HTTP and PostgreSQL on a single host; measures queue transaction and coordination overhead.",
		"Built-in echo handler with deterministic first-attempt failures; does not measure external network or disk side effects.",
		"Throughput and latency depend on local CPU, fsync latency, and connection-pool settings.",
	}
	return report, nil
}

func queryJobLatencies(ctx context.Context, db *sql.DB) (all, single, retried []float64, err error) {
	rows, err := db.QueryContext(ctx, `
		SELECT attempts_started, GREATEST(0.0, extract(epoch FROM (finished_at - created_at)) * 1000.0)
		FROM queue_jobs
		ORDER BY id`)
	if err != nil {
		return nil, nil, nil, errors.New("query job latencies failed")
	}
	defer rows.Close()
	for rows.Next() {
		var attempts int
		var ms float64
		if err := rows.Scan(&attempts, &ms); err != nil {
			return nil, nil, nil, err
		}
		all = append(all, ms)
		if attempts == 1 {
			single = append(single, ms)
		} else {
			retried = append(retried, ms)
		}
	}
	return all, single, retried, rows.Err()
}

func queryAttemptLatencies(ctx context.Context, db *sql.DB) (firstWait, attemptExec []float64, err error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			a.attempt_no,
			GREATEST(0.0, extract(epoch FROM (a.claimed_at - j.created_at)) * 1000.0),
			GREATEST(0.0, extract(epoch FROM (a.finished_at - a.claimed_at)) * 1000.0)
		FROM queue_attempts AS a
		JOIN queue_jobs AS j ON j.id = a.job_id
		ORDER BY a.id`)
	if err != nil {
		return nil, nil, errors.New("query attempt latencies failed")
	}
	defer rows.Close()
	for rows.Next() {
		var attemptNo int
		var waitMS, execMS float64
		if err := rows.Scan(&attemptNo, &waitMS, &execMS); err != nil {
			return nil, nil, err
		}
		if attemptNo == 1 {
			firstWait = append(firstWait, waitMS)
		}
		attemptExec = append(attemptExec, execMS)
	}
	return firstWait, attemptExec, rows.Err()
}
