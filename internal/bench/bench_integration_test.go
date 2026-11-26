package bench

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigValidationAndPercentileSummary(t *testing.T) {
	cfg := DefaultConfig("postgresql://localhost:5432/queue_test?sslmode=disable", "../..")
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.DSN = "" },
		func(c *Config) { c.DSN = "http://localhost:5432/db" },
		func(c *Config) { c.Jobs = 0 },
		func(c *Config) { c.Duplicates = -1 },
		func(c *Config) { c.Duplicates = c.Jobs + 1 },
		func(c *Config) { c.Producers = 0 },
		func(c *Config) { c.Workers = 0 },
		func(c *Config) { c.RetryEvery = -1 },
		func(c *Config) { c.Heartbeat = c.Lease },
	} {
		bad := cfg
		mutate(&bad)
		if err := bad.Validate(); err == nil {
			t.Fatalf("expected validation error for %+v", bad)
		}
	}

	if empty := SummarizeLatencies(nil); empty.Count != 0 {
		t.Fatalf("expected zero count for empty samples, got %+v", empty)
	}
	single := SummarizeLatencies([]float64{4.25})
	if single.Count != 1 || single.MinMS != 4.25 || single.P50MS != 4.25 || single.P99MS != 4.25 || single.MaxMS != 4.25 {
		t.Fatalf("unexpected single summary: %+v", single)
	}
	samples := make([]float64, 100)
	for i := range samples {
		samples[i] = float64(100 - i)
	}
	sum := SummarizeLatencies(samples)
	if sum.Count != 100 || sum.MinMS != 1 || sum.P50MS != 50 || sum.P95MS != 95 || sum.P99MS != 99 || sum.MaxMS != 100 || sum.MeanMS != 50.5 {
		t.Fatalf("unexpected 1..100 percentile summary: %+v", sum)
	}
}

func TestRepeatableLoadBenchmarkPostgres(t *testing.T) {
	dsn := os.Getenv("QUEUE_TEST_DSN")
	if dsn == "" {
		t.Skip("set QUEUE_TEST_DSN for PostgreSQL load benchmark integration test")
	}
	cfg := DefaultConfig(dsn, filepath.Join("..", ".."))
	cfg.Jobs = 60
	cfg.Duplicates = 15
	cfg.Producers = 6
	cfg.Workers = 3
	cfg.RetryEvery = 10
	cfg.Poll = 2 * time.Millisecond
	cfg.RetryBase = 3 * time.Millisecond
	cfg.RetryCap = 10 * time.Millisecond

	report, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if report.FinalDiagnostics.TotalJobs != 60 ||
		report.FinalDiagnostics.Succeeded != 60 ||
		report.FinalDiagnostics.Failed != 0 ||
		report.FinalDiagnostics.RetriedJobs != 6 ||
		report.FinalDiagnostics.TotalAttempts != 66 ||
		report.FinalDiagnostics.SucceededAttempts != 60 ||
		report.FinalDiagnostics.FailedAttempts != 6 ||
		report.FinalDiagnostics.ExpiredAttempts != 0 {
		t.Fatalf("unexpected benchmark diagnostics: %+v", report.FinalDiagnostics)
	}
	if report.Integrity.Created201Responses != 60 ||
		report.Integrity.Replayed200Responses != 15 ||
		!report.Integrity.HTTPDiagnosticsMatchedStore ||
		!report.Integrity.ZeroLostOrFailedJobs {
		t.Fatalf("unexpected benchmark integrity: %+v", report.Integrity)
	}
	if report.Latencies.HTTPSubmissionMS.Count != 75 ||
		report.Latencies.FirstClaimQueueWaitMS.Count != 60 ||
		report.Latencies.AttemptExecutionMS.Count != 66 ||
		report.Latencies.EndToEndAllJobsMS.Count != 60 ||
		report.Latencies.EndToEndSingleAttemptMS.Count != 54 ||
		report.Latencies.EndToEndRetriedJobsMS.Count != 6 {
		t.Fatalf("unexpected latency sample counts: %+v", report.Latencies)
	}
	if report.Throughput.SubmissionsPerSecond <= 0 ||
		report.Throughput.JobsCompletedPerSecond <= 0 ||
		report.Throughput.AttemptsProcessedPerSecond <= 0 {
		t.Fatalf("expected positive throughput: %+v", report.Throughput)
	}
}
