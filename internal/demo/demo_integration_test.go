package demo

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDemoConfigValidation(t *testing.T) {
	cfg := DefaultConfig("postgresql://localhost:5432/queue_test?sslmode=disable", "../..")
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.DSN = "" },
		func(c *Config) { c.DSN = "http://localhost:5432/db" },
		func(c *Config) { c.Lease = 50 * time.Millisecond },
		func(c *Config) { c.Heartbeat = c.Lease },
		func(c *Config) { c.VictimDelay = c.Lease },
		func(c *Config) { c.Timeout = time.Second },
	} {
		bad := cfg
		mutate(&bad)
		if err := bad.Validate(); err == nil {
			t.Fatalf("expected validation error for %+v", bad)
		}
	}
}

func TestFailureAndRecoveryDemoPostgres(t *testing.T) {
	dsn := os.Getenv("QUEUE_TEST_DSN")
	if dsn == "" {
		t.Skip("set QUEUE_TEST_DSN for PostgreSQL CLI failure and recovery demo test")
	}
	cfg := DefaultConfig(dsn, filepath.Join("..", ".."))
	report, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if report.BeforeCrash.Job.State != "leased" ||
		report.BeforeCrash.Job.AttemptsStarted != 1 ||
		len(report.BeforeCrash.Attempts) != 1 ||
		report.BeforeCrash.Attempts[0].WorkerID != "worker-A" ||
		report.BeforeCrash.Attempts[0].State != "running" {
		t.Fatalf("unexpected before-crash snapshot: %+v", report.BeforeCrash)
	}
	if report.AfterRecovery.Job.State != "succeeded" ||
		report.AfterRecovery.Job.AttemptsStarted != 2 ||
		report.AfterRecovery.Job.LeaseTokenSet ||
		len(report.AfterRecovery.Attempts) != 2 ||
		report.AfterRecovery.Attempts[0].State != "lease_expired" ||
		report.AfterRecovery.Attempts[1].State != "succeeded" ||
		report.AfterRecovery.Attempts[1].WorkerID != "worker-B" {
		t.Fatalf("unexpected after-recovery snapshot: %+v", report.AfterRecovery)
	}
	v := report.Verification
	if !v.HeartbeatExtensionWitnessed || !v.LiveCompetitorDidNotSteal || !v.VictimKilledWithSIGKILL ||
		!v.TokenRotatedOnReclaim || !v.ReclaimedNotBeforeExpiry || !v.AtomicExpiryTransition ||
		!v.StaleRenewRejected || !v.StaleCompleteRejected || !v.StaleFailRejected {
		t.Fatalf("unexpected verification flags: %+v", v)
	}
	summary := FormatTextSummary(report)
	if !strings.Contains(summary, "BEFORE CRASH") || !strings.Contains(summary, "AFTER SIGKILL & LEASE-EXPIRY RECOVERY") {
		t.Fatalf("unexpected formatted text summary:\n%s", summary)
	}
}
