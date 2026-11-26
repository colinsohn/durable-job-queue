package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"durable-job-queue/internal/bench"
)

func main() {
	dsn := os.Getenv("QUEUE_TEST_DSN")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	cfg := bench.DefaultConfig(dsn, ".")
	flag.IntVar(&cfg.Jobs, "jobs", cfg.Jobs, "unique jobs to submit and complete")
	flag.IntVar(&cfg.Duplicates, "duplicates", cfg.Duplicates, "concurrent duplicate idempotent submissions")
	flag.IntVar(&cfg.Producers, "producers", cfg.Producers, "concurrent HTTP submission producers")
	flag.IntVar(&cfg.Workers, "workers", cfg.Workers, "competing queue worker loops")
	flag.IntVar(&cfg.RetryEvery, "retry-every", cfg.RetryEvery, "fail first attempt of every Nth job (0 disables)")
	flag.DurationVar(&cfg.WorkDelay, "work-delay", cfg.WorkDelay, "simulated handler execution delay")
	flag.DurationVar(&cfg.Lease, "lease", cfg.Lease, "worker lease duration")
	flag.DurationVar(&cfg.Heartbeat, "heartbeat", cfg.Heartbeat, "worker heartbeat interval")
	flag.DurationVar(&cfg.Poll, "poll", cfg.Poll, "empty-queue poll interval")
	flag.DurationVar(&cfg.OperationTimeout, "operation-timeout", cfg.OperationTimeout, "database operation timeout")
	flag.DurationVar(&cfg.RetryBase, "retry-base", cfg.RetryBase, "first retry backoff delay")
	flag.DurationVar(&cfg.RetryCap, "retry-cap", cfg.RetryCap, "maximum retry backoff delay")
	flag.DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "overall benchmark timeout")
	reportPath := flag.String("report", "", "optional JSON report path written after all invariants pass")
	flag.Parse()

	if err := cfg.Validate(); err != nil {
		log.Fatal("invalid benchmark configuration; set QUEUE_TEST_DSN or DATABASE_URL and valid positive flags")
	}

	report, err := bench.Run(context.Background(), cfg)
	if err != nil {
		log.Fatal("benchmark failed; inspect disposable PostgreSQL connectivity and parameters")
	}

	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Fatal("failed to encode benchmark report")
	}
	encoded = append(encoded, '\n')

	if *reportPath != "" {
		dir := filepath.Dir(*reportPath)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatal("failed to create report directory")
		}
		if err := os.WriteFile(*reportPath, encoded, 0o644); err != nil {
			log.Fatal("failed to write benchmark report")
		}
	}
	fmt.Print(string(encoded))
}
