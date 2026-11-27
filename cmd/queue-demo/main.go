package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"durable-job-queue/internal/demo"
)

func main() {
	dsn := os.Getenv("QUEUE_TEST_DSN")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	cfg := demo.DefaultConfig(dsn, ".")
	flag.DurationVar(&cfg.Lease, "lease", cfg.Lease, "worker lease duration")
	flag.DurationVar(&cfg.Heartbeat, "heartbeat", cfg.Heartbeat, "worker heartbeat interval")
	flag.DurationVar(&cfg.Poll, "poll", cfg.Poll, "worker poll interval")
	flag.DurationVar(&cfg.OperationTimeout, "operation-timeout", cfg.OperationTimeout, "database operation timeout")
	flag.DurationVar(&cfg.VictimDelay, "victim-delay", cfg.VictimDelay, "victim worker handler delay before SIGKILL")
	flag.DurationVar(&cfg.SurvivorDelay, "survivor-delay", cfg.SurvivorDelay, "survivor worker handler delay")
	flag.DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "overall demo timeout")
	reportPath := flag.String("report", "", "optional JSON report path written after recovery succeeds")
	jsonOnly := flag.Bool("json", false, "print JSON report instead of formatted before/after history")
	flag.Parse()

	if err := cfg.Validate(); err != nil {
		log.Fatal("invalid demo configuration; set QUEUE_TEST_DSN or DATABASE_URL and valid positive durations")
	}

	report, err := demo.Run(context.Background(), cfg)
	if err != nil {
		log.Fatal("failure and recovery demo failed; inspect disposable PostgreSQL connectivity")
	}

	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Fatal("failed to encode demo report")
	}
	encoded = append(encoded, '\n')

	if *reportPath != "" {
		if err := os.MkdirAll(filepath.Dir(*reportPath), 0o755); err != nil {
			log.Fatal("failed to create report directory")
		}
		if err := os.WriteFile(*reportPath, encoded, 0o644); err != nil {
			log.Fatal("failed to write demo report")
		}
	}

	if *jsonOnly {
		fmt.Print(string(encoded))
	} else {
		fmt.Print(demo.FormatTextSummary(report))
	}
}
