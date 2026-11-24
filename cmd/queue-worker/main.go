package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"durable-job-queue/internal/worker"
	_ "github.com/lib/pq"
)

func main() {
	cfg := worker.DefaultConfig("worker-1")
	flag.StringVar(&cfg.WorkerID, "id", cfg.WorkerID, "worker ID (use a distinct ID per process)")
	flag.DurationVar(&cfg.Lease, "lease", cfg.Lease, "lease duration")
	flag.DurationVar(&cfg.Heartbeat, "heartbeat", cfg.Heartbeat, "heartbeat interval")
	flag.DurationVar(&cfg.Poll, "poll", cfg.Poll, "empty-queue poll interval")
	flag.DurationVar(&cfg.OperationTimeout, "operation-timeout", cfg.OperationTimeout, "database operation timeout")
	flag.DurationVar(&cfg.ShutdownGrace, "shutdown-grace", cfg.ShutdownGrace, "active job drain window")
	flag.DurationVar(&cfg.RetryBase, "retry-base", cfg.RetryBase, "first retry delay")
	flag.DurationVar(&cfg.RetryCap, "retry-cap", cfg.RetryCap, "maximum retry delay")
	delay := flag.Duration("echo-delay", 0, "simulated echo work duration (for local demonstrations)")
	flag.Parse()
	if cfg.Validate() != nil || *delay < 0 {
		log.Fatal("invalid worker options")
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("open database failed")
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	ping, done := context.WithTimeout(context.Background(), 5*time.Second)
	err = db.PingContext(ping)
	done()
	if err != nil {
		log.Fatal("database connection failed")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// A second signal uses the OS default action while the first drains work.
	go func() { <-ctx.Done(); stop() }()
	w := worker.Worker{DB: db, Config: cfg, Handlers: map[string]worker.Handler{"echo": worker.Echo(*delay)}, Logger: log.Default()}
	log.Printf("worker=%s started", cfg.WorkerID)
	if err = w.Run(ctx); err != nil {
		// Do not print DSNs or arbitrary database/handler error contents.
		log.Fatal("worker stopped with an error; inspect job status and database availability")
	}
	log.Printf("worker=%s stopped", cfg.WorkerID)
}
