package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"durable-job-queue/internal/queue"
)

var ErrPermanent = errors.New("permanent handler failure")
var ErrShutdownTimeout = errors.New("worker shutdown deadline exceeded; unfinished lease left for recovery")

type Handler func(context.Context, *queue.Claim) (json.RawMessage, error)

type Config struct {
	WorkerID                                                string
	Lease, Heartbeat, Poll, OperationTimeout, ShutdownGrace time.Duration
	RetryBase, RetryCap                                     time.Duration
}

func DefaultConfig(id string) Config {
	return Config{WorkerID: id, Lease: 30 * time.Second, Heartbeat: 10 * time.Second, Poll: 250 * time.Millisecond,
		OperationTimeout: 2 * time.Second, ShutdownGrace: 5 * time.Second, RetryBase: time.Second, RetryCap: 30 * time.Second}
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.WorkerID) == "" || c.Lease < time.Millisecond || c.Heartbeat < time.Millisecond ||
		c.OperationTimeout < time.Millisecond || c.OperationTimeout >= c.Lease || c.Heartbeat >= c.Lease-c.OperationTimeout ||
		c.Poll <= 0 || c.ShutdownGrace < 0 || c.RetryBase < time.Millisecond || c.RetryCap < c.RetryBase {
		return errors.New("invalid worker configuration: positive timings, heartbeat + operation timeout < lease, and 1ms <= retry base <= cap required")
	}
	return nil
}

type Worker struct {
	DB       *sql.DB
	Config   Config
	Handlers map[string]Handler // immutable after Run starts
	Logger   *log.Logger
}

func (w Worker) event(c *queue.Claim, state string) {
	if w.Logger != nil {
		w.Logger.Printf("worker=%s job=%d attempt=%d state=%s", w.Config.WorkerID, c.JobID, c.Attempt, state)
	}
}

// Run processes one job at a time. Independent processes can compete safely.
// Database errors fail closed; a supervisor may restart the worker. Handlers
// must honor cancellation and make external effects idempotent.
func (w Worker) Run(ctx context.Context) error {
	if w.DB == nil {
		return errors.New("database connection is nil")
	}
	if err := w.Config.Validate(); err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		op, cancel := context.WithTimeout(ctx, w.Config.OperationTimeout)
		claim, err := queue.ClaimNext(op, w.DB, w.Config.WorkerID, w.Config.Lease)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("worker claim: %w", err)
		}
		if claim == nil {
			timer := time.NewTimer(w.Config.Poll)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
			continue
		}
		w.event(claim, "leased")
		err = w.process(ctx, claim)
		if errors.Is(err, queue.ErrLeaseLost) {
			w.event(claim, "lease_lost")
			continue
		}
		if err != nil {
			return err
		}
	}
}

type outcome struct {
	result json.RawMessage
	err    error
}

func invoke(ctx context.Context, handler Handler, claim *queue.Claim) (o outcome) {
	defer func() {
		if recover() != nil {
			o = outcome{err: errors.New("handler panicked")}
		}
	}()
	if handler == nil {
		return outcome{err: fmt.Errorf("%w: unsupported job type", ErrPermanent)}
	}
	o.result, o.err = handler(ctx, claim)
	if o.err == nil && !json.Valid(o.result) {
		o.err = fmt.Errorf("%w: handler returned invalid JSON", ErrPermanent)
	}
	return o
}

func (w Worker) process(parent context.Context, c *queue.Claim) error {
	// Keep heartbeats and work alive during the drain window, but cancellation
	// must also interrupt an in-flight database operation when grace expires.
	work, cancel := context.WithCancel(context.WithoutCancel(parent))
	defer cancel()
	go func() {
		select {
		case <-work.Done():
			return
		case <-parent.Done():
			timer := time.NewTimer(w.Config.ShutdownGrace)
			defer timer.Stop()
			select {
			case <-work.Done():
				return
			case <-timer.C:
				cancel()
			}
		}
	}()
	results := make(chan outcome, 1)
	go func() { results <- invoke(work, w.Handlers[c.Type], c) }()
	ticker := time.NewTicker(w.Config.Heartbeat)
	defer ticker.Stop()
	operationError := func(err error) error {
		if work.Err() != nil && parent.Err() != nil {
			return ErrShutdownTimeout
		}
		return err
	}
	for {
		select {
		case <-work.Done():
			return ErrShutdownTimeout
		case <-ticker.C:
			op, stop := context.WithTimeout(work, w.Config.OperationTimeout)
			_, err := queue.Renew(op, w.DB, c.JobID, c.Token, w.Config.WorkerID, w.Config.Lease)
			stop()
			if err != nil {
				return operationError(err)
			}
		case o := <-results:
			op, stop := context.WithTimeout(work, w.Config.OperationTimeout)
			if o.err == nil {
				err := queue.Complete(op, w.DB, c.JobID, c.Token, w.Config.WorkerID, o.result)
				stop()
				if err != nil {
					return operationError(err)
				}
				w.event(c, "succeeded")
			} else {
				reason := o.err.Error()
				if strings.TrimSpace(reason) == "" {
					reason = "handler failed without a message"
				}
				state, err := queue.Fail(op, w.DB, c.JobID, c.Token, w.Config.WorkerID, reason,
					w.Config.RetryBase, w.Config.RetryCap, errors.Is(o.err, ErrPermanent))
				stop()
				if err != nil {
					return operationError(err)
				}
				w.event(c, state)
			}
			return nil
		}
	}
}

// Echo is the built-in demonstration handler. Delay simulates cancellable
// work without performing any external effect; the JSON payload is returned.
func Echo(delay time.Duration) Handler {
	return func(ctx context.Context, c *queue.Claim) (json.RawMessage, error) {
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return c.Payload, nil
	}
}
