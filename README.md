# Durable Job Queue

A Go and PostgreSQL job queue for running background jobs when workers can fail or restart. A reproducible process-level regression runs competing workers, kills one after its claim, and verifies recovery after real lease expiry.

## Current status

Implemented:
- Capped retry policy with unit tests.
- Actual SIGKILL recovery/exhaustion scenarios with a measured attempt-history report.
- Exhausted-lease sweep regressions for 105 jobs, healthy-work progress, and stale-write rejection.
- Runnable Go worker with heartbeats, bounded graceful shutdown, panic containment, and explicit retry/terminal failure handling.
- Token-fenced failure transitions, database-authoritative attempt limits, and preserved attempt errors.
- Black-box API/worker smoke test covering real SIGTERM drain and result lookup.
- Runnable submission/status HTTP API with PostgreSQL-backed idempotency.
- Real PostgreSQL HTTP tests for 24 concurrent submissions, equivalent replays, conflicts, status lookup, and request validation.
- PostgreSQL job and attempt schema with indexes and bounded attempts.
- Transactional `queue_claim` function using `FOR UPDATE SKIP LOCKED` and expiring leases.
- A new token per claim, with retained attempt history; an expired final attempt becomes a visible terminal failure.
- Token- and owner-checked lease renewal and successful completion, with atomic job/attempt updates.
- PostgreSQL lifecycle tests for expiry, stale tokens, concurrent completion, lock waits, and rollback.
- Go `ClaimNext` wrapper (accepts a standard `database/sql` PostgreSQL connection), with stub tests.
- PostgreSQL integration script covering two competing claimers, lease recovery, token rotation, and exhausted claims.

Remaining work: diagnostics/load benchmark and the portfolio demo walkthrough. Lease operations are Go worker primitives; no worker control endpoints or HTTP authentication are exposed.

## Run tests

With Go 1.23 or later:

```sh
go test ./...
go test -race ./...
```

For real PostgreSQL concurrency and lease tests, connect to a disposable **PostgreSQL 13+** database with permission to create schemas. Install `psql` and run:

```sh
export QUEUE_TEST_DSN='postgresql://localhost:5432/queue_test?sslmode=disable'
./tests/claim_integration.sh
```

Alternatively, use standard `PGHOST`, `PGUSER`, `PGDATABASE`, etc. The script creates a uniquely named schema and deletes it on exit. It does not modify `public` tables, but should still run against a disposable test database. The schema is in [`db/schema.sql`](db/schema.sql).

The API registers the `lib/pq` PostgreSQL driver. The integration script tests the underlying PostgreSQL function directly and does not require Go dependencies.

## Delivery semantics

The queue provides **at-least-once** execution, not exactly-once effects. A worker may finish an external operation just as its lease expires; another worker can reclaim the same job. Callers must use an idempotency key or unique business transaction identifier in their downstream database/API, and should commit the business side effect and its deduplication record atomically where possible. A lease token fences renewal, completion, and failure writes *to the queue*; it cannot undo a payment or notification that already occurred.

## Run the API

Create an empty PostgreSQL 13+ database and apply the schema once:

```sh
export DATABASE_URL='postgresql://localhost:5432/queue_demo?sslmode=disable'
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/schema.sql -f db/lease.sql -f db/failure.sql
go run ./cmd/queue-api
```

The API defaults to `127.0.0.1:8080` (override with `LISTEN_ADDR`). It is a local demo without authentication. The built-in `echo` job returns its payload as the result when a worker is running.

```sh
curl -i http://127.0.0.1:8080/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: example-1' \
  -d '{"type":"echo","payload":{"message":"hello"}}'
curl -i http://127.0.0.1:8080/jobs/1
```

- New submissions return `201` with a `Location` header. Equivalent replays return `200` with the original job's current state. A changed request under the same key returns `409`.
- JSONB equality ignores object key order, whitespace, and equivalent JSON numbers; array order matters. Keys stay reserved for the life of a job. There is no deletion endpoint.
- A nonblank key up to 200 bytes and a JSON body with `type` and `payload` are required. JSON null is a valid payload. Unknown fields, unsupported types, extra JSON values, and bodies over 1 MiB return `400`.
- Status lookup returns the job, attempt count, JSON result, last failure reason, and terminal timestamp; missing IDs return `404`, invalid IDs return `400`. Tokens and owner details are not exposed.
- Submissions use schema defaults: three attempts, zero priority, immediate eligibility. Scheduling options are not yet exposed.
- SIGINT/SIGTERM allows up to five seconds for HTTP shutdown. Keep `DATABASE_URL` outside version control.

Set `QUEUE_TEST_DSN` to a disposable PostgreSQL **URL** before `go test -v ./...` and `go test -race ./...` to run the HTTP/database integration test. It creates and drops its own schema; without that variable, the integration test is explicitly skipped. The claim shell fixture uses the same database variable.

## Worker lease primitives

Apply `db/lease.sql` after `db/schema.sql` for a new database. To upgrade a database that already has the job/attempt tables, apply only `db/lease.sql`; it uses `CREATE OR REPLACE FUNCTION` and can be reapplied. No table rewrite or data deletion is needed.

Workers use the `internal/queue` functions with a PostgreSQL `*sql.DB`:

```go
claim, err := queue.ClaimNext(ctx, db, "worker-A", 30*time.Second)
// Check err and claim != nil before using the token.
until, err := queue.Renew(ctx, db, claim.JobID, claim.Token, "worker-A", 30*time.Second)
// Check err; until is the database's authoritative lease deadline.
err = queue.Complete(ctx, db, claim.JobID, claim.Token, "worker-A", json.RawMessage(`{"ok":true}`))
```

Renewal and completion require the same job ID, worker ID, and current token, with an unexpired lease. They lock the job first and then check the database clock, including time spent waiting for the lock. A heartbeat extends to at least database-now plus the requested duration; a short heartbeat never shortens an existing deadline. Both job and attempt lease deadlines update in one transaction.

`Complete` atomically records the JSON result, marks the attempt and job succeeded, and clears the job's lease fields. JSON null is permitted. An unknown job, wrong owner/token, expired lease, or already completed attempt returns `queue.ErrLeaseLost`. Duplicate completion cannot overwrite a recorded result. Workers must stop committing queue results after losing their lease; retrying a successful completion whose response was lost also returns `ErrLeaseLost`, so inspect job status if needed.

`QUEUE_TEST_DSN=... go test -count=1 -v ./...` runs the lifecycle tests as well as the submission tests. Each uses an isolated schema. The lifecycle checks include 16 simultaneous completions (one winner), reclaiming with a new token, rejected stale writes before and after reclaim, both operations blocked behind a row lock until expiry, and rollback of a partial renewal if its attempt is missing. Tests do not simulate external exactly-once side effects. Explicit failure and worker-loop tests are also included; the actual killed-process recovery demo is the next milestone.

## Run a worker

After applying all three SQL files, keep the API running in one terminal and start a worker in another:

```sh
export DATABASE_URL='postgresql://localhost:5432/queue_demo?sslmode=disable'
go run ./cmd/queue-worker --id worker-A
```

Use the API curl submission above, then poll its `Location` URL until `state` becomes `succeeded`. `result` contains the echo payload and `finished_at` is set. Start another independent process with `--id worker-B` to compete for jobs; each process handles one active claim at a time. `--echo-delay 2s` simulates cancellable work to make lease/stop behavior visible without performing an external effect. For process-level signal experiments, build the binary first (`go build -o /tmp/queue-worker ./cmd/queue-worker`) and run that binary rather than signaling the `go run` parent.

Defaults: a 30s lease, heartbeat every 10s, 250ms empty-queue polling, 2s database-operation timeout, 5s shutdown grace, 1s first retry delay, and 30s retry cap. Flags of the same names (`--lease`, `--heartbeat`, `--poll`, `--operation-timeout`, `--shutdown-grace`, `--retry-base`, `--retry-cap`) accept Go durations. Heartbeat plus operation timeout must be less than the lease. Use distinct worker IDs for clear logs.

On SIGINT/SIGTERM, the worker stops new claims, continues heartbeat renewal, and lets its active handler finish within the grace window. At grace expiry it cancels the handler and stops writing outcomes; unfinished work is left for lease-expiry recovery. A second signal uses the OS default action. Handlers must honor context cancellation; Go cannot forcibly stop an uncooperative handler goroutine. External side effects remain subject to at-least-once execution.

A lost lease cancels that handler and lets the worker move on. A database operation error stops the worker instead of guessing ownership or persisting an uncertain outcome; a supervisor can restart it. A timed-out database call may already have committed; inspect persisted status rather than assuming an error means rollback. The CLI reports a generic error and exits nonzero on database failure or shutdown timeout. Logs contain worker/job/attempt IDs and state transitions rather than payloads, tokens, or raw database errors.

## Failures and retries

Apply `db/failure.sql` to upgrade an existing database that already has the job tables and lease functions. It uses `CREATE OR REPLACE FUNCTION`, can be reapplied, and does not rewrite tables. New databases need `schema.sql`, `lease.sql`, then `failure.sql`.

`queue.Fail` locks the job and checks its owner, token, and database-clock expiry before closing the running attempt. A retryable error changes the parent back to `queued` with delay `min(cap, base * 2^(failed_attempt-1))`. The failed attempt stays visible with its completion timestamp and reason. Reclaims are blocked until `run_at`. The authoritative `max_attempts` is read from the locked job row, counting all starts including crashed attempts; the submission API currently uses the schema default of three. A final failure clears the lease, records `finished_at`, and leaves the job in visible `failed` state.

Handlers can wrap `worker.ErrPermanent` to fail immediately without consuming the remaining starts. Unsupported job types and invalid JSON results are permanent failures. Panics are converted to the bounded retryable reason `handler panicked`; the panic value is not stored. Explicit failure reasons are limited to 2,000 Unicode characters and appear in the status API. Successful completion clears the parent's last error while preserving prior failed-attempt history. Duplicate, expired, and stale-token failure writes return `queue.ErrLeaseLost` and cannot overwrite state.

## Verify the worker

With a disposable PostgreSQL URL and `psql`, Go, and Python 3 installed:

```sh
export QUEUE_TEST_DSN='postgresql://localhost:5432/queue_test?sslmode=disable'
go test -count=1 -v ./...
go test -count=1 -race ./...
go vet ./...
./tests/claim_integration.sh
./tests/worker_smoke.sh
```

The Go integration tests use unique schemas. They verify heartbeat-spanning echo execution, retry-to-success, capped retry scheduling, exhausted/permanent failures, panic and invalid-result handling, shutdown drain with no next claim, bounded shutdown cancellation, lease-loss cancellation, failure rollback, and failure expiry while blocked behind a row lock. The smoke script builds and starts the actual API and worker binaries against its own schema, sends SIGTERM during a 1.2s echo with a 600ms lease, checks the returned JSON result, leaves a second job queued, and removes its temporary schema/binaries. It also reapplies the failure migration to check upgrade repeatability. These checks passed with Go 1.23.12/PostgreSQL 16.2. The separate SIGKILL regression below now verifies killed-worker recovery; no throughput benchmark is claimed.

## Reproduce an actual worker crash

With the same disposable `QUEUE_TEST_DSN`, Go, `psql`, and Python 3:

```sh
python3 -B tests/crash_recovery.py
# To retain a small measured report after every scenario passes:
python3 -B tests/crash_recovery.py --report reports/crash_recovery.json
```

The harness creates a unique schema, applies the SQL, builds the real worker binary, and signals only child processes it starts. It uses a 1s lease, 200ms heartbeat, and 200ms database-operation timeout. **No lease clocks or database expiry timestamps are changed** in these process-level scenarios. Temporary workers, logs, binaries and the schema are cleaned up; connection URLs and live claim tokens are not printed or saved in the report. Go tests separately use synthetic expired rows to check database edge cases.

| Scenario | Observed result |
| --- | --- |
| Competing worker while the first worker heartbeats | Attempt 1 remains with the live owner; a heartbeat extension is witnessed |
| SIGKILL the first worker | The survivor waits for actual expiry, claims attempt 2 with a different token, and succeeds |
| Old owner tries renewal, completion, and failure | All are rejected while the survivor's new lease is live |
| SIGKILL both starts of a two-attempt job | Final sweep marks the job failed, preserves both expired attempts, and creates no attempt 3 |
| Fresh job after terminal sweep | The same survivor completes it; an exhausted job does not poison the worker loop |

The saved [measured report](reports/crash_recovery.json) records actual database attempt timestamps/states and configuration. Recovery asserts the second claim is at or after the expired attempt's last renewed deadline and the old attempt closes atomically with the new claim. Final exhaustion exposes `last_error = "lease expired after final attempt"`, clears lease fields, and sets `finished_at`. Timestamps are observations, not recovery-latency promises or throughput measurements. Echo has no external effect, so this does not establish exactly-once payments, notifications, or other downstream work.

Additional Go regressions check that 105 exhausted leases are swept in bounded batches of 100 plus 5 without starving a ready job. Repeated claims and late renewal/completion/failure cannot resurrect a one-attempt exhausted job. Runtime queue logic required no changes for these checks; the milestone adds reproducible proof and regression coverage.
