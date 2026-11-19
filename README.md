# Durable Job Queue

A Go and PostgreSQL job queue for running background jobs when workers can fail or restart. The target demo runs two workers, kills one after it claims a job, and shows another recover it after its lease expires.

## Current status

Implemented:
- Capped retry policy with unit tests.
- Runnable submission/status HTTP API with PostgreSQL-backed idempotency.
- Real PostgreSQL HTTP tests for 24 concurrent submissions, equivalent replays, conflicts, status lookup, and request validation.
- PostgreSQL job and attempt schema with indexes and bounded attempts.
- Transactional `queue_claim` function using `FOR UPDATE SKIP LOCKED` and expiring leases.
- A new token per claim, with retained attempt history; an expired final attempt becomes a visible terminal failure.
- Go `ClaimNext` wrapper (accepts a standard `database/sql` PostgreSQL connection), with stub tests.
- PostgreSQL integration script covering two competing claimers, lease recovery, token rotation, and exhausted claims.

Not yet implemented: authenticated lease completion/renewal, worker loop, crash demo, benchmarks. **Stale-token completion is not protected until those operations are implemented and tested.**

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

The queue provides **at-least-once** execution, not exactly-once effects. A worker may finish an external operation just as its lease expires; another worker can reclaim the same job. Callers must use an idempotency key or unique business transaction identifier in their downstream database/API, and should commit the business side effect and its deduplication record atomically where possible. A lease token fences writes *to the queue* once completion methods are implemented; it cannot undo a payment or notification that already occurred.

## Run the API

Create an empty PostgreSQL 13+ database and apply the schema once:

```sh
export DATABASE_URL='postgresql://localhost:5432/queue_demo?sslmode=disable'
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/schema.sql
go run ./cmd/queue-api
```

The API defaults to `127.0.0.1:8080` (override with `LISTEN_ADDR`). It is a local demo without authentication. The first supported job type is `echo`; execution arrives with the worker milestone.

```sh
curl -i http://127.0.0.1:8080/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: example-1' \
  -d '{"type":"echo","payload":{"message":"hello"}}'
curl -i http://127.0.0.1:8080/jobs/1
```

- New submissions return `201` with a `Location` header. Equivalent replays return `200` with the original job's current state. A changed request under the same key returns `409`.
- JSONB equality ignores object key order, whitespace, and equivalent JSON numbers; array order matters. Keys stay reserved for the life of a job. There is no deletion endpoint.
- A nonblank key up to 200 bytes and a JSON body with `type` and `payload` are required. JSON null is a valid payload. Unknown fields, unsupported types, extra JSON values, and bodies over 1 MiB return `400`.
- Status lookup returns the job and attempt count; missing IDs return `404`, invalid IDs return `400`. Tokens and owner details are not exposed.
- Submissions use schema defaults: three attempts, zero priority, immediate eligibility. Scheduling options are not yet exposed.
- SIGINT/SIGTERM allows up to five seconds for HTTP shutdown. Keep `DATABASE_URL` outside version control.

Set `QUEUE_TEST_DSN` to a disposable PostgreSQL **URL** before `go test -v ./...` and `go test -race ./...` to run the HTTP/database integration test. It creates and drops its own schema; without that variable, the integration test is explicitly skipped. The claim shell fixture uses the same database variable.
