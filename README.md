# Durable Job Queue

A Go and PostgreSQL job queue for running background jobs when workers can fail or restart. The target demo runs two workers, kills one after it claims a job, and shows another recover it after its lease expires.

## Current status

Implemented:
- Capped retry policy with unit tests.
- PostgreSQL job and attempt schema with indexes and bounded attempts.
- Transactional `queue_claim` function using `FOR UPDATE SKIP LOCKED` and expiring leases.
- A new token per claim, with retained attempt history; an expired final attempt becomes a visible terminal failure.
- Go `ClaimNext` wrapper (accepts a standard `database/sql` PostgreSQL connection), with stub tests.
- PostgreSQL integration script covering two competing claimers, lease recovery, token rotation, and exhausted claims.

Not yet implemented: submission and status API, authenticated lease completion/renewal, worker loop, crash demo, benchmarks. **Stale-token completion is not protected until those operations are implemented and tested.**

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

`ClaimNext` uses `database/sql` and requires a PostgreSQL driver to be registered by the future application entry point. The integration script tests the underlying PostgreSQL function directly and does not require Go dependencies.

## Delivery semantics

The queue provides **at-least-once** execution, not exactly-once effects. A worker may finish an external operation just as its lease expires; another worker can reclaim the same job. Callers must use an idempotency key or unique business transaction identifier in their downstream database/API, and should commit the business side effect and its deduplication record atomically where possible. A lease token fences writes *to the queue* once completion methods are implemented; it cannot undo a payment or notification that already occurred.
