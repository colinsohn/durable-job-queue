# Durable Job Queue

A Go and PostgreSQL job queue for running background work when workers can fail or restart. The planned demo starts two workers, stops one after it claims a job, and shows the other recover the job after its claim expires.

## Current status

The first release of the source includes a tested retry policy. It caps the delay between attempts and stops retrying once a job reaches its attempt limit. The PostgreSQL storage, API, and worker are under development.

Run the current tests with Go 1.23 or later.

```sh
go test ./...
```
