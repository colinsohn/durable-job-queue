#!/usr/bin/env bash
# Exercises concurrent claims and lease recovery against a real PostgreSQL 13+.
# Connection: QUEUE_TEST_DSN (optional), or standard PGHOST/PGPORT/PGUSER/PGDATABASE.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if ! command -v psql >/dev/null 2>&1; then
  echo 'PostgreSQL psql is required for this integration test' >&2
  exit 2
fi

psql_cmd() {
  if [[ -n "${QUEUE_TEST_DSN:-}" ]]; then
    psql "$QUEUE_TEST_DSN" -X -q -At -v ON_ERROR_STOP=1 "$@"
  else
    psql -X -q -At -v ON_ERROR_STOP=1 "$@"
  fi
}

assert_eq() {
  if [[ "$1" != "$2" ]]; then
    printf 'assertion failed: expected <%s>, got <%s> (%s)\n' "$2" "$1" "$3" >&2
    exit 1
  fi
}

schema="queue_integration_$$"
psql_cmd -c "CREATE SCHEMA $schema" >/dev/null
cleanup() {
  psql_cmd -c "DROP SCHEMA IF EXISTS $schema CASCADE" >/dev/null || true
  rm -f "${worker_a_file:-}" "${worker_b_file:-}"
}
trap cleanup EXIT
# Use a unique schema, not public or the application tables. Search path is
# exported so all background psql processes use the same test tables.
export PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }-c search_path=$schema,public"
psql_cmd -f "$root/db/schema.sql" >/dev/null

psql_cmd -c "INSERT INTO queue_jobs (idempotency_key, job_type, payload)
VALUES ('first', 'example', '{\"n\":1}'::jsonb),
       ('second', 'example', '{\"n\":2}'::jsonb)" >/dev/null

worker_a_file="$(mktemp)"
worker_b_file="$(mktemp)"
psql_cmd -c "SELECT job_id || ':' || attempt_no
             FROM queue_claim('worker-A', INTERVAL '5 minutes')" >"$worker_a_file" &
pid_a=$!
psql_cmd -c "SELECT job_id || ':' || attempt_no
             FROM queue_claim('worker-B', INTERVAL '5 minutes')" >"$worker_b_file" &
pid_b=$!
wait "$pid_a"
wait "$pid_b"
worker_a="$(cat "$worker_a_file")"
worker_b="$(cat "$worker_b_file")"
[[ "$worker_a" =~ ^[0-9]+:1$ ]] || { echo "invalid worker-A claim: $worker_a" >&2; exit 1; }
[[ "$worker_b" =~ ^[0-9]+:1$ ]] || { echo "invalid worker-B claim: $worker_b" >&2; exit 1; }
[[ "$worker_a" != "$worker_b" ]] || { echo 'two workers received the same claim' >&2; exit 1; }
assert_eq "$(psql_cmd -c "SELECT count(*) FROM queue_attempts")" '2' 'two attempts created'
assert_eq "$(psql_cmd -c "SELECT count(*) FROM queue_claim('worker-C', INTERVAL '5 minutes')")" '0' 'nothing claimable while leased'

first_id="${worker_a%:*}"
# Simulate a crashed worker by expiring its lease. A new worker must create
# attempt 2 for the same job, closing attempt 1 as lease_expired.
psql_cmd -c "UPDATE queue_jobs
             SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
             WHERE id = $first_id" >/dev/null
recovered="$(psql_cmd -c "SELECT job_id || ':' || attempt_no FROM queue_claim('worker-C', INTERVAL '5 minutes')")"
assert_eq "$recovered" "$first_id:2" 'expired job recovered by next worker'
assert_eq "$(psql_cmd -c "SELECT state FROM queue_attempts WHERE job_id = $first_id AND attempt_no = 1")" 'lease_expired' 'previous attempt closed'
assert_eq "$(psql_cmd -c "SELECT count(DISTINCT claim_token) FROM queue_attempts WHERE job_id = $first_id")" '2' 'recovery rotates the fencing token'

# Max attempts counts claims, including an attempt whose worker dies.
psql_cmd -c "INSERT INTO queue_jobs (idempotency_key, job_type, payload, max_attempts)
             VALUES ('one-shot', 'example', '{}'::jsonb, 1)" >/dev/null
one_shot_id="$(psql_cmd -c "SELECT id FROM queue_jobs WHERE idempotency_key = 'one-shot'")"
claimed_once="$(psql_cmd -c "SELECT job_id FROM queue_claim('worker-D', INTERVAL '5 minutes')")"
assert_eq "$claimed_once" "$one_shot_id" 'last attempt claimed once'
psql_cmd -c "UPDATE queue_jobs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'
             WHERE id = $one_shot_id" >/dev/null
assert_eq "$(psql_cmd -c "SELECT count(*) FROM queue_claim('worker-E', INTERVAL '5 minutes')")" '0' 'terminal job cannot be reclaimed'
assert_eq "$(psql_cmd -c "SELECT state FROM queue_jobs WHERE id = $one_shot_id")" 'failed' 'expired final lease is visible as failed'
assert_eq "$(psql_cmd -c "SELECT state FROM queue_attempts WHERE job_id = $one_shot_id")" 'lease_expired' 'terminal attempt history retained'
assert_eq "$(psql_cmd -c "SELECT attempts_started FROM queue_jobs WHERE id = $one_shot_id")" '1' 'max attempts respected'

echo 'PASS: competing workers, no double-claim, expired-lease recovery, unique tokens, exhausted attempts'
