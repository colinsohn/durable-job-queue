#!/usr/bin/env bash
# Black-box API + worker check. Requires a disposable PostgreSQL URL, Go, psql,
# Python 3, and permission to create schemas. No application tables are changed.
set -euo pipefail
: "${QUEUE_TEST_DSN:?set QUEUE_TEST_DSN to a disposable PostgreSQL URL}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
queue_smoke_schema="queue_worker_smoke_$$"
queue_smoke_tmp="$(mktemp -d)"
api_pid=''
worker_pid=''
cleanup() {
  if [[ -n "$worker_pid" ]]; then kill "$worker_pid" 2>/dev/null || true; wait "$worker_pid" 2>/dev/null || true; fi
  if [[ -n "$api_pid" ]]; then kill "$api_pid" 2>/dev/null || true; wait "$api_pid" 2>/dev/null || true; fi
  psql "$QUEUE_TEST_DSN" -X -q -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS $queue_smoke_schema CASCADE" >/dev/null
  rm -rf "$queue_smoke_tmp"
}
trap cleanup EXIT
psql "$QUEUE_TEST_DSN" -X -q -v ON_ERROR_STOP=1 -c "CREATE SCHEMA $queue_smoke_schema"
export PGOPTIONS="-c search_path=$queue_smoke_schema,public"
psql "$QUEUE_TEST_DSN" -X -q -v ON_ERROR_STOP=1 -f "$root/db/schema.sql" -f "$root/db/lease.sql" -f "$root/db/failure.sql" -f "$root/db/failure.sql"
# Convert the URL using structured parsing; do not print the DSN.
DATABASE_URL="$(python3 - "$QUEUE_TEST_DSN" "$queue_smoke_schema" <<'PY'
import sys,urllib.parse
u=urllib.parse.urlsplit(sys.argv[1])
if u.scheme not in ('postgres','postgresql'):raise ValueError('PostgreSQL URL required')
q=urllib.parse.parse_qs(u.query);q['search_path']=[sys.argv[2]]
print(urllib.parse.urlunsplit(u._replace(query=urllib.parse.urlencode(q,doseq=True))))
PY
)"
export DATABASE_URL
# Select a currently free local port; the processes use no public listener.
queue_smoke_port="$(python3 - <<'PY'
import socket
with socket.socket() as s:
 s.bind(('127.0.0.1',0));print(s.getsockname()[1])
PY
)"
(cd "$root" && go build -o "$queue_smoke_tmp/api" ./cmd/queue-api && go build -o "$queue_smoke_tmp/worker" ./cmd/queue-worker)
LISTEN_ADDR="127.0.0.1:$queue_smoke_port" "$queue_smoke_tmp/api" >"$queue_smoke_tmp/api.log" 2>&1 &
api_pid=$!
"$queue_smoke_tmp/worker" --id smoke --lease 600ms --heartbeat 100ms --operation-timeout 100ms --shutdown-grace 3s --echo-delay 1200ms >"$queue_smoke_tmp/worker.log" 2>&1 &
worker_pid=$!
python3 - "$queue_smoke_port" "$worker_pid" <<'PY'
import json,os,signal,sys,time,urllib.error,urllib.request
base='http://127.0.0.1:'+sys.argv[1]
def get(path):
 with urllib.request.urlopen(base+path,timeout=3) as r:return json.load(r)
def submit(key):
 req=urllib.request.Request(base+'/jobs',data=b'{"type":"echo","payload":{"message":"worker smoke"}}',headers={'Content-Type':'application/json','Idempotency-Key':key})
 with urllib.request.urlopen(req,timeout=3) as r:
  assert r.status==201
  return json.load(r)['id']
def wait_for(path,state):
 for _ in range(500):
  value=get(path)
  if value['state']==state:return value
  time.sleep(.01)
 raise RuntimeError('job did not become '+state)
for _ in range(300):
 try:get('/jobs/999999')
 except urllib.error.HTTPError as e:
  if e.code==404:break
 except OSError:time.sleep(.01)
else:raise RuntimeError('API did not start')
first=submit('first');wait_for('/jobs/'+str(first),'leased')
second=submit('second')
os.kill(int(sys.argv[2]),signal.SIGTERM)
j=wait_for('/jobs/'+str(first),'succeeded')
assert j['result']=={'message':'worker smoke'} and j['last_error'] is None and j['finished_at']
other=get('/jobs/'+str(second))
assert other['state']=='queued' and other['attempts_started']==0
print('PASS: live API/worker, heartbeat-spanning echo, SIGTERM drain, result lookup, no new claim after shutdown')
PY
wait "$worker_pid"
worker_pid=''
kill -TERM "$api_pid"
wait "$api_pid"
api_pid=''
