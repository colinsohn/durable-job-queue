#!/usr/bin/env python3
"""Real worker SIGKILL/recovery regression using only Go, psql, and Python.

QUEUE_TEST_DSN must be a disposable PostgreSQL URL with schema privileges.
Every process signaled by this harness is a child it started. No clocks or
lease timestamps are changed in the database; recovery waits for real expiry.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import urllib.parse
import uuid


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


class Harness:
    def __init__(self, root, temporary, dsn):
        self.root, self.temporary = root, Path(temporary)
        self.base = dsn
        u = urllib.parse.urlsplit(dsn)
        require(u.scheme in ('postgres', 'postgresql'), 'QUEUE_TEST_DSN must be a PostgreSQL URL')
        self.schema = 'queue_crash_' + uuid.uuid4().hex
        query = urllib.parse.parse_qs(u.query, keep_blank_values=True)
        options = query.get('options', [''])[-1]
        query['options'] = [options + ' -c search_path=' + self.schema + ',public -c statement_timeout=3000']
        self.scoped = urllib.parse.urlunsplit(u._replace(query=urllib.parse.urlencode(query, doseq=True, quote_via=urllib.parse.quote)))
        self.processes, self.logs = [], []
        self.created = False
        self.environment = os.environ.copy()
        self.environment.pop('PGOPTIONS', None)

    def sql(self, statement, admin=False):
        # Capture errors privately: subprocess exceptions must never echo DSNs.
        try:
            result = subprocess.run(['psql', self.base if admin else self.scoped, '-X', '-q', '-At',
                '-v', 'ON_ERROR_STOP=1'], input=statement, text=True, capture_output=True,
                env=self.environment, timeout=10)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError('psql failed to start or timed out') from None
        require(result.returncode == 0, 'psql operation failed; inspect disposable database access/schema setup')
        return result.stdout.strip()

    def query(self, statement):
        text = self.sql('SELECT row_to_json(r) FROM (' + statement + ') r;')
        return [json.loads(line) for line in text.splitlines()] if text else []

    def setup(self):
        self.sql('CREATE SCHEMA "' + self.schema + '";', admin=True)
        self.created = True
        for name in ('schema.sql', 'lease.sql', 'failure.sql'):
            self.sql((self.root/'db'/name).read_text())
        try:
            result = subprocess.run(['go', 'build', '-o', str(self.temporary/'worker'), './cmd/queue-worker'],
                cwd=self.root, env=self.environment, capture_output=True, timeout=120)
        except (OSError, subprocess.TimeoutExpired):
            raise RuntimeError('worker build failed to start or timed out') from None
        require(result.returncode == 0, 'worker build failed')

    def worker(self, name, delay):
        stream = (self.temporary/(name+'.log')).open('wb')
        self.logs.append(stream)
        env = self.environment.copy()
        env['DATABASE_URL'] = self.scoped
        process = subprocess.Popen([str(self.temporary/'worker'), '--id', name,
            '--lease', '1s', '--heartbeat', '200ms', '--operation-timeout', '200ms',
            '--poll', '20ms', '--shutdown-grace', '300ms', '--echo-delay', delay],
            stdout=stream, stderr=stream, env=env)
        self.processes.append(process)
        return process

    def kill(self, process):
        require(process.poll() is None, 'victim worker exited before SIGKILL')
        process.kill()
        require(process.wait(timeout=5) == -signal.SIGKILL, 'victim did not exit from SIGKILL')

    def stop(self, process):
        process.terminate()
        require(process.wait(timeout=5) == 0, 'survivor did not exit cleanly')

    def job(self, key, maximum):
        # key/max are fixed harness values, not untrusted SQL inputs.
        return int(self.sql("INSERT INTO queue_jobs (idempotency_key,job_type,payload,max_attempts) "
            f"VALUES ('{key}','echo','{{\"case\":\"{key}\"}}',{maximum}) RETURNING id;"))

    def status(self, job):
        return self.query(f"SELECT id,state,attempts_started,max_attempts,lease_owner,lease_token::text, "
            f"lease_expires_at,result,last_error,finished_at FROM queue_jobs WHERE id={job}")[0]

    def wait(self, condition, description):
        deadline = time.monotonic()+15
        while time.monotonic() < deadline:
            value = condition()
            if value:
                return value
            time.sleep(.02)
        raise RuntimeError('timed out waiting for '+description)

    def claimed(self, job, owner, attempt):
        value = self.status(job)
        return value if value['state']=='leased' and value['lease_owner']==owner and value['attempts_started']==attempt else None

    def terminal(self, job, state):
        value = self.status(job)
        return value if value['state']==state else None

    def reject_old_writes(self, job, token, owner, expected_owner=None):
        # Tokens remain private in memory. The report contains booleans only.
        holder = (f"(SELECT state='leased' AND lease_owner='{expected_owner}' AND lease_expires_at>clock_timestamp() FROM queue_jobs WHERE id={job}) AS fresh_holder_live, " if expected_owner else "")
        result = self.query(f"SELECT {holder}queue_renew({job},'{token}'::uuid,'{owner}',interval '1 second') IS NULL AS renew, "
            f"NOT queue_complete({job},'{token}'::uuid,'{owner}','{{\"stale\":true}}'::jsonb) AS complete, "
            f"queue_fail({job},'{token}'::uuid,'{owner}','stale',interval '1 second',interval '2 seconds',false) IS NULL AS fail")[0]
        if expected_owner:
            require(result.pop('fresh_holder_live'), 'survivor lease was not live during stale-write check')
        require(all(result.values()), 'old claim accepted a lifecycle write')
        return result

    def recovery(self):
        job = self.job('recovery', 3)
        victim = self.worker('recover-A', '10s')
        old = self.wait(lambda:self.claimed(job,'recover-A',1), 'first recovery claim')
        survivor = self.worker('recover-B', '2s')
        # Witness an actual heartbeat extension while the competitor is alive.
        self.wait(lambda:self.query(f"SELECT lease_expires_at > claimed_at + interval '1500 milliseconds' AS extended FROM queue_attempts WHERE job_id={job} AND attempt_no=1")[0]['extended'], 'live heartbeat extension')
        live = self.status(job)
        require(live['attempts_started']==1 and live['lease_owner']=='recover-A', 'live lease was stolen')
        self.kill(victim)
        fresh = self.wait(lambda:self.claimed(job,'recover-B',2), 'survivor reclaim after actual expiry')
        require(fresh['lease_token'] != old['lease_token'], 'reclaim did not rotate token')
        rejected = self.reject_old_writes(job,old['lease_token'],'recover-A',expected_owner='recover-B')
        complete = self.wait(lambda:self.terminal(job,'succeeded'), 'recovered echo completion')
        require(complete['result']=={'case':'recovery'} and complete['attempts_started']==2, 'incorrect recovered outcome')
        history = self.query(f"SELECT attempt_no,state,worker_id,claimed_at,lease_expires_at,finished_at FROM queue_attempts WHERE job_id={job} ORDER BY attempt_no")
        require([a['state'] for a in history]==['lease_expired','succeeded'], 'recovery history incorrect')
        require(datetime.fromisoformat(history[1]['claimed_at']) >= datetime.fromisoformat(history[0]['lease_expires_at']), 'reclaimed before expiry')
        require(history[0]['finished_at']==history[1]['claimed_at'], 'expired attempt did not close atomically with reclaim')
        require(complete['lease_token'] is None and complete['finished_at'] is not None, 'successful lease not cleared')
        self.stop(survivor)
        return {'state':'succeeded','attempts_started':2,'victim_exit_signal':'SIGKILL',
            'history':history,'live_competitor_did_not_steal':True,'token_rotated':True,
            'reclaimed_not_before_expiry':True,'stale_writes_checked_while_survivor_live':True,'stale_writes_rejected':rejected}

    def exhaustion(self):
        job = self.job('exhaustion', 2)
        old_tokens = []
        for attempt in (1,2):
            owner = 'exhaust-'+str(attempt)
            victim = self.worker(owner,'10s')
            old = self.wait(lambda:self.claimed(job,owner,attempt), 'exhaustion claim '+str(attempt))
            old_tokens.append((old['lease_token'],owner))
            self.kill(victim)
        sweeper = self.worker('exhaust-sweeper','0s')
        final = self.wait(lambda:self.terminal(job,'failed'), 'terminal final lease expiry')
        require(final['attempts_started']==2 and final['max_attempts']==2, 'attempt limit exceeded')
        require(final['last_error']=='lease expired after final attempt' and final['finished_at'] is not None, 'terminal failure not visible')
        require(final['result'] is None and final['lease_token'] is None, 'terminal fields incorrect')
        for token,owner in old_tokens:
            self.reject_old_writes(job,token,owner)
        history = self.query(f"SELECT attempt_no,state,worker_id,claimed_at,lease_expires_at,finished_at FROM queue_attempts WHERE job_id={job} ORDER BY attempt_no")
        require([a['state'] for a in history]==['lease_expired','lease_expired'], 'exhaustion history lost')
        require(datetime.fromisoformat(history[1]['claimed_at']) >= datetime.fromisoformat(history[0]['lease_expires_at']), 'retry began early')
        # Repeated claims must not resurrect this exhausted job. A fresh job
        # proves the same survivor remains useful after sweeping the terminal.
        healthy = self.job('healthy-after-exhaustion',3)
        self.wait(lambda:self.terminal(healthy,'succeeded'), 'healthy follow-up')
        final_again = self.status(job)
        require(final_again['state']=='failed' and final_again['attempts_started']==2, 'exhausted job resurrected')
        self.stop(sweeper)
        return {'state':'failed','attempts_started':2,'max_attempts':2,'worker_crashes':2,
            'history':history,'no_third_attempt':True,'stale_writes_rejected':True,
            'terminal_error':final['last_error'],'healthy_follow_up_succeeded':True}

    def cleanup(self):
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
                try:process.wait(timeout=3)
                except subprocess.TimeoutExpired:process.kill();process.wait(timeout=3)
        for stream in self.logs:stream.close()
        if self.created:self.sql('DROP SCHEMA IF EXISTS "'+self.schema+'" CASCADE;',admin=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report',type=Path,help='write a small measured report only after all scenarios pass')
    args = parser.parse_args()
    dsn = os.environ.get('QUEUE_TEST_DSN','')
    require(bool(dsn), 'set QUEUE_TEST_DSN to a disposable PostgreSQL URL')
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix='queue-crash-') as tmp:
        harness = Harness(root,tmp,dsn)
        try:
            harness.setup()
            report={'format_version':1,'measured_at_utc':datetime.now(timezone.utc).isoformat(),
                'postgresql_version':harness.sql('SHOW server_version;'),
                'go_version':subprocess.check_output(['go','version'],text=True).strip(),
                'lease_seconds':1,'heartbeat_milliseconds':200,'operation_timeout_milliseconds':200,
                'recovery':harness.recovery(),'exhaustion':harness.exhaustion(),
                'limitations':['Local disposable PostgreSQL and echo handler only; no external exactly-once effects.',
                    'No manual expiry updates were used; timestamps are measurements, not latency guarantees.']}
        finally:harness.cleanup()
    if args.report:
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
    print('PASS: real SIGKILL recovery, live-lease fencing, stale writes, exhausted crash attempts, healthy follow-up')


if __name__=='__main__':main()
