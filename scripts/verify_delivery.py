"""Real subprocess outage/SIGKILL/rotation/replay verification and measured report."""
import argparse
import json
import os
from pathlib import Path
import socket
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parent.parent


def wait_until(predicate, label, timeout=40):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if predicate():
                return
        except (OSError, sqlite3.Error):
            pass
        time.sleep(.05)
    raise AssertionError('Timeout: ' + label)


def run(count):
    with tempfile.TemporaryDirectory() as directory:
        directory = Path(directory)
        db, state, logfile = [directory / name for name in ('server.db', 'agent.db', 'app.log')]
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        url = f'http://127.0.0.1:{port}'
        env = dict(os.environ, PORT=str(port), LOG_SENTINEL_DB_PATH=str(db), LOG_SENTINEL_TOKEN='')
        processes = []
        output = (directory / 'process.log').open('w')
        def start(args):
            process = subprocess.Popen([sys.executable, *args], cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT)
            processes.append(process)
            return process
        def rows():
            with sqlite3.connect(db) as connection:
                return connection.execute("SELECT message,event_id FROM logs WHERE source='experiment'").fetchall()
        def pending():
            with sqlite3.connect(state) as connection:
                return connection.execute('SELECT count(*) FROM outbox').fetchone()[0]
        def launch_agent():
            return start(['agent.py', '--file', str(logfile), '--server', url, '--source', 'experiment',
                          '--state', str(state), '--batch-size', '100', '--batch-interval', '.05'])
        def healthy():
            with urllib.request.urlopen(url + '/api/rules', timeout=1) as response:
                return response.status == 200
        try:
            # Collect while the server is absent, kill the agent, restart with the same outbox.
            logfile.write_text(''.join(f'event-{i}\n' for i in range(100)))
            agent = launch_agent()
            wait_until(lambda: pending() == 100, 'durable collection during outage')
            agent.kill()
            agent.wait()
            agent = launch_agent()
            server = start(['server.py'])
            wait_until(healthy, 'server startup')
            wait_until(lambda: len(rows()) == 100, 'crash recovery delivery')
            # Drain a renamed file and collect an already populated replacement.
            logfile.rename(directory / 'app.log.1')
            logfile.write_text('event-100\n')
            wait_until(lambda: len(rows()) == 101, 'rename rotation')
            logfile.write_text('event-101\n')
            wait_until(lambda: len(rows()) == 102, 'same-size truncate/regrow')
            # Live delivery sample: producer append -> visible durable server row.
            latencies = []
            begin = time.monotonic()
            for index in range(20):
                tick = time.monotonic()
                with logfile.open('a') as stream:
                    stream.write(f'latency-{index}\n')
                wait_until(lambda: any(message == f'latency-{index}' for message, _ in rows()), 'latency event')
                latencies.append((time.monotonic() - tick) * 1000)
            latency_elapsed = time.monotonic() - begin
            begin = time.monotonic()
            with logfile.open('a') as stream:
                stream.writelines(f'bulk-{index}\n' for index in range(count))
            expected = count + 122
            wait_until(lambda: len(rows()) == expected, 'bulk drain')
            elapsed = time.monotonic() - begin
            # Simulate a response lost after durable commit by replaying identical IDs.
            message, event_id = rows()[0]
            payload = [{'timestamp': '2026-01-01T00:00:00Z', 'level': 'INFO', 'message': message,
                        'source': 'experiment', 'event_id': event_id}]
            request = urllib.request.Request(url + '/api/logs/ingest', data=json.dumps(payload).encode(),
                                             headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(request) as response:
                acknowledgement = json.load(response)
            assert acknowledgement['duplicates'] == 1 and acknowledgement['acknowledged_ids'] == [event_id]
            actual = rows()
            unique = len({message for message, _ in actual})
            assert len(actual) == expected == unique
            wait_until(lambda: pending() == 0, 'acknowledged outbox drained')
            return {'expected_events': expected, 'stored_events': len(actual), 'missing_events': expected - unique,
                    'duplicate_rows': len(actual) - unique, 'replay_duplicates_suppressed': 1,
                    'bulk_events': count, 'bulk_seconds': elapsed, 'bulk_events_per_second': count / elapsed,
                    'latency_samples': 20, 'latency_p50_ms': statistics.median(latencies),
                    'latency_p95_ms': sorted(latencies)[18], 'latency_poll_resolution_ms': 50,
                    'scenarios': ['server unavailable', 'agent SIGKILL and restart', 'rename rotation',
                                  'same-size truncate/regrow', 'post-commit replay'],
                    'scope': 'Single local run; latency includes 50ms observation polling; throughput includes file collection and alert evaluation.'}
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            output.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--events', type=int, default=1000)
    parser.add_argument('--output')
    args = parser.parse_args()
    if args.events < 1:
        parser.error('events must be positive')
    result = json.dumps(run(args.events), indent=2, ensure_ascii=False)
    if args.output:
        Path(args.output).write_text(result + '\n')
    print(result)
