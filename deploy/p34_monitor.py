#!/usr/bin/env python3
"""Independent bounded observer/TCP sampling lanes; all remote commands are read-only."""
import argparse
import json
import os
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


def record(output, **fields):
    output.write(json.dumps(dict(at=datetime.now(timezone.utc).isoformat(), **fields)) + '\n')
    output.flush()


def run_sample(command, script, destination, timeout_seconds, metadata):
    """Kill the entire local process group, including inherited SSH pipe holders."""
    with script.open('rb') as source:
        process = subprocess.Popen(command, stdin=source, stdout=destination,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        record(metadata, event='start', pid=process.pid)
        try:
            code = process.wait(timeout=timeout_seconds)
            record(metadata, event='end', pid=process.pid, returncode=code)
            return code
        except subprocess.TimeoutExpired:
            # timeout without --kill-after can hang on a child ignoring TERM.
            # Writing straight to file also removes grep/pipe EOF dependencies.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            record(metadata, event='timeout', pid=process.pid)
            return 124
        finally:
            # A command can exit while descendants retain descriptors.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def lane(name, command, script, directory, interval, timeout_seconds, duration, stop):
    deadline = time.monotonic() + duration
    due = time.monotonic()
    with (directory / (name + '.log')).open('a', buffering=1) as output, \
            (directory / (name + '_heartbeat.jsonl')).open('a', buffering=1) as metadata:
        while not stop.is_set() and time.monotonic() < deadline:
            now = time.monotonic()
            remaining = deadline - now
            record(metadata, event='scheduled', delay_seconds=max(0, now - due))
            try:
                run_sample(command, script, output, min(timeout_seconds, remaining), metadata)
            except OSError as exc:
                record(metadata, event='error', error=str(exc))
            # Fixed cadence; missed slots are explicit, without catch-up bursts.
            due += interval
            now = time.monotonic()
            if due < now:
                missed = int((now - due) // interval) + 1
                record(metadata, event='missed', slots=missed, delay_seconds=now - due)
                due += missed * interval
            stop.wait(max(0, min(due, deadline) - now))
        record(metadata, event='done')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='qd-sg')
    parser.add_argument('--since', default='2026-10-16T18:04:03Z')
    parser.add_argument('--duration', type=float, default=1800)
    parser.add_argument('--output', type=Path, default=Path('p34_round3'))
    args = parser.parse_args()
    # Validate timestamp before interpolating it into the remote shell command.
    datetime.strptime(args.since, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
    if args.duration <= 0 or args.host.startswith('-'):
        parser.error('duration must be positive and host must not begin with -')
    args.output.mkdir(parents=True, exist_ok=True)
    scripts = Path(__file__).resolve().parent
    ssh = ['ssh', '-F', str(Path.home() / '.ssh/config'), '-T', '-o', 'BatchMode=yes',
           '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2', args.host]
    stop = threading.Event()
    threads = []
    for name, filename, interval, timeout_seconds, remote in [
            ('observer', 'p34_mon.sh', 120, 40, 'sudo -n bash -s -- ' + args.since),
            ('tcp', 'p34_tcp.sh', 65, 64, 'sudo -n bash -s')]:
        thread = threading.Thread(target=lane, args=(name, ssh + [remote], scripts / filename,
                                  args.output, interval, timeout_seconds, args.duration, stop))
        thread.start()
        threads.append(thread)
    try:
        while any(thread.is_alive() for thread in threads):
            for thread in threads:
                thread.join(.5)
    except KeyboardInterrupt:
        stop.set()
        for thread in threads:
            thread.join()


if __name__ == '__main__':
    main()
