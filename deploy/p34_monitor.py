#!/usr/bin/env python3
"""Independent bounded observer/TCP sampling lanes; all remote commands are read-only."""
import argparse
import json
import os
import selectors
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


def record(output, **fields):
    output.write(json.dumps(dict(at=datetime.now(timezone.utc).isoformat(), **fields)) + '\n')
    output.flush()


class RotatingLog:
    """Strict byte limit per file, including writes larger than one segment."""
    def __init__(self, path, maximum=4 * 1024 * 1024, backups=4):
        self.path, self.maximum, self.backups = path, maximum, backups
        if path.exists() and path.stat().st_size > maximum:
            with path.open('rb') as previous:
                previous.seek(-maximum, os.SEEK_END)
                tail = previous.read(maximum)
            path.write_bytes(tail)
        self.file = path.open('ab', buffering=0)

    def write(self, value):
        data = value.encode() if isinstance(value, str) else value
        while data:
            available = self.maximum - self.file.tell()
            if available <= 0:
                self.file.close()
                for index in range(self.backups, 0, -1):
                    source = self.path if index == 1 else self.path.with_name(self.path.name + f'.{index - 1}')
                    if source.exists():
                        source.replace(self.path.with_name(self.path.name + f'.{index}'))
                self.file = self.path.open('wb', buffering=0)
                available = self.maximum
            chunk, data = data[:available], data[available:]
            self.file.write(chunk)

    def flush(self):
        self.file.flush()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.file.close()


def run_sample(command, script, destination, timeout_seconds, metadata):
    """Stream bounded chunks; deadline never waits for inherited pipe EOF."""
    began = time.monotonic()
    with script.open('rb') as source:
        process = subprocess.Popen(command, stdin=source, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, start_new_session=True)
    record(metadata, event='start', pid=process.pid)
    os.set_blocking(process.stdout.fileno(), False)
    timed_out = False
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                remaining = began + timeout_seconds - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    break
                for key, _ in selector.select(min(.1, remaining)):
                    chunk = os.read(key.fd, 65536)
                    if chunk:
                        destination.write(chunk.decode(errors='replace'))
                    else:
                        selector.unregister(key.fileobj)
                if process.poll() is not None:
                    break
    finally:
        # Kill descendants even if the parent exited while they hold stdout.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=2)
        while True:
            try:
                chunk = os.read(process.stdout.fileno(), 65536)
            except BlockingIOError:
                break
            if not chunk:
                break
            destination.write(chunk.decode(errors='replace'))
        process.stdout.close()
    code = 124 if timed_out else process.returncode
    record(metadata, event='timeout' if timed_out else 'end', pid=process.pid,
           returncode=code, elapsed_seconds=time.monotonic() - began)
    return code


def lane(name, command, script, directory, interval, timeout_seconds, duration, stop,
         max_bytes=4 * 1024 * 1024, backups=4):
    deadline = time.monotonic() + duration if duration else float('inf')
    due = time.monotonic()
    with RotatingLog(directory / (name + '.log'), max_bytes, backups) as output, \
            RotatingLog(directory / (name + '_heartbeat.jsonl'), max_bytes, backups) as metadata:
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
    parser.add_argument('--duration', type=float, default=0, help='seconds; 0 runs until stopped')
    parser.add_argument('--max-bytes', type=int, default=4 * 1024 * 1024)
    parser.add_argument('--backups', type=int, default=4)
    parser.add_argument('--output', type=Path, default=Path('p34_round4'))
    args = parser.parse_args()
    # Validate timestamp before interpolating it into the remote shell command.
    datetime.strptime(args.since, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
    if args.duration < 0 or args.max_bytes < 1024 or not 1 <= args.backups <= 100 or args.host.startswith('-'):
        parser.error('duration >= 0, max-bytes >= 1024, backups 1..100; host must not begin with -')
    args.output.mkdir(parents=True, exist_ok=True)
    scripts = Path(__file__).resolve().parent
    ssh = ['ssh', '-F', str(Path.home() / '.ssh/config'), '-T', '-o', 'BatchMode=yes',
           '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2', args.host]
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    threads = []
    for name, filename, interval, timeout_seconds, remote in [
            ('observer', 'p34_mon.sh', 120, 40, 'sudo -n bash -s -- ' + args.since),
            ('tcp', 'p34_tcp.sh', 50, 49, 'sudo -n timeout -k 1 47 bash -s')]:
        thread = threading.Thread(target=lane, args=(name, ssh + [remote], scripts / filename,
                                  args.output, interval, timeout_seconds, args.duration, stop, args.max_bytes, args.backups))
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
