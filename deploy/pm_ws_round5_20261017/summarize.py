"""Rebuild the round-five evidence summary from immutable compressed captures."""
import gzip
import json
import re
from datetime import datetime
from itertools import pairwise
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def read(name):
    return gzip.decompress((ROOT / name).read_bytes()).decode()


def bounds(rows, key):
    values = [r[key] for r in rows if isinstance(r.get(key), (int, float))]
    return {'min': min(values), 'max': max(values)} if values else None


def main():
    events = []
    failures = []
    for line in read('observer.log.gz').splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        message = row.get('message', '')
        if 'transport failure' in message:
            evidence = json.loads(message.split('evidence=', 1)[1])
            samples = evidence['samples']
            failures.append({'at': row['timestamp'], 'evidence': evidence,
                'sample_count': len(samples), 'bounds': {k: bounds(samples, k) for k in (
                    'tcp_pending_bytes', 'rcv_wnd', 'total_retrans', 'sample_gap_seconds',
                    'scheduler_lag_seconds', 'read_bytes_per_second')}})
        if 'Polymarket WS ' in message and 'transport failure' not in message:
            events.append({'at': row['timestamp'], 'message': message})
    observations = [json.loads(line[4:]) for line in read('p36_mon.log.gz').splitlines() if line.startswith('OBS|')]
    sockets = {}
    timestamps = []
    for block in read('ss.log.gz').split('@ ')[1:]:
        lines = block.splitlines()
        at = lines[0].split()[0]
        timestamps.append(at)
        for index, line in enumerate(lines):
            match = re.match(r'\s*(\d+)\s+(\d+)\s+\S+:(\d+)\s+\S+:443\s*$', line)
            if not match or index + 1 >= len(lines):
                continue
            details = lines[index + 1]
            row = {'at': at, 'recv_q': int(match[1]), 'send_q': int(match[2])}
            for key in ('bytes_received', 'bytes_sent', 'rcv_ssthresh', 'rcv_space', 'lastsnd', 'lastrcv'):
                found = re.search(r'\b' + key + r':(\d+)', details)
                if found:
                    row[key] = int(found[1])
            drops = re.search(r',d(\d+)\)', details)
            if drops:
                row['socket_drops'] = int(drops[1])
            sockets.setdefault(match[3], []).append(row)
    windows = {}
    for name, port, start, end in (
        ('first_failure', '35950', '2026-10-17T02:30:00', '2026-10-17T02:31:00'),
        ('second_failure', '55118', '2026-10-17T03:14:27', '2026-10-17T03:15:29'),
        ('second_connection_start', '55118', '2026-10-17T03:10:07', '2026-10-17T03:10:40'),
    ):
        rows = [r for r in sockets.get(port, []) if start <= r['at'] < end]
        rates = []
        for previous, row in pairwise(rows):
            elapsed = (datetime.fromisoformat(row['at']) - datetime.fromisoformat(previous['at'])).total_seconds()
            if elapsed > 0 and 'bytes_received' in row and 'bytes_received' in previous:
                rates.append({'bytes_per_s': (row['bytes_received'] - previous['bytes_received']) / elapsed})
        windows[name] = {'port': port, 'count': len(rows), 'rows': rows,
            'bounds': {k: bounds(rows, k) for k in ('recv_q', 'send_q', 'rcv_ssthresh', 'socket_drops')},
            'bytes_per_s': bounds(rates, 'bytes_per_s')}
    promoted = [e for e in events if 'rotation promoted' in e['message']]
    report = {'ss_start': timestamps[0], 'ss_end': timestamps[-1], 'ss_sample_count': len(timestamps),
        'failures': failures, 'windows': windows, 'lifecycle': events,
        'rotation_promotions': len(promoted), 'data_collapse_promotions': sum('data_collapse' in e['message'] for e in promoted),
        'p36_shards': [{'shard': i, 'assets': bounds([{'assets': d['assets'][i]} for d in observations], 'assets'),
                         'load': bounds([{'load': d['loads'][i]} for d in observations], 'load')} for i in range(4)]}
    (ROOT / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k not in ('failures', 'windows', 'lifecycle')}, indent=2))
    for name, window in windows.items():
        print(name, json.dumps({k: v for k, v in window.items() if k != 'rows'}))


if __name__ == '__main__':
    main()
