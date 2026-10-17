"""Rebuild the historical comparison from preserved local evidence."""
import json
from datetime import datetime
from pathlib import Path

root = Path(__file__).resolve().parent
selected = []
for line in (root / 'historical_lifecycle.jsonl').read_text().splitlines():
    row = json.loads(line)
    if any(t in row['at'] for t in ('18:40:', '19:03:', '23:42:')):
        selected.append(row)
line = next(line for line in (root / 'production_lifecycle.txt').read_text().splitlines() if 'evidence=' in line)
outer_prefix, raw = line.split(' ', 1)
message = json.loads(raw)['message'].split('evidence=', 1)[1]
# docker logs inserted the same outer timestamp inside a long application line.
# Preserve the raw file; only remove exact repeated prefix tokens in this derived summary.
insertions = message.count(outer_prefix + ' ')
evidence = json.loads(message.replace(outer_prefix + ' ', ''))
samples = evidence.pop('samples')
summary = {'historical_events': selected, 'production_lost': evidence,
           'normalization': {'removed_exact_token': outer_prefix + ' ', 'occurrences': insertions},
           'history_samples': len(samples), 'history_start': samples[0]['at'], 'history_end': samples[-1]['at']}
summary['history_ranges'] = {}
for key in ('tcp_pending_bytes', 'rcv_wnd', 'rcvbuf_bytes', 'read_bytes_per_second', 'sample_gap_seconds', 'pong_age_seconds'):
    values = [row[key] for row in samples if key in row]
    summary['history_ranges'][key] = {'min': min(values), 'max': max(values), 'last': values[-1], 'samples': len(values)}
lost = [row for row in selected if '23:42:' in row['at'] and row['message'].startswith('Connection to remote')]
summary['lost_pair_gap_seconds'] = (datetime.fromisoformat(lost[1]['at']) - datetime.fromisoformat(lost[0]['at'])).total_seconds()
(root / 'historical_comparison.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False) + '\n')
print(json.dumps({k:v for k,v in summary.items() if k!='historical_events'},indent=2))
