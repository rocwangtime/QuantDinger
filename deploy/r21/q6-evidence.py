"""Read only supplied public telemetry; extract byte counters, sockets and memory."""
import json
from pathlib import Path
import re
from datetime import datetime, timezone, timedelta

ROOT = Path('/workspace/quantdinger/deploy/p35/h0922_r20')
OUT = Path(__file__).parent
status = json.loads((ROOT/'observer_status_full.json').read_text())
transport, ports, monitors = [], {}, []
for n, line in enumerate((ROOT/'observer_docker_0818_0925_sgt.dechunked.log').read_text().splitlines(), 1):
    if 'evidence=' not in line:
        continue
    evidence = json.JSONDecoder().raw_decode(line.split('evidence=', 1)[1])[0]
    transport.append(dict(line=n, **evidence))
    for sample in evidence.get('samples', []):
        if sample.get('local_port'):
            ports[sample['local_port']] = evidence['source']
for line in (ROOT/'p51_mon_since_0815_sgt.log').read_text().splitlines():
    if line.startswith('==='):
        at = line[4:23]
    if line.startswith('S|'):
        monitors.append(dict(at=at, **json.loads(line[2:])))
    if line.startswith('G|') and monitors:
        monitors[-1]['memory'] = json.loads(line[2:]).get('mem', {})
    if line.startswith('B|'):
        for row in json.loads(line[2:]).values():
            port = row.get('transport', {}).get('local_port')
            if port:
                ports[port] = row['source']
windows = []
for name, start, end in [('pm_ss_0822_0824_sgt.txt','00:23:30','00:23:34'),
                         ('pm_ss_0855_0858_sgt.txt','00:55:59','00:56:04'),
                         ('pm_ss_0855_0858_sgt.txt','00:56:07','00:56:36'),
                         ('pm_ss_0916_0918_sgt.txt','01:17:15','01:17:19')]:
    sockets = {}
    for line in (ROOT/name).read_text().splitlines():
        if line.startswith('@ '):
            at = line.split()[1]
        if not start <= at[11:19] <= end:
            continue
        if re.match(r'\d+\s+\d+\s+', line):
            fields = line.split()
            port = int(fields[2].rsplit(':', 1)[1])
            pending = int(fields[0])
        elif 'bytes_received:' in line:
            match = re.search(r'bytes_received:(\d+)',line)
            if match:
                sockets.setdefault(port, []).append(dict(at=at, bytes=int(match[1]), pending=pending,
                    lastrcv_ms=int(m[1]) if (m := re.search(r'lastrcv:(\d+)', line)) else None))
    windows.append(dict(file=name, start=start, end=end, sockets=[dict(port=p,source=ports.get(p),
        count=len(rows), byte_delta=rows[-1]['bytes']-rows[0]['bytes'],first=rows[0],last=rows[-1],
        max_pending=max(r['pending'] for r in rows)) for p,rows in sockets.items()]))
result = dict(input_root=str(ROOT),failures=status['ws_hot_primary_failures'],
    liveness=status['ws_liveness_silence'],r5_transport=next(e for e in transport if e['source']=='feed-30011'),
    socket_windows=windows,memory_samples=[dict(at=r['at'],rss=r['rss'],memory=r.get('memory')) for r in monitors])
(OUT/'q6-production-evidence.json').write_text(json.dumps(result,indent=2)+'\n')
for w in windows:
    print(w['start'],w['end'],[(s['port'],s['source'],s['byte_delta'],s['max_pending']) for s in w['sockets']])
