import json, os, subprocess, time
from pathlib import Path
root=Path('/workspace/quantdinger/wt_r21');out=root/'deploy/r21'
rows=[]
for phase,backend in [('before',Path('/tmp/pm-r21-baseline/backend_api_python')),('after',root/'backend_api_python')]:
    for scenario in ('four-second','promoted-primary'):
        started=time.monotonic()
        r=subprocess.run(['/tmp/pm-r20-venv/bin/python','scripts/polymarket_ws_silence_replay.py','--scenario',scenario],
            cwd=backend,env=dict(os.environ,PYTHONPATH='.',SKIP_STARTUP_HOOKS='1'),capture_output=True,text=True)
        label=f'silence-{phase}-{scenario}'
        (out/(label+'.stderr')).write_text(r.stderr)
        if not r.stdout.strip():
            print(label,r.stderr,flush=True);raise SystemExit(r.returncode or 1)
        result=json.loads(r.stdout.splitlines()[-1])
        row=dict(phase=phase,scenario=scenario,exit=r.returncode,wall_seconds=time.monotonic()-started,result=result)
        rows.append(row)
        (out/(label+'.json')).write_text(json.dumps(row,indent=2)+'\n')
        print(label,'valid',result['valid'],'gap',result['max_publication_gap_ms'],'switches',result['switches'],flush=True)
(out/'silence-comparison.json').write_text(json.dumps(rows,indent=2)+'\n')
raise SystemExit(0 if all(r['exit']==0 and r['result']['valid'] for r in rows) else 1)
