"""Serial 60s growing-delay regressions and controls, confined to four CPUs."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
root=Path(__file__).resolve().parents[2]
out=root/'deploy/r22/lag-comparison';out.mkdir(exist_ok=True)
script=root/'backend_api_python/scripts/polymarket_ws_lag_replay.py'
baseline=Path('/tmp/pm-r22-baseline/backend_api_python')
(baseline/'scripts'/script.name).write_bytes(script.read_bytes())
assert (root/'deploy/r22/suite-summary.json').exists(), 'full suite must pass first'
env=dict(os.environ,PYTHONPATH='.',SKIP_STARTUP_HOOKS='1',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
def confine():
    os.sched_setaffinity(0,sorted(os.sched_getaffinity(0))[:4])
rows=[]
for phase,backend,args in [('before',baseline,[]),('after',root/'backend_api_python',[]),('quiet',root/'backend_api_python',['--quiet']),('pressure',root/'backend_api_python',['--seconds','10','--pressure'])]:
    start=time.monotonic()
    run=subprocess.run([sys.executable,'scripts/'+script.name,*args],cwd=backend,env=env,capture_output=True,text=True,preexec_fn=confine)
    (out/(phase+'.stderr')).write_text(run.stderr)
    (out/(phase+'.stdout')).write_text(run.stdout)
    try: result=json.loads(run.stdout.splitlines()[-1])
    except (IndexError,ValueError):result={'valid':False}
    row=dict(phase=phase,exit=run.returncode,wall_seconds=time.monotonic()-start,result=result)
    rows.append(row);(out/(phase+'.json')).write_text(json.dumps(row,indent=2)+'\n')
    print(phase,'exit',run.returncode,'valid',result.get('valid'),'gap',result.get('max_publication_gap_ms'),'reason',result.get('reason'),'classification',result.get('classification'),flush=True)
(out/'results.json').write_text(json.dumps(rows,indent=2)+'\n')
a={r['phase']:r for r in rows}
assert all(r['exit']==0 and r['result']['valid'] for r in rows)
assert a['before']['result']['max_publication_gap_ms']>1500
assert a['before']['result']['primary_min_bytes_per_second']>=300000
assert a['after']['result']['max_publication_gap_ms']<=1500
assert a['after']['result']['reason']=='upstream_lag'
assert not a['quiet']['result']['switches']
assert a['pressure']['result']['reason']=='ingress_pressure'
assert a['pressure']['result']['classification']=='ingress_pressure'
