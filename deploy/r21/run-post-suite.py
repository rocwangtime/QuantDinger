"""Run all performance/hand-off/memory evidence serially after the PG18 suite."""
import json,os,subprocess,time
from pathlib import Path
root=Path('/workspace/quantdinger/wt_r21');py='/tmp/pm-r20-venv/bin/python';out=root/'deploy/r21'
env=dict(os.environ,PYTHONPATH='.',SKIP_STARTUP_HOOKS='1',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
summary=json.loads((out/'suite-summary.json').read_text())
assert summary['failed']==0 and summary['passed']>5000
steps=[['deploy/r21/prepare-baseline.py'],['deploy/r21/run-comparison.py'],['deploy/r21/prepare-capacity.py'],['deploy/r21/run-capacity.py'],['deploy/r21/run-silence-comparison.py'],['deploy/r21/run-memory-check.py']]
for step in steps:
    print('START',step[0],flush=True)
    r=subprocess.run([py,*step],cwd=root,env=env)
    if r.returncode:raise SystemExit(r.returncode)
for phase,backend in [('before',Path('/tmp/pm-r21-baseline/backend_api_python')),('after',root/'backend_api_python')]:
    r=subprocess.run([py,'scripts/polymarket_ws_handoff_replay.py'],cwd=backend,env=env,capture_output=True,text=True)
    (out/f'handoff-{phase}.stderr').write_text(r.stderr)
    if r.returncode:print(r.stderr);raise SystemExit(r.returncode)
    result=json.loads(r.stdout.splitlines()[-1]);(out/f'handoff-{phase}.json').write_text(json.dumps(result,indent=2)+'\n')
    print('HANDOFF',phase,result['event']['merged_gap_ms'],result['rest_requests'],result['valid'],flush=True)
r=subprocess.run([py,'scripts/polymarket_ws_backup_cost.py'],cwd=root/'backend_api_python',env=env,capture_output=True,text=True)
if r.returncode:print(r.stderr);raise SystemExit(r.returncode)
(out/'backup-cost-final.json').write_text(json.dumps(json.loads(r.stdout),indent=2)+'\n')
print('ALL POST-SUITE VALIDATIONS COMPLETED',flush=True)
