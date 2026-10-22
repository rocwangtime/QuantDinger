"""Run only after PG18 suite; serial replay matrix, lag controls and profiles."""
from pathlib import Path
import json
import os
import subprocess
import sys
root=Path(__file__).resolve().parents[2]
assert (root/'deploy/r22/suite-summary.json').exists()
env=dict(os.environ,PYTHONPATH='.',SKIP_STARTUP_HOOKS='1',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
for name in ['run-lag-comparison.py','run-comparison.py']:
    with (root/'deploy/r22'/(name+'.log')).open('w') as log:
        subprocess.run([sys.executable,str(root/'deploy/r22'/name)],cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
for label,backend in [('before',Path('/tmp/pm-r22-baseline/backend_api_python')),('after',root/'backend_api_python')]:
    script=root/'backend_api_python/scripts/polymarket_ws_warm_profile.py'
    (backend/'scripts'/script.name).write_bytes(script.read_bytes())
    with (root/'deploy/r22'/('warm-profile-final-'+label+'.txt')).open('w') as log:
        subprocess.run([sys.executable,'scripts/'+script.name,'--profile','/tmp/r22-final-'+label+'.stats'],cwd=backend,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
print('all final replay gates passed',flush=True)
