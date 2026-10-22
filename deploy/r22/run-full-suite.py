"""Run serial pytest groups against PG18; isolate slow and clock-sensitive modules."""
import ast
import json
import os
from pathlib import Path
import re
import subprocess
import time

ROOT = Path('/workspace/quantdinger/wt_r22')
BACKEND = ROOT / 'backend_api_python'
OUT = ROOT / 'deploy/r22/accepted-suite'
PYTHON = '/tmp/pm-r22-venv/bin/python'
OUT.mkdir(parents=True, exist_ok=True)
tree = ast.parse((BACKEND/'tests/test_us_screener.py').read_text())
dsn = next(ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign)
           and any(isinstance(t, ast.Name) and t.id == 'DSN' for t in n.targets))
env = dict(os.environ, DATABASE_URL=dsn, QD_TEST_POSTGRES_DSN=dsn,
           AUTOMATION_TEST_DATABASE_URL=dsn, DB_TYPE='postgresql', SKIP_STARTUP_HOOKS='1',
           OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1')
files = sorted(str(p.relative_to(BACKEND)) for p in (BACKEND/'tests').rglob('test_*.py'))
rows = []
groups = [[f for f in files[shard::6] if f not in (
    'tests/test_polymarket_ws_replay.py', 'tests/test_polymarket_ws_round11.py')] for shard in range(6)]
groups.append(['tests/test_polymarket_ws_round11.py'])
for shard, selected in enumerate(groups):
    (OUT/f'shard-{shard}-files.json').write_text(json.dumps(selected, indent=2)+'\n')
    began = time.monotonic()
    with (OUT/f'shard-{shard}.log').open('w') as log:
        result = subprocess.run([PYTHON, '-m', 'pytest', '-q', *selected,
                                 '-o', 'faulthandler_timeout=0'], cwd=BACKEND,
                                env=env, stdout=log, stderr=subprocess.STDOUT)
    summary = next((l for l in reversed((OUT/f'shard-{shard}.log').read_text().splitlines())
                    if re.search(r'\b\d+ passed.* in [\d.]+s', l)), 'no summary')
    rows.append(dict(shard=shard, exit=result.returncode, summary=summary,
                     files=len(selected), elapsed=time.monotonic()-began))
    (OUT/'results.json').write_text(json.dumps(rows, indent=2)+'\n')
    print(summary, flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
subprocess.run([PYTHON, str(ROOT/'deploy/r22/run-cold-simulation.py')], check=True)
subprocess.run([PYTHON, str(ROOT/'deploy/r22/check-suite.py')], check=True)
