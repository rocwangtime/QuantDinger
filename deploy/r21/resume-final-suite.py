"""Preserve passed groups; rerun affected 0/5, then isolated rollover and cold."""
import ast,json,os,re,subprocess,time
from pathlib import Path
root=Path('/workspace/quantdinger/wt_r21');backend=root/'backend_api_python';out=root/'deploy/r21/accepted-suite';py='/tmp/pm-r20-venv/bin/python'
prior=json.loads((out/'results.json').read_text());assert all(r['exit']==0 for r in prior[:5])
rows={r['shard']:r for r in prior if r['exit']==0}
files=sorted(str(p.relative_to(backend)) for p in (backend/'tests').rglob('test_*.py'))
groups=[[f for f in files[shard::6] if f not in ('tests/test_polymarket_ws_replay.py','tests/test_polymarket_ws_round11.py')] for shard in range(6)]
groups.append(['tests/test_polymarket_ws_round11.py'])
tree=ast.parse((backend/'tests/test_us_screener.py').read_text())
dsn=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='DSN' for t in n.targets))
env=dict(os.environ,DATABASE_URL=dsn,QD_TEST_POSTGRES_DSN=dsn,AUTOMATION_TEST_DATABASE_URL=dsn,DB_TYPE='postgresql',SKIP_STARTUP_HOOKS='1',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
for shard in (0,5,6):
    selected=groups[shard];(out/f'shard-{shard}-files.json').write_text(json.dumps(selected,indent=2)+'\n')
    start=time.monotonic()
    with (out/f'shard-{shard}.log').open('w') as log:
        r=subprocess.run([py,'-m','pytest','-q',*selected,'-o','faulthandler_timeout=30'],cwd=backend,env=env,stdout=log,stderr=subprocess.STDOUT)
    summary=next((line for line in reversed((out/f'shard-{shard}.log').read_text().splitlines()) if re.search(r'\b\d+ passed.* in [\d.]+s',line)),'no summary')
    rows[shard]=dict(shard=shard,exit=r.returncode,summary=summary,files=len(selected),elapsed=time.monotonic()-start)
    (out/'results.json').write_text(json.dumps([rows[i] for i in sorted(rows)],indent=2)+'\n')
    print(summary,flush=True)
    if r.returncode:raise SystemExit(r.returncode)
subprocess.run([py,str(root/'deploy/r21/run-cold-simulation.py')],check=True)
subprocess.run([py,str(root/'deploy/r21/check-suite.py')],check=True)
