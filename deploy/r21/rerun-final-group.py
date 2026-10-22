"""Recheck the group containing the final replay bootstrap boundary test."""
import argparse,ast,json,os,re,subprocess,time
from pathlib import Path
root=Path('/workspace/quantdinger/wt_r21');backend=root/'backend_api_python';out=root/'deploy/r21/accepted-suite'
parser=argparse.ArgumentParser();parser.add_argument('--shard',type=int,default=0);args=parser.parse_args();shard=args.shard
rows=json.loads((out/'results.json').read_text())
assert len(rows)==7 and all(row['exit']==0 for row in rows)
selected=json.loads((out/f'shard-{shard}-files.json').read_text())
tree=ast.parse((backend/'tests/test_us_screener.py').read_text())
dsn=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='DSN' for t in n.targets))
env=dict(os.environ,DATABASE_URL=dsn,QD_TEST_POSTGRES_DSN=dsn,AUTOMATION_TEST_DATABASE_URL=dsn,DB_TYPE='postgresql',SKIP_STARTUP_HOOKS='1',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
began=time.monotonic()
with (out/f'shard-{shard}.log').open('w') as log:
    r=subprocess.run(['/tmp/pm-r20-venv/bin/python','-m','pytest','-q',*selected,'-o','faulthandler_timeout=0'],cwd=backend,env=env,stdout=log,stderr=subprocess.STDOUT)
summary=next(line for line in reversed((out/f'shard-{shard}.log').read_text().splitlines()) if re.search(r'\b\d+ passed.* in [\d.]+s',line))
rows[shard]=dict(shard=shard,exit=r.returncode,summary=summary,files=len(selected),elapsed=time.monotonic()-began,reason='final behavior test added after initial group run')
(out/'results.json').write_text(json.dumps(rows,indent=2)+'\n');print(summary,flush=True)
if r.returncode:raise SystemExit(r.returncode)
subprocess.run(['/tmp/pm-r20-venv/bin/python',str(root/'deploy/r21/check-suite.py')],check=True)
