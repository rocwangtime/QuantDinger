"""Keep disposable PG alive through migration and the serial full test suite."""
import ast
import os
from pathlib import Path
import subprocess
import sys
root=Path(__file__).resolve().parents[2]
backend=root/'backend_api_python'
pg='/tmp/pm-r22-pg/bin/pg_ctl'
data='/tmp/pm-r22-pgdata-utf8'
tree=ast.parse((backend/'tests/test_us_screener.py').read_text())
dsn=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='DSN' for t in n.targets))
env=dict(os.environ,DATABASE_URL=dsn,QD_TEST_POSTGRES_DSN=dsn,AUTOMATION_TEST_DATABASE_URL=dsn,DB_TYPE='postgresql',QD_PROCESS_ROLE='migration',SKIP_STARTUP_HOOKS='1',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
fresh=not Path(data).exists()
if fresh:
    subprocess.run(['/tmp/pm-r22-pg/bin/initdb','-D',data,'-A','trust','--no-locale','--encoding=UTF8'],stdout=subprocess.DEVNULL,check=True)
subprocess.run([pg,'-D',data,'-l','/tmp/pm-r22-pg-server.log','-o','-h 127.0.0.1 -p 5432 -k /tmp','start'],check=True)
try:
    if fresh:
        subprocess.run(['/tmp/pm-r22-pg/bin/createuser','-h','127.0.0.1','-p','5432','-s','qd_test'],check=True)
        subprocess.run(['/tmp/pm-r22-pg/bin/createdb','-h','127.0.0.1','-p','5432','-O','qd_test','qd_automation_test'],check=True)
    with (root/'deploy/r22/migrations.log').open('w') as log:
        subprocess.run([sys.executable,'-m','app.commands.migrate'],cwd=backend,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
    with (root/'deploy/r22/preflight.log').open('w') as log:
        selected=[str(p.relative_to(backend)) for p in sorted((backend/'tests').glob('test_polymarket_ws_r22*.py'))]
        selected+=['tests/test_polymarket_ws_r21_supply.py','tests/test_polymarket_ws_fix_b.py::test_group_processes_keep_status_shape_and_reap']
        subprocess.run([sys.executable,'-m','pytest','-q',*selected],cwd=backend,env=dict(env,QD_PROCESS_ROLE='legacy'),stdout=log,stderr=subprocess.STDOUT,check=True)
    with (root/'deploy/r22/full-suite-driver.log').open('w') as log:
        result=subprocess.run([sys.executable,str(root/'deploy/r22/run-full-suite.py')],env=dict(env,QD_PROCESS_ROLE='legacy'),stdout=log,stderr=subprocess.STDOUT)
    raise SystemExit(result.returncode)
finally:
    subprocess.run([pg,'-D',data,'stop','-m','fast'],check=False)
