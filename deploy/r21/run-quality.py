import subprocess,pathlib,json,sys
repo=pathlib.Path('/workspace/quantdinger/wt_r21')
checks=[['/tmp/pm-r20-venv/bin/ruff','check','app','scripts','tests'],['/tmp/pm-r20-venv/bin/python','-m','compileall','-q','app/services/polymarket','scripts/polymarket_ws_worker_replay.py','scripts/polymarket_ws_handoff_replay.py','scripts/polymarket_ws_backup_cost.py'],['git','diff','--check']]
rows=[]
for args in checks:
 p=subprocess.run(args,cwd=repo/'backend_api_python',text=True,capture_output=True)
 rows.append(dict(command=args,exit=p.returncode,output=(p.stdout+p.stderr)[-3000:]))
(repo/'deploy/r21/quality.json').write_text(json.dumps(rows,indent=2)+'\n')
print([(r['command'][0].split('/')[-1],r['exit']) for r in rows])
raise SystemExit(0 if all(r['exit']==0 for r in rows) else 1)
