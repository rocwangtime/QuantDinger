import os,pathlib,subprocess,time,json
root=pathlib.Path('/workspace/quantdinger/wt_r21');out=root/'deploy/r21'
# This extra run starts only after the required eight-run matrix has finished.
rows=json.loads((out/'comparison/results.json').read_text());assert len(rows)==8 and all(r['result']['valid'] and r['exit']==0 for r in rows)
args=['/tmp/pm-r20-venv/bin/python','/tmp/r21-capacity-worker.py','--process-per-group','--seconds','30','--rate','3792','--confine-cpus','4','--backup-count','2','--kicks','0']
env=dict(os.environ,PYTHONPATH=str(root/'backend_api_python'),SKIP_STARTUP_HOOKS='1',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
t0=time.monotonic();r=subprocess.run(args,cwd=root/'backend_api_python',env=env,capture_output=True,text=True)
(out/'capacity.stderr').write_text(r.stderr)
try:res=json.loads(r.stdout.splitlines()[-1])
except Exception:(out/'capacity.stdout').write_text(r.stdout);raise
row=dict(args=args,wall_seconds=time.monotonic()-t0,exit=r.returncode,result=res)
(out/'capacity.json').write_text(json.dumps(row,indent=2)+'\n');print(json.dumps(dict(valid=res['valid'],cpu=res['measured_cpu_cores'],ready=res['ready_ratio'],per_copy=res['backup_book_cores_by_copy'])),flush=True)
raise SystemExit(0 if r.returncode==0 and res['valid'] else 1)
