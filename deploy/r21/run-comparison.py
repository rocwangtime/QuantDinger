import os,pathlib,subprocess,json,time
repo=pathlib.Path('/workspace/quantdinger/wt_r21');out=repo/'deploy/r21/comparison';out.mkdir(exist_ok=True)
env=dict(os.environ,PYTHONPATH='.',SKIP_STARTUP_HOOKS='1',PYTHONDONTWRITEBYTECODE='1',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1')
results=[]
for phase,root in [('before','/tmp/pm-r21-baseline/backend_api_python'),('after',str(repo/'backend_api_python'))]:
 for rate in (1896,3792):
  for repeat in (1,2):
   label=f'{phase}-{rate}-{repeat}';args=['/tmp/pm-r20-venv/bin/python','scripts/polymarket_ws_worker_replay.py','--process-per-group','--seconds','15','--rate',str(rate),'--confine-cpus','4','--backup-count','2']
   start=time.monotonic();r=subprocess.run(args,cwd=root,env=env,capture_output=True,text=True)
   (out/(label+'.stderr')).write_text(r.stderr)
   try:res=json.loads(r.stdout.splitlines()[-1])
   except Exception:res={'valid':False,'error':'no JSON'};(out/(label+'.stdout')).write_text(r.stdout)
   row=dict(phase=phase,rate=rate,repeat=repeat,exit=r.returncode,wall_seconds=time.monotonic()-start,result=res)
   (out/(label+'.json')).write_text(json.dumps(row,indent=2)+'\n');results.append(row)
   print(label, 'valid',res.get('valid'),'CPU',res.get('measured_cpu_cores'),'p99',res.get('primary_queue_wait_ms',{}).get('p99'),'gap',res.get('failover_gap_max_s'),'ready',res.get('ready_ratio'),flush=True)
(out/'results.json').write_text(json.dumps(results,indent=2)+'\n')
raise SystemExit(0 if all(r['exit']==0 and r['result']['valid'] for r in results) else 1)
