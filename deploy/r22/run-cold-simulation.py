import subprocess,time,resource,os,pathlib,json
repo=pathlib.Path('/workspace/quantdinger/wt_r22/backend_api_python');out=repo.parent/'deploy/r22/accepted-suite'
env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',SKIP_STARTUP_HOOKS='1')
t0=time.monotonic()
with (out/'cold-simulation.log').open('w') as log:
 r=subprocess.run(['/tmp/pm-r22-venv/bin/python','-m','pytest','-q','tests/test_polymarket_ws_replay.py','--timeout=600'],cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT)
usage=resource.getrusage(resource.RUSAGE_CHILDREN)
result=dict(exit=r.returncode,wall_seconds=time.monotonic()-t0,user_cpu=usage.ru_utime,system_cpu=usage.ru_stime,max_rss_kb=usage.ru_maxrss)
(out/'cold-simulation-resources.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
raise SystemExit(r.returncode)
