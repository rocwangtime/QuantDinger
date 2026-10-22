import ast,pathlib,difflib,subprocess,hashlib,json
repo=pathlib.Path('/workspace/quantdinger/wt_r21')
p='backend_api_python/app/services/polymarket/ws_group_process.py'
source=(repo/p).read_text(); target=pathlib.Path('/tmp/pm-r21-baseline')/p
original=subprocess.check_output(['git','show','e90ce02:'+p],cwd=repo,text=True)

def nodes(text):
 tree=ast.parse(text);found={}
 for n in tree.body:
  if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)):found[n.name]=n
  if isinstance(n,ast.ClassDef):
   for m in n.body:
    if isinstance(m,ast.FunctionDef):found[n.name+'.'+m.name]=m
 return found
names=['ReplaySocket.send','_bootstrap','_drive','_Outbox.submit','_Outbox.run','_Feed.__init__','_Feed._handle','_Feed._apply']
a,b=nodes(original),nodes(source);oldlines=original.splitlines(True);newlines=source.splitlines(True)
for name in sorted(names,key=lambda n:a[n].lineno,reverse=True):
 n,m=a[name],b[name];oldlines[n.lineno-1:n.end_lineno]=newlines[m.lineno-1:m.end_lineno]
updated=''.join(oldlines);target.write_text(updated)
(repo/'deploy/r21/baseline-replay-fixture.patch').write_text(''.join(difflib.unified_diff(original.splitlines(True),updated.splitlines(True),fromfile='e90ce02/'+p,tofile='replay-instrumentation/'+p)))
for name in ('polymarket_ws_worker_replay.py','polymarket_ws_handoff_replay.py'):
 (pathlib.Path('/tmp/pm-r21-baseline/backend_api_python/scripts')/name).write_bytes((repo/'backend_api_python/scripts'/name).read_bytes())
observer='backend_api_python/app/services/polymarket/observer.py'
assert (pathlib.Path('/tmp/pm-r21-baseline')/observer).read_bytes()==subprocess.check_output(['git','show','e90ce02:'+observer],cwd=repo)
production_files=subprocess.check_output(['git','ls-tree','-r','--name-only','e90ce02',
    'backend_api_python/app/services/polymarket'],cwd=repo,text=True).splitlines()
different=[file for file in production_files if (pathlib.Path('/tmp/pm-r21-baseline')/file).read_bytes()
           != subprocess.check_output(['git','show','e90ce02:'+file],cwd=repo)]
assert different == [p], different
(repo/'deploy/r21/baseline-manifest.json').write_text(json.dumps(dict(commit='e90ce02',observer_sha256=hashlib.sha256((pathlib.Path('/tmp/pm-r21-baseline')/observer).read_bytes()).hexdigest(),overlaid_functions=names,production_observer_unchanged=True,production_files_checked=len(production_files),production_files_differing=different),indent=2)+'\n')
print('baseline observer unchanged; equal replay fixture ready')
