"""Prepare four reviewable commit trees without changing the working files."""
from pathlib import Path
import os
import subprocess
root=Path(__file__).resolve().parents[2]
backend='backend_api_python/'
env=dict(os.environ,GIT_INDEX_FILE='/tmp/pm-r22-delivery.index')
def git(*args,**kw):return subprocess.check_output(['git',*args],cwd=root,env=env,**kw).decode().strip()
def put(path,text=None):
    content=(root/path).read_bytes() if text is None else text.encode()
    blob=git('hash-object','-w','--stdin',input=content)
    git('update-index','--add','--cacheinfo','100644',blob,path)
obs_path=backend+'app/services/polymarket/observer.py'
final=(root/obs_path).read_text()
r2=final.replace('from .ws_payloads import cheap_envelope, feed_envelope, loads','from .ws_payloads import cheap_envelope, decoded_envelope, loads').replace('tips = feed_envelope(event)','tips = decoded_envelope(event)')
r1=r2.replace(', DisconnectClusters, business_gap_counts',', DisconnectClusters')
r1=r1.replace('def receive(self, raw, owned=None, received_at=None, inbox=None, epoch=None, source=None):','def receive(self, raw, owned=None, received_at=None, inbox=None, epoch=None):',1)
r1=r1.replace("published=published, source=source or getattr(inbox, 'source', None))",'published=published)')
r1=r1.replace("            for group, event in gap_events.items():\n                event['at'] = state['outage_wall'].timestamp()\n                self.continuity.watch_gap(event, set(state['assets']))\n                self.double_kicks.watch_primary(group, state.get('source'), event, now)","            for event in gap_events.values():\n                self.continuity.watch_gap(event, set(state['assets']))")
start=r1.index("                rolling = row.setdefault('rolling_events'")
end=r1.index("                _LOG.warning('Polymarket WS hot primary failure",start)
r1=r1[:start]+r1[end:]
r1=r1.replace("        inbox.source = state['source']\n",'')
start=r1.index('                          ws_feed_exchange_lag=')
end=r1.index('                          ws_backup_book_cpu_seconds=',start)
r1=r1[:start]+r1[end:]
r1=r1.replace("                              **business_gap_counts(v.get('rolling_events', v['events']), datetime.now(timezone.utc).timestamp()),\n",'')
r1=r1.replace('                              stale_by_source=dict(m.stale_by_source),\n','')
for source in (r1,r2,final):compile(source,obs_path,'exec')
git('read-tree','c6acf50')
put(obs_path,r1)
for p in ['app/services/polymarket/ws_payloads.py','tests/test_polymarket_ws_r21_supply.py','tests/test_polymarket_ws_r22_freshness.py','scripts/polymarket_ws_lag_replay.py']:
    put(backend+p)
put('deploy/r22/R1-evidence.md')
trees=[dict(item='R1',tree=git('write-tree'))]
put(obs_path,r2)
for p in ['app/services/polymarket/recording.py','app/services/polymarket/ws_redundancy.py','app/services/polymarket/ws_group_process.py','tests/test_polymarket_ws_r22_metrics.py','tests/test_polymarket_ws_fix_b.py']:
    put(backend+p)
put('deploy/r22/R2-evidence.md')
trees.append(dict(item='R2',tree=git('write-tree')))
put(obs_path)
for p in ['app/services/polymarket/ws_rotation.py','scripts/polymarket_ws_warm_profile.py','tests/test_polymarket_ws_r22_warm.py']:
    put(backend+p)
put('deploy/r22/R3-evidence.md')
trees.append(dict(item='R3',tree=git('write-tree')))
for p in ['app/services/polymarket/client.py','app/services/polymarket/paper_account.py','tests/test_polymarket_paper_account.py','tests/test_polymarket_server_time.py']:
    put(backend+p)
put('deploy/r22/R4-evidence.md')
trees.append(dict(item='R4',tree=git('write-tree')))
import json
(root/'deploy/r22/commit-trees.json').write_text(json.dumps(trees,indent=2)+'\n')
assert not git('diff',trees[-1]['tree'],'--','backend_api_python'), 'final code tree differs from verified files'
print(json.dumps(trees))
