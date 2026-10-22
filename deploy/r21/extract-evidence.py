import json,pathlib,datetime,statistics,re
p=pathlib.Path('/workspace/quantdinger/deploy/p35/h0818_r20'); out=pathlib.Path('/workspace/quantdinger/wt_r21/deploy/r21')
s=json.loads((p/'observer_status_focus.json').read_text()); events=[]
for g,r in s['ws_hot_primary_failures'].items():
 for e in r['events']:
  if e.get('merged_gap_ms',0) is not None and e['merged_gap_ms']>1500:
   at=datetime.datetime.fromisoformat(e['interruption_at']); pub=datetime.datetime.fromisoformat(e['gap_maximum']['at'])
   events.append(dict(group=g,detected=at.isoformat(),last_publication=(pub-datetime.timedelta(milliseconds=e['merged_gap_ms'])).isoformat(),first_valid_publication=pub.isoformat(),detection_to_publication_ms=(pub-at).total_seconds()*1000,reported_detection_to_takeover_ms=e['detection_to_takeover_ms'],source_silence_ms=e['source_silence_ms'],merged_gap_ms=e['merged_gap_ms'],promotion_at=None,need_book_at=None,rest_return_at=None,missing_reason='r20 logs do not retain per-request / promotion stage timestamps',ready_backups=e['ready_backups']))
transport=[]
for n,l in enumerate((p/'observer_docker_0720_0820_sgt.dechunked.log').read_text().splitlines(),1):
 if 'transport failure' not in l:continue
 e=json.JSONDecoder().raw_decode(l.split('evidence=',1)[1])[0]
 transport.append(dict(line=n,code=e['code'],source=e['source'],role=e['role'],queue_wait=e['queue_wait_seconds'],drops=e['drops'],sample=e['sample']))
rows=[];at=None
for l in (p/'p51_mon_since_0715_sgt.log').read_text().splitlines():
 if l.startswith('==='):at=l[4:23]
 if l.startswith('S|'):v=json.loads(l[2:]);rows.append(dict(at=at,status_cpu=v.get('cpuc'),mps=v.get('mps')))
 if l.startswith('D|') and rows:
  m=re.match(r'D\|([\d.]+)%',l); rows[-1]['docker_cpu']=float(m[1])/100 if m else None
full=json.loads((p/'observer_status_full.json').read_text())['status']
result=dict(events=events,transport=transport,cpu_samples=rows,rest=s['ws_rest_recovery'],stage_metrics={k:v for k,v in full.items() if 'cpu' in k or 'stage' in k},evidence_files=[str(p/x) for x in ('observer_status_focus.json','observer_docker_0720_0820_sgt.dechunked.log','p51_mon_since_0715_sgt.log')])
(out/'production-evidence.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(dict(events=events,stage_keys=list(result['stage_metrics']),transport_count=len(transport)),indent=2))
