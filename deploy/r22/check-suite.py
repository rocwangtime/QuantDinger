import json,pathlib,re,hashlib
root=pathlib.Path('/workspace/quantdinger/wt_r22');p=root/'deploy/r22/accepted-suite'
rows=json.loads((p/'results.json').read_text());assert len(rows)==7 and all(r['exit']==0 for r in rows)
files=[]
for i in range(7):files+=json.loads((p/f'shard-{i}-files.json').read_text())
files+=['tests/test_polymarket_ws_replay.py']
expected=sorted(str(x.relative_to(root/'backend_api_python')) for x in (root/'backend_api_python/tests').rglob('test_*.py'))
assert len(files)==len(set(files)) and sorted(files)==expected
cold=json.loads((p/'cold-simulation-resources.json').read_text());assert cold['exit']==0
summaries=[r['summary'] for r in rows]+[(p/'cold-simulation.log').read_text().strip().splitlines()[-1]]
passed=sum(int(re.search(r'(\d+) passed',s)[1]) for s in summaries)
skipped=sum(int(re.search(r'(\d+) skipped',s)[1]) if ' skipped' in s else 0 for s in summaries)
assert passed > 5000 and skipped==24,(passed,skipped)
source_hashes={str(x.relative_to(root)):hashlib.sha256(x.read_bytes()).hexdigest() for x in (root/'backend_api_python/app/services/polymarket').glob('*.py')}
result=dict(python='3.12.15',postgres='18.0',database='isolated PG18 under /tmp, localhost:5432',test_files=len(files),passed=passed,skipped=skipped,failed=0,errors=0,total=passed+skipped,shards=rows,cold_simulation=cold,source_hashes=source_hashes)
(root/'deploy/r22/suite-summary.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k not in ('shards','source_hashes')}))
