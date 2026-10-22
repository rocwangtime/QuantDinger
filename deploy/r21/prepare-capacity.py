from pathlib import Path
import difflib
root=Path('/workspace/quantdinger/wt_r21');p=root/'backend_api_python/scripts/polymarket_ws_worker_replay.py';s=p.read_text();old=s
s=s.replace("        child.send(dict(op='stages', token=token))", "        child.send(dict(op='status'))\n        child.send(dict(op='stages', token=token))")
s=s.replace("        stages_before = _snapshot_stages(feeds)\n        apply_before", "        stages_before = _snapshot_stages(feeds)\n        copies_before = {key: ((c.latest or {}).get('status') or {}).get('ws_backup_book_cpu_seconds', {})\n                         for key, c in feeds.children.items()}\n        apply_before")
s=s.replace("        stages_after = _snapshot_stages(feeds)\n        apply_after", "        stages_after = _snapshot_stages(feeds)\n        copies_after = {key: ((c.latest or {}).get('status') or {}).get('ws_backup_book_cpu_seconds', {})\n                        for key, c in feeds.children.items()}\n        apply_after")
s=s.replace("        result = dict(mode='process-per-group'", "        per_copy = {}\n        for key, copies in copies_after.items():\n            for source, costs in copies.items():\n                first = copies_before.get(key, {}).get(source, {})\n                per_copy[key+'/'+source] = {name: max(0, value-first.get(name, 0))/elapsed\n                    for name, value in costs.items()}\n        result = dict(backup_book_cores_by_copy=per_copy, mode='process-per-group'")
Path('/tmp/r21-capacity-worker.py').write_text(s)
(root/'deploy/r21/capacity-output.patch').write_text(''.join(difflib.unified_diff(old.splitlines(True),s.splitlines(True),fromfile='worker.py',tofile='capacity-output.py')))
