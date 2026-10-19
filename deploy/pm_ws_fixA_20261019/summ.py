import json, sys, statistics as st
rows=[json.loads(l) for l in open(sys.argv[1]) if "\"r\":}" not in l]
def g(r,path):
    for p in path.split('.'):
        r=r.get(p,{}) if isinstance(r,dict) else {}
    return r if r!={} else 0
keys=[('cpu','r.cpu_cores'),('ms/frame','r.ms_per_wire_message'),('ready','r.ready_ratio'),('resync','r.resyncs'),('http','r.http_attempts'),('valid','r.valid'),
 ('P.qw.p99','r.primary_queue_wait_ms.p99'),('P.qw.max','r.primary_queue_wait_ms.max'),('B.qw.p99','r.backup_queue_wait_ms.p99'),('B.qw.max','r.backup_queue_wait_ms.max'),
 ('lc.bk.wait_s','r.lock_timing.lifecycle.backup.wait.total_s'),('lc.bk.wait_p99','r.lock_timing.lifecycle.backup.wait.p99_ms'),
 ('lc.sup.hold_s','r.lock_timing.lifecycle.supervisor.hold.total_s'),('lc.sup.hold_p99','r.lock_timing.lifecycle.supervisor.hold.p99_ms'),
 ('lc.loop.hold_p99','r.lock_timing.lifecycle.market-loop.hold.p99_ms'),
 ('pl.pr.wait_s','r.lock_timing.pipeline.primary.wait.total_s'),('pl.pr.wait_p99','r.lock_timing.pipeline.primary.wait.p99_ms'),('pl.loop.hold_max','r.lock_timing.pipeline.market-loop.hold.max_ms'),('sec','r.seconds')]
by={}
for r in rows: by.setdefault((r['rate'],r['label']),[]).append(r)
print('rate label n | '+' | '.join(k for k,_ in keys))
for (rate,label),rs in sorted(by.items()):
    vals=[]
    for k,p in keys:
        xs=[float(g(r,p)) for r in rs]
        vals.append(f"{st.median(xs):.3g}" + (f"[{min(xs):.3g}-{max(xs):.3g}]" if len(xs)>1 and k in ('P.qw.p99','B.qw.p99','ready','cpu','ms/frame') else ''))
    print(rate,label,len(rs),'|',' | '.join(vals))
