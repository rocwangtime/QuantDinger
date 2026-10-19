import json, sys, statistics as st
rows=[json.loads(l) for l in open(sys.argv[1]) if '"r":}' not in l]
def g(x,path):
    for p in path.split('.'): x=x[p]
    return x
M=[('cpu','cpu_cores'),('ms/frame','ms_per_wire_message'),('ready','ready_ratio'),('resync','resyncs'),
   ('MERGED.p50','merged_publication_lag_ms.p50'),('MERGED.p99','merged_publication_lag_ms.p99'),('MERGED.max','merged_publication_lag_ms.max'),
   ('P.qw.p50','primary_queue_wait_ms.p50'),('P.qw.p99','primary_queue_wait_ms.p99'),('P.qw.max','primary_queue_wait_ms.max'),
   ('B.qw.p99','backup_queue_wait_ms.p99'),('B.qw.max','backup_queue_wait_ms.max'),
   ('lcBkWait_s','lock_timing.lifecycle.backup.wait.total_s'),('lcSupHold_s','lock_timing.lifecycle.supervisor.hold.total_s'),('lcSupHold_p99','lock_timing.lifecycle.supervisor.hold.p99_ms'),
   ('plPrWait_s','lock_timing.pipeline.primary.wait.total_s'),('plLoopHoldMax','lock_timing.pipeline.market-loop.hold.max_ms'),
   ('rssGrowMB','rss')]
by={}
excl=[]
for r in rows:
    if r['r']['clock_drift_s']>0.01: excl.append((r['rate'],r['label'],r['trial'],r['r']['clock_drift_s'])); continue
    by.setdefault((r['rate'],r['label']),[]).append(r['r'])
print('excluded(clock step):',excl)
for (rate,label),rs in sorted(by.items()):
    out=[]
    for k,p in M:
        if p=='rss': xs=[(x['rss_bytes']['drained']-x['rss_bytes']['began'])/1e6 for x in rs]
        else: xs=[float(g(x,p)) for x in rs]
        out.append(f"{k}={st.median(xs):.3g}[{min(xs):.3g},{max(xs):.3g}]")
    print(rate,label,'n=%d'%len(rs),' '.join(out))
