import gzip, json, sys, collections, bisect, random
def pct(a,p):
    if not a: return None
    a=sorted(a); return a[min(len(a)-1,int(p*len(a)))]
frames=collections.defaultdict(collections.Counter)   # key -> second -> frames
books=collections.defaultdict(list)    # (key,asset) -> recv times (excluding first per asset)
seen=set(); assets_by_key=collections.defaultdict(set); first_t={}
pc_assets=collections.defaultdict(list)
t0=None; t1=None; closes=[]; nbytes=collections.Counter()
def lines():
    f=gzip.open(sys.argv[1],'rt')
    try:
        for l in f: yield l
    except EOFError: return
for line in lines():
    try: d=json.loads(line)
    except Exception: break
    t=d['t']; k=d['k']; t0=t if t0 is None else t0; t1=t
    if 'close' in d: closes.append(d); continue
    if 'r' not in d: continue
    raw=d['r']
    if raw=='PONG': continue
    frames[k][int(t)]+=1; nbytes[k]+=len(raw)
    try: ev=json.loads(raw)
    except Exception: continue
    for e in (ev if isinstance(ev,list) else [ev]):
        if not isinstance(e,dict): continue
        typ=e.get('event_type')
        if typ=='book':
            a=e.get('asset_id'); assets_by_key[k].add(a)
            if (k,a) in seen: books[(k,a)].append(t)
            else: seen.add((k,a)); first_t[(k,a)]=t
        elif typ=='price_change':
            for pc in e.get('price_changes') or []:
                pc_assets[(k,pc.get('asset_id'))].append(t)
res={'window_s':t1-t0,'closes':len(closes)}
# per-second rates
allsec=collections.Counter()
for k,c in frames.items():
    for s,n in c.items(): allsec[s]+=n
    secs=[c.get(s,0) for s in range(int(t0)+5,int(t1)-5)]
    res[k]=dict(fps_mean=sum(secs)/len(secs), fps_p50=pct(secs,.5), fps_p99=pct(secs,.99), fps_max=max(secs), bytes_per_frame=nbytes[k]/max(1,sum(c.values())))
secs=[allsec.get(s,0)*2 for s in range(int(t0)+5,int(t1)-5)]  # x2 = primary+backup physical
res['physical_x2']=dict(mean=sum(secs)/len(secs),p50=pct(secs,.5),p90=pct(secs,.9),p99=pct(secs,.99),max=max(secs),
  share_gt_3300=sum(s>3300 for s in secs)/len(secs),share_gt_4000=sum(s>4000 for s in secs)/len(secs),share_gt_4950=sum(s>4950 for s in secs)/len(secs))
# book spacing: only assets that are live (have price changes) and spacing within a market lifetime
gaps_by_key=collections.defaultdict(list)
for (k,a),ts in books.items():
    ts=sorted(ts)
    g=[b-a_ for a_,b in zip(ts,ts[1:])]
    gaps_by_key[k]+=g
for k,g in gaps_by_key.items():
    res[k].update(book_gap_n=len(g),book_gap_p50=pct(g,.5),book_gap_p95=pct(g,.95),book_gap_p99=pct(g,.99),book_gap_max=max(g) if g else None,
                  books_per_asset_per_min=60*len(g)/max(1,(t1-t0))/max(1,len(assets_by_key[k])))
# residual: random kick times; for the group's ACTIVE market assets (those with price changes in the last 5s), time to next non-initial book (max over assets)
random.seed(1)
for k in frames:
    bt={a:sorted(books.get((k,a),[])) for a in assets_by_key[k]}
    pcs={a:sorted(pc_assets.get((k,a),[])) for a in assets_by_key[k]}
    out=[]
    for _ in range(4000):
        x=random.uniform(t0+60,t1-120)
        waits=[]
        for a,ts in bt.items():
            p=pcs[a]; i=bisect.bisect_left(p,x)
            if i==0 or x-p[i-1]>5: continue   # asset not active
            j=bisect.bisect_right(ts,x)
            waits.append(ts[j]-x if j<len(ts) else float('inf'))
        if waits: out.append(max(waits))
    fin=[w for w in out if w!=float('inf')]
    res[k].update(gap_after_failover_p50=pct(out,.5),gap_after_failover_p95=pct(out,.95),gap_after_failover_p99=pct(out,.99),
                  share_gt_1_5s=sum(w>1.5 for w in out)/max(1,len(out)), n=len(out), inf=len(out)-len(fin))
print(json.dumps(res,indent=1,default=str))
