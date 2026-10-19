import json, sys, time, timeit
from app.services.polymarket.ws_recovery import SnapshotRecovery
out = {}
for buckets in (1648, 3599):
    r = SnapshotRecovery()
    base = time.monotonic()
    for k in range(buckets, 0, -1):
        for f in ('attempts', 'requests', 'completed'):
            r.count('recovery', f, base - k)
        r.count('pressure', 'deferred', base - k)
    n = 200
    t = timeit.timeit(r.metrics, number=n) / n
    # exactness: compare rolling_hour vs full re-sum of retained buckets
    from collections import Counter
    m = r.metrics()
    full = sum(r.hour.values(), Counter())
    exact = all(m['categories'][k]['rolling_hour'][f] == full[k, f] for k in m['categories'] for f in m['categories'][k]['rolling_hour'])
    out[buckets] = dict(metrics_ms=t * 1000, buckets=m['hour_buckets'], exact=exact)
print(json.dumps(out))
