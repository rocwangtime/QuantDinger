"""Offline profile of the actual warm-spare decode/apply/checkpoint path."""
import argparse
import cProfile
import hashlib
import json
import pstats
import time
from datetime import datetime, timezone
from polymarket_ws_cpu_replay import frames
from app.services.polymarket.ws_payloads import loads
try:
    from app.services.polymarket.ws_payloads import feed_envelope as envelope
except ImportError:
    from app.services.polymarket.ws_payloads import decoded_envelope as envelope
from app.services.polymarket.ws_warm_book import WarmBook


def run(raws):
    warm = WarmBook({'ay','an'})
    at = datetime.now(timezone.utc)
    parse = apply = 0
    for raw in raws:
        t = time.process_time()
        event = loads(raw)
        envelope(event)
        parse += time.process_time()-t
        t = time.process_time()
        warm.receive(event, at)
        apply += time.process_time()-t
    sha = hashlib.sha256(json.dumps(warm.books, sort_keys=True).encode()).hexdigest()
    return dict(parse_cpu_seconds=parse, apply_cpu_seconds=apply, sha256=sha)


if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--frames',type=int,default=20000)
    p.add_argument('--profile')
    a=p.parse_args()
    raws=frames(a.frames)
    results=[]
    for _ in range(3):
        t=time.process_time()
        result=run(raws)
        results.append(dict(cpu_seconds=time.process_time()-t,**result))
    print(json.dumps(dict(frames=a.frames,results=results)),flush=True)
    if a.profile:
        profiler=cProfile.Profile()
        profiler.runcall(run,raws)
        profiler.dump_stats(a.profile)
        pstats.Stats(profiler).strip_dirs().sort_stats('cumtime').print_stats(25)
