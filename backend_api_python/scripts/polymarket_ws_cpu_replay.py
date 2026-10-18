"""Offline synthetic public-feed replay; no sockets, credentials or database.

From backend_api_python:
    PYTHONPATH=. python scripts/polymarket_ws_cpu_replay.py --profile /tmp/ws.stats
Run the same script with PYTHONPATH pointing to a baseline checkout to compare.
Generation is excluded from CPU timing. The replay includes parsing, fan-in,
continuity, live/shadow ladders, recording metadata and bounded paper history.
Reader threads, evaluator and SQL are covered separately by the test suite.
"""
import argparse
import cProfile
import gc
import hashlib
import json
import pstats
import time
from datetime import datetime, timezone
from types import SimpleNamespace

from app.services.polymarket import observer, ws_payloads
from app.services.polymarket.account_history import BookHistory
from app.services.polymarket.ws_redundancy import FeedMerge, FeedContinuity
from app.services.polymarket.ws_rotation import ShadowBooks

FPS = 180
EPOCH_MS = 1792294000000
ASSETS = frozenset({'ay', 'an'})


def frames(count):
    """20% paired full books (140 levels/asset), 80% eight-row deltas."""
    result = []
    for i in range(count):
        at = EPOCH_MS + round(i * 1000 / FPS)
        if i % 5 == 0:
            event = [dict(event_type='book', asset_id=asset, timestamp=at, hash=f'{i}{asset}',
                bids=[dict(price=str(x/1000), size='3') for x in range(400, 470)],
                asks=[dict(price=str(x/1000), size='4') for x in range(530, 600)]) for asset in sorted(ASSETS)]
        else:
            event = dict(event_type='price_change', timestamp=at, price_changes=[
                dict(asset_id=asset, side='SELL', price=str(x/1000), size=str(i), hash=f'{i}{asset}{x}',
                    best_bid='.469', best_ask='.530') for asset in sorted(ASSETS) for x in range(530, 534)])
        result.append(json.dumps(event, separators=(',', ':')))
    return result


def replay(raws, schedule, pace=False):
    pipeline = observer.Pipeline()
    pipeline.set_markets([dict(id='a', yesAssetId='ay', noAssetId='an')])
    pipeline.recording.enabled = False
    pipeline.paper_account = SimpleNamespace(history=BookHistory())
    cache, shadow = ws_payloads.ParsedFrames(), ShadowBooks(ASSETS)
    merge, continuity = FeedMerge({}), FeedContinuity()
    started = time.perf_counter()
    for due, index, source in schedule:
        if pace:
            time.sleep(max(0, started + due/FPS - time.perf_counter()))
        received = datetime.fromtimestamp((EPOCH_MS + 86 + due*1000/FPS)/1000, timezone.utc)
        event = cache.parse(raws[index])
        if source == 'backup':
            shadow.receive(event, received)
        continuity.receive(event, source, received)
        merged = merge.filter(event, source, ASSETS, backup=source == 'backup')
        if merged:
            pipeline.receive(merged, ASSETS, received)
            continuity.publication(pipeline.recording.metadata, ASSETS)
    assert pipeline.books == shadow.books
    return dict(published=pipeline.messages, duplicates=merge.duplicates, stale=merge.stale,
        cache=cache.metrics(), levels=sum(len(side) for book in pipeline.books.values() for side in book.values()),
        final_book_sha256=hashlib.sha256(json.dumps(pipeline.books, sort_keys=True).encode()).hexdigest())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--frames', type=int, default=10800)
    parser.add_argument('--skew-frames', type=int, default=54, help='54 frames = 300ms at 180fps')
    parser.add_argument('--trials', type=int, default=3)
    parser.add_argument('--profile', help='Write cProfile stats to this path')
    parser.add_argument('--pace', action='store_true', help='Release at 180fps rather than run at full speed')
    parser.add_argument('--stdlib-json', action='store_true', help='Measure optional-decoder fallback')
    args = parser.parse_args()
    if args.frames < 1 or args.trials < 1 or args.skew_frames < 0:
        parser.error('frames/trials must be positive and skew-frames nonnegative')
    if args.stdlib_json:
        ws_payloads.orjson = None
    raws = frames(args.frames)
    schedule = sorted([(i, i, 'primary') for i in range(args.frames)] +
                      [(i+args.skew_frames, i, 'backup') for i in range(args.frames)])
    print('shape', json.dumps(dict(frames_per_source=args.frames, seconds=args.frames/FPS, fps=FPS,
        bytes_per_s=sum(map(len, raws))/(args.frames/FPS), skew_ms=args.skew_frames/FPS*1000)))
    for trial in range(args.trials):
        gc.collect()
        cpu_start, wall_start = time.process_time(), time.perf_counter()
        result = replay(raws, schedule, args.pace)
        cpu, wall = time.process_time()-cpu_start, time.perf_counter()-wall_start
        print('trial', json.dumps(dict(trial=trial, cpu_s=cpu, wall_s=wall,
            ms_per_wire_message=cpu/(2*args.frames)*1000, **result)), flush=True)
    if args.profile:
        profiler = cProfile.Profile()
        profiler.runcall(replay, raws, schedule, args.pace)
        profiler.dump_stats(args.profile)
        pstats.Stats(profiler).strip_dirs().sort_stats('cumtime').print_stats(30)


if __name__ == '__main__':
    main()
