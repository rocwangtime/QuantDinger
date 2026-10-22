"""Offline per-copy CPU benchmark; generation is outside the timed region."""
import argparse
import hashlib
import json
import time
from datetime import datetime, timezone

from app.services.polymarket import ws_payloads
from app.services.polymarket.ws_warm_book import WarmBook
from polymarket_ws_cpu_replay import frames


def measure(count=5000, trials=3):
    raws = frames(count)
    results = []
    now = datetime.now(timezone.utc)
    for mode in ('scan_only', 'double_decode', 'single_decode'):
        if mode == 'single_decode' and not hasattr(ws_payloads, 'decoded_envelope'):
            continue
        for trial in range(trials):
            shadow = WarmBook(['ay', 'an'])
            parser = ws_payloads.ParsedFrames(cache=False)
            began = time.thread_time()
            for raw in raws:
                if mode == 'scan_only':
                    ws_payloads.cheap_envelope(raw)
                elif mode == 'double_decode':
                    shadow.receive(parser.parse(raw), now)
                    ws_payloads.cheap_envelope(raw)
                else:
                    event = ws_payloads.loads(raw)
                    shadow.receive(event, now)
                    ws_payloads.decoded_envelope(event)
            cpu = time.thread_time()-began
            results.append(dict(mode=mode, trial=trial, frames=count, cpu_s=cpu,
                us_per_frame=cpu/count*1e6,
                sha=hashlib.sha256(json.dumps(shadow.books, sort_keys=True).encode()).hexdigest()))
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--frames', type=int, default=5000)
    parser.add_argument('--trials', type=int, default=3)
    args = parser.parse_args()
    print(json.dumps(measure(args.frames, args.trials)))
