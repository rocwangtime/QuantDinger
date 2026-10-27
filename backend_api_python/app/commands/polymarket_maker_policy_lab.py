"""Read-only paper-maker cohort screen: python -m app.commands.polymarket_maker_policy_lab.

Does not open a database connection. Export SQL lives on maker_policy_lab
(FILLS_SQL, SUMMARY_SQL). `--fills` reads a local JSONL file. `--data` reads a
local maker_replay CSV directory. `go: true` in the JSON is not a switch.
"""
import argparse
import json
from pathlib import Path

from app.services.polymarket.maker_policy_lab import collect_replay, evaluate, load_fills
from app.services.polymarket.maker_replay import load


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--fills', type=Path, help='JSONL export of shadow_fills events')
    source.add_argument('--data', type=Path, help='Directory of maker_replay CSV exports')
    parser.add_argument('--cells', default='BTC:5m,ETH:5m,BTC:15m,ETH:15m')
    parser.add_argument('--set', action='append', default=[], metavar='KEY=VALUE', help='Maker parameter override')
    parser.add_argument('--iters', type=int, default=2000)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    try:
        config = dict(item.split('=', 1) for item in args.set)
        rest = float(config.get('touch_stable_rest_ms', 1000))
        touch = float(config.get('touch_stable_ms', 2000))
        if args.fills:
            fills, skipped = load_fills(args.fills)
        else:
            fills = collect_replay(load(args.data), config, cells=tuple(args.cells.split(',')))
            skipped = 0
        report = evaluate(fills, rest_min=rest, touch_min=touch, iters=args.iters)
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    report['skipped_rows'] = skipped
    report['fills'] = len(fills)
    text = json.dumps(report, indent=1, default=str)
    if args.output:
        args.output.write_text(text)
    print(text)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
