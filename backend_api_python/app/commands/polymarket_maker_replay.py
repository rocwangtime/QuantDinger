"""Offline v3 paper-maker replay over recorded exports: python -m app.commands.polymarket_maker_replay.

Reads a directory of read-only CSV exports (EXPORT_SQL in maker_replay); no network, no database.
"""

import argparse
import json
from pathlib import Path

from app.services.polymarket.maker_replay import load, replay
from app.services.polymarket.updown import timestamp


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True, help="Directory with windows/spot/books_*/trades_* .csv.gz")
    parser.add_argument("--cells", default="BTC:15m,ETH:15m", help="Comma-separated ASSET:INTERVAL cells")
    parser.add_argument("--set", action="append", default=[], metavar="KEY=VALUE", help="Maker parameter override (lower-case key)")
    parser.add_argument("--queue", choices=("pessimistic", "proportional"), default="pessimistic")
    parser.add_argument("--source", choices=("admitted", "cold_start"), default="cold_start",
                        help="cold_start runs at probation size like a fresh deployment")
    parser.add_argument("--start", help="ISO timestamp with timezone")
    parser.add_argument("--end", help="ISO timestamp with timezone")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        config = dict(item.split("=", 1) for item in args.set)
        bound = lambda value: timestamp(value).timestamp() * 1000 if value else None
        out = replay(load(args.data), config, cells=tuple(args.cells.split(",")), start=bound(args.start),
                     end=bound(args.end), queue=args.queue, source=args.source)
    except ValueError as exc:
        parser.error(str(exc))
    text = json.dumps(dict(out, config=config, queue=args.queue, source=args.source), indent=1, default=str)
    if args.output:
        args.output.write_text(text)
    print(text)


if __name__ == "__main__":
    main()
