"""Offline bounded paper research: python -m app.commands.polymarket_backtest."""

import argparse
from pathlib import Path

from app.services.polymarket.backtest import markdown, run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="Inclusive window end bound, ISO timestamp with timezone")
    parser.add_argument("--end", required=True, help="Exclusive window end bound, at most seven days after start")
    parser.add_argument("--allow-proxy", action="store_true")
    parser.add_argument("--grid", action="store_true", help="T=30,60,120; D=3,10,20 bps")
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--threshold-bps", type=float, default=3)
    parser.add_argument("--max-book-age-seconds", type=float, default=1)
    parser.add_argument("--cap", type=float, default=1000)
    parser.add_argument("--max-windows", type=int, default=1000)
    parser.add_argument("--bootstrap", type=int, default=1000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    config = vars(args).copy()
    output, grid = config.pop("output"), config.pop("grid")
    seconds, threshold = config.pop("seconds"), config.pop("threshold_bps")
    config.update(seconds=[30, 60, 120] if grid else [seconds], thresholds=[3, 10, 20] if grid else [threshold])
    try:
        summary = markdown(run(config))
    except ValueError as exc:
        parser.error(str(exc))
    if output:
        output.write_text(summary)
    print(summary)


if __name__ == "__main__":
    main()
