#!/usr/bin/env bash
# Start detached when the terminal/tool session may disappear:
# nohup bash deploy/p34_loop.sh --output deploy/p34_round3 > /tmp/p34_loop.out 2>&1 < /dev/null &
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
exec python3 -u "$script_dir/p34_monitor.py" "$@"
