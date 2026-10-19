#!/bin/bash
# Prod-speed calibration on this machine's CPUs 0 and 1.
# usage: calibrated_run.sh LABEL S BG_PRODCORES PRIMARY_FIRST PARENT_LOAD -- replay args...
# S=0.78 matches the comparable part of a ~300us frame and is the gate.
# S=0.65 slows the frame itself to ~300us and is the conservative check.
set -euo pipefail
LABEL=$1; S=$2; BG=$3; PF=$4; PL=$5; shift 5
if [ "${1:-}" = "--" ]; then shift; fi
BGPID=
STEAL=()
cleanup() {
  if [ -n "${BGPID:-}" ]; then kill "$BGPID" 2>/dev/null || true; fi
  if [ ${#STEAL[@]} -gt 0 ]; then
    for p in "${STEAL[@]}"; do kill "$p" 2>/dev/null || true; done
  fi
}
trap cleanup EXIT
C=/sys/fs/cgroup/fixb
PERIOD=10000
sudo -n mkdir -p "$C/obs" "$C/bg" "$C/steal0" "$C/steal1"
if ! grep -qw cpu /sys/fs/cgroup/cgroup.subtree_control; then
  echo '+cpu +cpuset' | sudo -n tee /sys/fs/cgroup/cgroup.subtree_control >/dev/null
fi
echo '+cpu +cpuset' | sudo -n tee "$C/cgroup.subtree_control" >/dev/null
echo '0-1' | sudo -n tee "$C/cpuset.cpus" >/dev/null
echo '0' | sudo -n tee "$C/cpuset.mems" >/dev/null
for spec in 'obs 0-1' 'bg 0-1' 'steal0 0' 'steal1 1'; do
  name=${spec%% *}; cpus=${spec##* }
  echo "$cpus" | sudo -n tee "$C/$name/cpuset.cpus" >/dev/null
  echo '0' | sudo -n tee "$C/$name/cpuset.mems" >/dev/null
done
echo "max $PERIOD" | sudo -n tee "$C/obs/cpu.max" >/dev/null
if python3 -c "import sys; sys.exit(0 if float('$S') < 1 else 1)"; then
  SQ=$(python3 -c "print(int(round((1-float('$S'))*$PERIOD)))")
  echo "$SQ $PERIOD" | sudo -n tee "$C/steal0/cpu.max" >/dev/null
  echo "$SQ $PERIOD" | sudo -n tee "$C/steal1/cpu.max" >/dev/null
  for name in steal0 steal1; do
    bash -c 'echo $$ | sudo -n tee /sys/fs/cgroup/fixb/'"$name"'/cgroup.procs >/dev/null; exec python3 -c "while True: pass"' &
    STEAL+=($!)
  done
  sleep 0.3
fi
if python3 -c "import sys; sys.exit(0 if float('$BG') > 0 else 1)"; then
  BQ=$(python3 -c "print(max(1000, int(round(float('$BG')*float('$S')*$PERIOD))))")
  echo "$BQ $PERIOD" | sudo -n tee "$C/bg/cpu.max" >/dev/null
  bash -c 'echo $$ | sudo -n tee /sys/fs/cgroup/fixb/bg/cgroup.procs >/dev/null; exec python3 -c "while True: pass"' &
  BGPID=$!
  sleep 0.3
fi
read_stat() { awk -v k="$2" '$1==k{print $2}' "$1/cpu.stat"; }
o0=$(read_stat "$C/obs" usage_usec); b0=$(read_stat "$C/bg" usage_usec); w0=$(date +%s.%N)
cd /workspace/backend_api_python
set +e
OUT=$(bash -c 'echo $$ | sudo -n tee /sys/fs/cgroup/fixb/obs/cgroup.procs >/dev/null; exec env POLYMARKET_OBSERVER_PRIMARY_FIRST='"$PF"' QD_REPLAY_PARENT_GIL_LOAD='"$PL"' QD_REPLAY_FULL_DRAIN=1 PYTHONPATH=. /tmp/qd-venv/bin/python scripts/polymarket_ws_worker_replay.py "$@"' _ "$@" 2>>/tmp/pm_ws_fixb/calibrated.err | tail -1)
set -e
w1=$(date +%s.%N); o1=$(read_stat "$C/obs" usage_usec); b1=$(read_stat "$C/bg" usage_usec)
cleanup
trap - EXIT
python3 - "$LABEL" "$S" "$BG" "$PF" "$PL" "$o0" "$o1" "$b0" "$b1" "$w0" "$w1" "$OUT" <<'PY'
import json, sys
label, s, bg, pf, pl, o0, o1, b0, b1, w0, w1, out = sys.argv[1:]
wall = float(w1) - float(w0)
s = float(s)
try:
    result = json.loads(out)
except Exception:
    result = {'raw': out[-800:], 'valid': False}
obs_box = (int(o1) - int(o0)) / 1e6 / wall
bg_box = (int(b1 or 0) - int(b0 or 0)) / 1e6 / wall
print(json.dumps(dict(
    label=label, speed=s, bg_target=float(bg), primary_first=pf, parent_load=float(pl), wall=wall,
    obs_box_cores=obs_box, bg_box_cores=bg_box,
    obs_prod_cores=obs_box / s, bg_prod_cores=bg_box / s,
    total_prod_cores=(obs_box + bg_box) / s, r=result), separators=(',', ':')))
PY
