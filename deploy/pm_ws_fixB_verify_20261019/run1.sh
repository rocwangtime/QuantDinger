#!/bin/bash
# usage: run1.sh LABEL S(speed factor, 1=unthrottled) BG_PRODCORES PF(1/0) PARENTLOAD -- replay args...
LABEL=$1; S=$2; BG=$3; PF=$4; PL=$5; shift 6
C=/sys/fs/cgroup/fixb
PERIOD=10000
echo "max $PERIOD" | sudo tee $C/cpu.max >/dev/null
STEAL=()
if [ "$S" != "1" ]; then
  SQ=$(python3 -c "print(int((1-$S)*$PERIOD))")
  for c in 6 7; do
    echo "$SQ $PERIOD" | sudo tee $C/steal$c/cpu.max >/dev/null
    bash -c 'echo $$ | sudo tee /sys/fs/cgroup/fixb/steal'$c'/cgroup.procs >/dev/null; exec python3 -c "while True: pass"' & STEAL+=($!)
  done
  sleep 0.5
fi
BGPID=
if python3 -c "import sys;sys.exit(0 if $BG>0 else 1)"; then
  BQ=$(python3 -c "print(int($BG*$S*$PERIOD))"); echo "$BQ $PERIOD" | sudo tee $C/bg/cpu.max >/dev/null
  bash -c 'echo $$ | sudo tee /sys/fs/cgroup/fixb/bg/cgroup.procs >/dev/null; exec python3 -c "while True: pass"' & BGPID=$!
  sleep 1
fi
read_stat(){ awk -v k=$2 '$1==k{print $2}' $1/cpu.stat; }
( while true; do echo "$(date +%s.%N) $(read_stat $C/obs usage_usec) $(read_stat $C/bg usage_usec)"; sleep 0.5; done ) > /tmp/fixB/samp.txt & SAMP=$!
o0=$(read_stat $C/obs usage_usec); b0=$(read_stat $C/bg usage_usec); t0=$(read_stat $C throttled_usec); w0=$(date +%s.%N)
cd /tmp/fixB/wt/backend_api_python
OUT=$(bash -c 'echo $$ | sudo tee /sys/fs/cgroup/fixb/obs/cgroup.procs >/dev/null; exec env QD_REPLAY_PRIMARY_FIRST='$PF' QD_REPLAY_PARENT_GIL_LOAD='$PL' PYTHONPATH=. /tmp/pm-round6-venv/bin/python scripts/polymarket_ws_worker_replay.py "$@"' _ "$@" 2>>/tmp/fixB/gate.err | tail -1)
w1=$(date +%s.%N); o1=$(read_stat $C/obs usage_usec); b1=$(read_stat $C/bg usage_usec); t1=$(read_stat $C throttled_usec)
kill $SAMP; [ -n "$BGPID" ] && kill $BGPID 2>/dev/null; for p in ${STEAL[@]}; do kill $p 2>/dev/null; done; pkill -f 'python3 -c while True: pass' 2>/dev/null
python3 - "$LABEL" "$S" "$BG" "$PF" "$PL" "$o0" "$o1" "$b0" "$b1" "$t0" "$t1" "$w0" "$w1" "$OUT" /tmp/fixB/samp.txt <<'PY' >> /tmp/fixB/gate.jsonl
import sys,json
l,s,bg,pf,pl,o0,o1,b0,b1,t0,t1,w0,w1,out,sf=sys.argv[1:]
rows=[tuple(map(float,x.split())) for x in open(sf) if len(x.split())==3]
best=0;bestb=0
for i in range(len(rows)):
  for j in range(i,len(rows)):
    if rows[j][0]-rows[i][0]>=10:
      d=rows[j][0]-rows[i][0]; v=(rows[j][1]-rows[i][1])/1e6/d
      if v>best: best=v; bestb=(rows[j][2]-rows[i][2])/1e6/d
      break
wall=float(w1)-float(w0); s=float(s)
try: r=json.loads(out)
except Exception: r={'raw':out[-500:]}
print(json.dumps(dict(label=l,speed=s,bg_target=float(bg),primary_first=pf,parent_load=float(pl),wall=wall,
  obs_box_cores=(int(o1)-int(o0))/1e6/wall, bg_box_cores=(int(b1 or 0)-int(b0 or 0))/1e6/wall,
  obs_prod_cores=(int(o1)-int(o0))/1e6/wall/s, bg_prod_cores=(int(b1 or 0)-int(b0 or 0))/1e6/wall/s,
  obs_peak10s_prod_cores=best/s, bg_at_peak_prod_cores=bestb/s, total_peak10s_prod_cores=(best+bestb)/s, throttled_s=(int(t1)-int(t0))/1e6, r=r)))
PY
echo "$LABEL done"
