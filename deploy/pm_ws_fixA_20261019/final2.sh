#!/bin/bash
V=/tmp/pm-round6-venv/bin
OUT=/tmp/fixA/final2.jsonl
taskset -c 4,5 $V/python /tmp/fixA/burn.py & BURN=$!
run() { tree=$1; label=$2; rate=$3; t=$4
  cd $tree/backend_api_python
  out=$(PYTHONPATH=. taskset -c 4,5 $V/python /tmp/fixA-replay.py --seconds 20 --rate $rate --lock-timing --market-loop --rest-hour-buckets 1648 --prod-gil 2>>/tmp/fixA/final2.err | tail -1)
  echo "{\"label\":\"$label\",\"rate\":$rate,\"trial\":$t,\"r\":$out}" >> $OUT
}
for spec in "2800 4" "3300 6" "3800 8"; do set -- $spec; rate=$1; n=$2
 for t in $(seq 1 $n); do
  if [ $((t % 2)) -eq 1 ]; then run /tmp/fixA-base base $rate $t; run /workspace/quantdinger/QuantDinger fixA $rate $t
  else run /workspace/quantdinger/QuantDinger fixA $rate $t; run /tmp/fixA-base base $rate $t; fi
 done
done
kill $BURN; echo DONE
