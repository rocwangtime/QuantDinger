#!/bin/bash
cd /tmp/fixB
while pgrep -f "matrix2\.sh" >/dev/null; do sleep 5; done
export QD_REPLAY_FULL_DRAIN=1
for t in 1 2; do for PF in 1 0; do
  ./run1.sh s78load_3300_pf${PF}_t$t 0.78 0.44 $PF 0.2 -- --process-per-group --seconds 30 --rate 3300 --kicks 1
done; done
./run1.sh s78_sp_3300_load 0.78 0.44 1 0 -- --seconds 30 --rate 3300 --market-loop --rest-hour-buckets 1648 --prod-gil --gil-load 0.2
echo MATRIX3_DONE
