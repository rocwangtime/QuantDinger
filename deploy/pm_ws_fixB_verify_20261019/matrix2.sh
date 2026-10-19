#!/bin/bash
cd /tmp/fixB
while kill -0 808640 2>/dev/null; do sleep 5; done
mv /tmp/fixB/wt/backend_api_python/scripts/polymarket_ws_worker_replay.py.tmp /tmp/fixB/wt/backend_api_python/scripts/polymarket_ws_worker_replay.py
export QD_REPLAY_FULL_DRAIN=1
for t in 1 2; do for R in 3300 4950; do for PF in 1 0; do
  ./run1.sh s78_${R}_pf${PF}_t$t 0.78 0.44 $PF 0 -- --process-per-group --seconds 30 --rate $R --kicks 1
done; done; done
for PF in 1 0; do ./run1.sh drain65_4950_pf${PF} 0.65 0.44 $PF 0 -- --process-per-group --seconds 30 --rate 4950 --kicks 1; done
./run1.sh s78_sp_3300 0.78 0.44 1 0 -- --seconds 30 --rate 3300 --market-loop --rest-hour-buckets 1648 --prod-gil
./run1.sh s78_sp_4950 0.78 0.44 1 0 -- --seconds 30 --rate 4950 --market-loop --rest-hour-buckets 1648 --prod-gil
echo MATRIX2_DONE
