#!/bin/bash
cd /tmp/fixB
S=0.65
./run1.sh base_sp_4950 $S 0.44 1 0 -- --seconds 30 --rate 4950 --market-loop --rest-hour-buckets 1648 --prod-gil
for t in 1 2 3; do
 for R in 3300 4950; do
  for PF in 1 0; do
   ./run1.sh pb_${R}_pf${PF}_t$t $S 0.44 $PF 0 -- --process-per-group --seconds 30 --rate $R --kicks 1
  done
 done
done
for t in 1 2; do for R in 3300 4950; do
 ./run1.sh pbload_${R}_pf1_t$t $S 0.44 1 0.2 -- --process-per-group --seconds 30 --rate $R --kicks 1
done; done
echo MATRIX_DONE
