#!/bin/bash
cd /tmp/fixB
while pgrep -f "matrixC\.sh" >/dev/null; do sleep 5; done
export QD_REPLAY_FULL_DRAIN=1 GATE_OUT=/tmp/fixC/gateC.jsonl QD_REPLAY_BOOK_EVERY=2000
A="--process-per-group --seconds 45 --kicks 1"
for t in 1 2; do ./run1.sh D_realbooks_s78L_3300_pf0_t$t 0.78 0.44 0 0.2 -- $A --rate 3300; done
echo MATRIXD_DONE
