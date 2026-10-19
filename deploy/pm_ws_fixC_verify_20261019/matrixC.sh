#!/bin/bash
cd /tmp/fixB
export QD_REPLAY_FULL_DRAIN=1 GATE_OUT=/tmp/fixC/gateC.jsonl
A="--process-per-group --seconds 30 --kicks 1"
for t in 1 2 3; do ./run1.sh C_s78L_3300_pf0_t$t 0.78 0.44 0 0.2 -- $A --rate 3300; done
for t in 1 2; do ./run1.sh C_s78L_3300_pf1_t$t 0.78 0.44 1 0.2 -- $A --rate 3300; done
for t in 1 2; do ./run1.sh C_s78L_4950_pf0_t$t 0.78 0.44 0 0.2 -- $A --rate 4950; done
./run1.sh C_s78L_4950_pf1_t1 0.78 0.44 1 0.2 -- $A --rate 4950
for t in 1 2; do ./run1.sh C_s65_3300_pf0_t$t 0.65 0.44 0 0 -- $A --rate 3300; done
for t in 1 2; do ./run1.sh C_s65_4950_pf0_t$t 0.65 0.44 0 0 -- $A --rate 4950; done
./run1.sh C_s65L_3300_pf0_t1 0.65 0.44 0 0.2 -- $A --rate 3300
echo MATRIXC_DONE
