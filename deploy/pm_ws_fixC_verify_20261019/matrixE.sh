cd /tmp/fixB
export QD_REPLAY_FULL_DRAIN=1 GATE_OUT=/tmp/fixC/gateC.jsonl
A="--process-per-group --seconds 30 --kicks 1"
./run1.sh C_s65L_3300_pf0_r2 0.65 0.44 0 0.2 -- $A --rate 3300; echo r2 done
./run1.sh C_s65L_3300_pf0_r3 0.65 0.44 0 0.2 -- $A --rate 3300; echo r3 done
echo MATRIXE_DONE
