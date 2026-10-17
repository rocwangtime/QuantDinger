#!/usr/bin/env bash
# Read the container network namespace, not the host's unrelated SSH sockets.
set -uo pipefail
pid=$(timeout -k 1 3 docker inspect -f '{{.State.Pid}}' quantdinger-polymarket-observer) || exit
for ((sample=0; sample<60; sample++)); do
  date -Ins
  timeout -k 1 2 nsenter -t "$pid" -n ss -tminp state established '( dport = :443 )' || true
  if ((sample % 10 == 0)); then
    timeout -k 1 2 nsenter -t "$pid" -n cat /proc/net/snmp /proc/net/netstat || true
  fi
  sleep 1
done
