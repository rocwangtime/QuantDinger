#!/usr/bin/env bash
# Observer-only read-only slow lane. Invoked on qd-sg via sudo bash -s.
set -uo pipefail
since=${1:-2026-10-16T18:04:03Z}
for endpoint in health health/ready health/workers; do
  printf '%s ' "$endpoint"
  curl --connect-timeout 2 --max-time 4 -s -o /dev/null -w '%{http_code}\n' "http://127.0.0.1:5001/api/$endpoint" || true
done
timeout -k 2 12 docker exec -i quantdinger-db sh -c \
 'PGOPTIONS="-c statement_timeout=5000 -c lock_timeout=1000 -c default_transaction_read_only=on" psql -X -qtA -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SELECT 'STATUS|' || status::text FROM qd_polymarket_observer_status;
SQL
# Read the log once per sample, with a hard limit on both time and output.
timeout -k 2 10 docker logs --tail 500 --since "$since" --timestamps quantdinger-polymarket-observer 2>&1 |
  grep -Ei 'slow consumer|close shard|transport failure|generation invalidated|reconnect|error|traceback' || true
