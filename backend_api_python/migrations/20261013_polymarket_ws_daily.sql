-- UTC cumulative session checkpoints; retries overwrite rather than double count.
CREATE TABLE IF NOT EXISTS qd_polymarket_ws_daily (
    day date NOT NULL,
    session_id text NOT NULL,
    reconnect_count bigint NOT NULL DEFAULT 0,
    slow_consumer_count bigint NOT NULL DEFAULT 0,
    rebalance_count bigint NOT NULL DEFAULT 0,
    shard_msg_totals jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (day, session_id)
);
