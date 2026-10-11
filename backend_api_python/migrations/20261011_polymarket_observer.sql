CREATE TABLE IF NOT EXISTS qd_polymarket_observer_status (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    status JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS qd_polymarket_observer_episodes (
    id BIGSERIAL PRIMARY KEY,
    market_id VARCHAR(80) NOT NULL,
    slug VARCHAR(300) NOT NULL DEFAULT '',
    question VARCHAR(1000) NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    duration_ms BIGINT NOT NULL DEFAULT 0,
    max_net_edge NUMERIC NOT NULL,
    max_net_edge_bps NUMERIC NOT NULL,
    best_size NUMERIC NOT NULL,
    sample_count INTEGER NOT NULL DEFAULT 1,
    source VARCHAR(8) NOT NULL CHECK (source IN ('ws','rest'))
);
CREATE UNIQUE INDEX IF NOT EXISTS qd_polymarket_observer_one_open_episode
    ON qd_polymarket_observer_episodes(market_id) WHERE ended_at IS NULL;
CREATE INDEX IF NOT EXISTS qd_polymarket_observer_episodes_started
    ON qd_polymarket_observer_episodes(started_at DESC);
CREATE TABLE IF NOT EXISTS qd_polymarket_observer_near_misses (
    market_id VARCHAR(80) NOT NULL,
    hour_bucket TIMESTAMPTZ NOT NULL,
    slug VARCHAR(300) NOT NULL DEFAULT '',
    question VARCHAR(1000) NOT NULL DEFAULT '',
    observed_at TIMESTAMPTZ NOT NULL,
    net_edge NUMERIC NOT NULL,
    net_edge_bps NUMERIC NOT NULL,
    best_edge_bps NUMERIC NOT NULL,
    samples INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (market_id, hour_bucket)
);
