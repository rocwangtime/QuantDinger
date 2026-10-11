ALTER TABLE qd_polymarket_observer_episodes
    ADD COLUMN IF NOT EXISTS market_type VARCHAR(8) NOT NULL DEFAULT 'yes_no',
    ADD COLUMN IF NOT EXISTS asset VARCHAR(4),
    ADD COLUMN IF NOT EXISTS interval VARCHAR(3),
    ADD COLUMN IF NOT EXISTS expiry_bucket VARCHAR(8) NOT NULL DEFAULT '';
ALTER TABLE qd_polymarket_observer_near_misses
    ADD COLUMN IF NOT EXISTS market_type VARCHAR(8) NOT NULL DEFAULT 'yes_no',
    ADD COLUMN IF NOT EXISTS asset VARCHAR(4),
    ADD COLUMN IF NOT EXISTS interval VARCHAR(3),
    ADD COLUMN IF NOT EXISTS expiry_bucket VARCHAR(8) NOT NULL DEFAULT '';
-- A deterministic tag key preserves nullable YES/NO tags without NULL uniqueness gaps.
ALTER TABLE qd_polymarket_observer_near_misses
    ADD COLUMN IF NOT EXISTS tag_key VARCHAR(40) NOT NULL DEFAULT 'yes_no|||';
ALTER TABLE qd_polymarket_observer_near_misses DROP CONSTRAINT IF EXISTS qd_polymarket_observer_near_misses_pkey;
ALTER TABLE qd_polymarket_observer_near_misses ADD PRIMARY KEY (market_id,hour_bucket,tag_key);
CREATE TABLE IF NOT EXISTS qd_polymarket_observer_samples (
    id BIGSERIAL PRIMARY KEY,
    market_id VARCHAR(80) NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    market_type VARCHAR(8) NOT NULL DEFAULT 'up_down',
    asset VARCHAR(4) NOT NULL,
    interval VARCHAR(3) NOT NULL,
    expiry_bucket VARCHAR(8) NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    up_ask NUMERIC,
    down_ask NUMERIC,
    up_mid NUMERIC,
    down_mid NUMERIC,
    price_to_beat NUMERIC,
    reference_price NUMERIC,
    reference_source VARCHAR(40) NOT NULL DEFAULT 'unavailable',
    reference_observed_at TIMESTAMPTZ,
    chainlink_price NUMERIC,
    source VARCHAR(8) NOT NULL,
    net_edge_bps NUMERIC
);
CREATE INDEX IF NOT EXISTS qd_polymarket_observer_samples_time ON qd_polymarket_observer_samples(observed_at);
