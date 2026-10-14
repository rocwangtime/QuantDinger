-- Public, paper-only observer recording. Repeatable and additive.
CREATE TABLE IF NOT EXISTS qd_polymarket_observer_books (
    recording_id UUID PRIMARY KEY,
    market_id VARCHAR(80) NOT NULL,
    token_id TEXT NOT NULL,
    exchange_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL,
    source VARCHAR(8) NOT NULL,
    bids JSONB NOT NULL,
    asks JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS qd_pm_books_token_time ON qd_polymarket_observer_books(token_id,received_at);
CREATE INDEX IF NOT EXISTS qd_pm_books_time ON qd_polymarket_observer_books(received_at);
CREATE INDEX IF NOT EXISTS qd_pm_books_market_time ON qd_polymarket_observer_books(market_id,received_at);
CREATE TABLE IF NOT EXISTS qd_polymarket_observer_trades (
    recording_id UUID PRIMARY KEY,
    market_id VARCHAR(80) NOT NULL,
    token_id TEXT NOT NULL,
    price NUMERIC NOT NULL CHECK (price >= 0 AND price <= 1),
    size NUMERIC CHECK (size >= 0),
    side VARCHAR(4) NOT NULL CHECK (side IN ('BUY','SELL')),
    exchange_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL,
    transaction_hash TEXT
);
CREATE INDEX IF NOT EXISTS qd_pm_trades_token_time ON qd_polymarket_observer_trades(token_id,received_at);
CREATE INDEX IF NOT EXISTS qd_pm_trades_time ON qd_polymarket_observer_trades(received_at);
CREATE INDEX IF NOT EXISTS qd_pm_trades_market_time ON qd_polymarket_observer_trades(market_id,received_at);
CREATE TABLE IF NOT EXISTS qd_polymarket_observer_twap (
    asset VARCHAR(4) NOT NULL CHECK (asset IN ('BTC','ETH')),
    window_seconds INTEGER NOT NULL CHECK (window_seconds = 60),
    exchange_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    price NUMERIC NOT NULL CHECK (price > 0),
    source VARCHAR(20) NOT NULL CHECK (source = 'chainlink'),
    PRIMARY KEY(asset,window_seconds,exchange_at)
);
CREATE INDEX IF NOT EXISTS qd_pm_twap_time ON qd_polymarket_observer_twap(exchange_at);
CREATE TABLE IF NOT EXISTS qd_polymarket_observer_windows (
    market_id VARCHAR(80) PRIMARY KEY,
    condition_id TEXT NOT NULL,
    asset VARCHAR(4) NOT NULL,
    interval VARCHAR(3) NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    window_seconds INTEGER NOT NULL DEFAULT 60 CHECK (window_seconds = 60),
    price_to_beat NUMERIC,
    start_twap_at TIMESTAMPTZ,
    settlement_twap NUMERIC,
    settlement_twap_at TIMESTAMPTZ,
    computed_outcome VARCHAR(4),
    official_outcome VARCHAR(4),
    outcome_matches BOOLEAN,
    resolution JSONB,
    resolution_checked_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS qd_pm_windows_time ON qd_polymarket_observer_windows(end_time);
CREATE INDEX IF NOT EXISTS qd_pm_windows_pending ON qd_polymarket_observer_windows(end_time,resolution_checked_at) WHERE official_outcome IS NULL;
ALTER TABLE qd_polymarket_observer_samples ADD COLUMN IF NOT EXISTS chainlink_observed_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS qd_pm_samples_market_time ON qd_polymarket_observer_samples(market_id,observed_at);
