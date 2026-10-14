-- Offline research only; preserve missing historical token identity.
ALTER TABLE qd_polymarket_observer_windows ADD COLUMN IF NOT EXISTS up_token_id TEXT;
ALTER TABLE qd_polymarket_observer_windows ADD COLUMN IF NOT EXISTS down_token_id TEXT;
CREATE TABLE IF NOT EXISTS qd_polymarket_backtests (
    id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    config JSONB NOT NULL,
    result JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS qd_pm_backtest_created ON qd_polymarket_backtests(created_at DESC);
