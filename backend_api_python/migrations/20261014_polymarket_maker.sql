-- Paper maker evidence; balances and daily metrics use the existing account ledger.
CREATE TABLE IF NOT EXISTS qd_polymarket_maker_events (
    id UUID PRIMARY KEY,
    account_id TEXT NOT NULL,
    strategy TEXT NOT NULL,
    market_id TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    event TEXT NOT NULL,
    detail JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS qd_pm_maker_account_time ON qd_polymarket_maker_events(account_id,observed_at);
CREATE INDEX IF NOT EXISTS qd_pm_maker_market_time ON qd_polymarket_maker_events(market_id,observed_at);
