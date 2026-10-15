CREATE TABLE IF NOT EXISTS qd_us_screener_earnings_cache (
 symbol TEXT PRIMARY KEY, dates JSONB NOT NULL, source TEXT NOT NULL,
 fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
