-- Research-only storage. No task, intent, account or order writes.
CREATE TABLE IF NOT EXISTS qd_us_screener_runs (
 id BIGSERIAL PRIMARY KEY, as_of DATE NOT NULL, run_key TEXT NOT NULL UNIQUE,
 params JSONB NOT NULL, regime JSONB NOT NULL DEFAULT '{}',
 status TEXT NOT NULL CHECK(status IN ('queued','running','completed','partial','failed')),
 counts JSONB NOT NULL DEFAULT '{}', error TEXT NOT NULL DEFAULT '',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), finished_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS qd_us_screener_symbols (
 run_id BIGINT NOT NULL REFERENCES qd_us_screener_runs(id) ON DELETE CASCADE,
 symbol TEXT NOT NULL, features JSONB NOT NULL, score DOUBLE PRECISION, rank INTEGER,
 included BOOLEAN NOT NULL DEFAULT FALSE, exclusion_reasons JSONB NOT NULL DEFAULT '[]',
 flags JSONB NOT NULL DEFAULT '[]', PRIMARY KEY(run_id,symbol)
);
CREATE TABLE IF NOT EXISTS qd_us_screener_bar_cache (
 symbol TEXT NOT NULL, as_of DATE NOT NULL, bars JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY(symbol,as_of)
);
CREATE TABLE IF NOT EXISTS qd_us_screener_quote_cache (
 symbol TEXT PRIMARY KEY, quote JSONB NOT NULL, observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
