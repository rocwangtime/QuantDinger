-- Additive paper-only account; no association with funded execution accounts.
CREATE TABLE IF NOT EXISTS qd_polymarket_paper_accounts (
 account_id VARCHAR(80) PRIMARY KEY,
 state JSONB NOT NULL,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS qd_polymarket_paper_trades (
 id VARCHAR(32) PRIMARY KEY,
 account_id VARCHAR(80) NOT NULL REFERENCES qd_polymarket_paper_accounts(account_id),
 market_id VARCHAR(80) NOT NULL,
 market_type VARCHAR(8) NOT NULL,
 status VARCHAR(32) NOT NULL CHECK(status IN ('pending','not_filled','filled_merged','single_leg_unwound','single_leg_residual','cancelled_by_limit')),
 signal_at TIMESTAMPTZ NOT NULL,
 released_at TIMESTAMPTZ,
 result JSONB NOT NULL,
 evidence BYTEA NOT NULL,
 evidence_hash VARCHAR(64) NOT NULL
);
CREATE INDEX IF NOT EXISTS qd_polymarket_paper_trades_account_time ON qd_polymarket_paper_trades(account_id,signal_at DESC);
CREATE TABLE IF NOT EXISTS qd_polymarket_paper_legs (
 trade_id VARCHAR(32) NOT NULL REFERENCES qd_polymarket_paper_trades(id) ON DELETE CASCADE,
 role VARCHAR(8) NOT NULL,
 detail JSONB NOT NULL,
 PRIMARY KEY(trade_id,role)
);
CREATE TABLE IF NOT EXISTS qd_polymarket_paper_ledger (
 id BIGSERIAL PRIMARY KEY,
 account_id VARCHAR(80) NOT NULL REFERENCES qd_polymarket_paper_accounts(account_id),
 observed_at TIMESTAMPTZ NOT NULL,
 event VARCHAR(40) NOT NULL,
 trade_id VARCHAR(32),
 free NUMERIC NOT NULL CHECK(free>=0),
 locked NUMERIC NOT NULL CHECK(locked>=0),
 positions NUMERIC NOT NULL CHECK(positions>=0),
 equity NUMERIC NOT NULL CHECK(equity>=0),
 detail JSONB NOT NULL,
 CHECK(abs(free+locked+positions-equity)<0.000001)
);
CREATE INDEX IF NOT EXISTS qd_polymarket_paper_ledger_time ON qd_polymarket_paper_ledger(account_id,observed_at);
CREATE TABLE IF NOT EXISTS qd_polymarket_paper_daily (
 account_id VARCHAR(80) NOT NULL REFERENCES qd_polymarket_paper_accounts(account_id),
 day DATE NOT NULL,
 stats JSONB NOT NULL,
 PRIMARY KEY(account_id,day)
);
