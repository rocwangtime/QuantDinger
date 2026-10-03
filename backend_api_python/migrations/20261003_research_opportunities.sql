CREATE TABLE IF NOT EXISTS qd_research_opportunities (
    id BIGSERIAL PRIMARY KEY,
    monitor_id INTEGER NOT NULL REFERENCES qd_position_monitors(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES qd_users(id) ON DELETE CASCADE,
    run_id BIGINT NOT NULL REFERENCES qd_position_monitor_runs(id) ON DELETE CASCADE,
    market VARCHAR(32) NOT NULL,
    symbol VARCHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'new'
      CHECK (status IN ('new', 'reviewed', 'dismissed')),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, market, symbol)
);
CREATE INDEX IF NOT EXISTS idx_research_opportunities_owner
    ON qd_research_opportunities(user_id, status, id DESC);
