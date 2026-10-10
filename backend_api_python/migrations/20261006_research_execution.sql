CREATE TABLE IF NOT EXISTS qd_research_studies (
 study_id VARCHAR(64) PRIMARY KEY,
 user_id INTEGER NOT NULL REFERENCES qd_users(id) ON DELETE CASCADE,
 source_id BIGINT NOT NULL,
 code_hash VARCHAR(64) NOT NULL,
 reserved_trials INTEGER NOT NULL CHECK (reserved_trials >= 0),
 bundle_id VARCHAR(64) NOT NULL DEFAULT '',
 holdout_key VARCHAR(64) NOT NULL DEFAULT '',
 holdout_start TIMESTAMPTZ,
 holdout_end TIMESTAMPTZ,
 holdout_exposed BOOLEAN NOT NULL DEFAULT FALSE,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_research_studies_family ON qd_research_studies(user_id, source_id);

CREATE TABLE IF NOT EXISTS qd_fundamental_revisions (
 id BIGSERIAL PRIMARY KEY,
 market TEXT NOT NULL,
 symbol TEXT NOT NULL,
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 snapshot JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fundamental_revision_lookup ON qd_fundamental_revisions(market, symbol, recorded_at);
-- Baseline is known at migration time; do not backdate unknown revision history.
INSERT INTO qd_fundamental_revisions(market, symbol, snapshot)
SELECT market, symbol, to_jsonb(current_snapshot)
FROM qd_fundamental_snapshots current_snapshot
WHERE NOT EXISTS (SELECT 1 FROM qd_fundamental_revisions revision
                  WHERE revision.snapshot->>'id' = current_snapshot.id::text);
CREATE OR REPLACE FUNCTION qd_archive_fundamental_revision() RETURNS TRIGGER AS $$
BEGIN
 INSERT INTO qd_fundamental_revisions(market, symbol, snapshot)
 VALUES (NEW.market, NEW.symbol, to_jsonb(NEW));
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS qd_fundamental_revision_archive ON qd_fundamental_snapshots;
CREATE TRIGGER qd_fundamental_revision_archive
AFTER INSERT OR UPDATE ON qd_fundamental_snapshots
FOR EACH ROW EXECUTE FUNCTION qd_archive_fundamental_revision();

CREATE TABLE IF NOT EXISTS qd_order_groups (
 group_id VARCHAR(64) PRIMARY KEY,
 user_id INTEGER NOT NULL REFERENCES qd_users(id) ON DELETE CASCADE,
 idempotency_key VARCHAR(120) NOT NULL,
 request_hash VARCHAR(64) NOT NULL,
 config JSONB NOT NULL,
 state JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 UNIQUE(user_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_order_groups_strategy ON qd_order_groups ((config->>'strategyId'));
CREATE INDEX IF NOT EXISTS idx_order_groups_active ON qd_order_groups(updated_at)
WHERE state->>'status' IN ('executing','unwinding');
