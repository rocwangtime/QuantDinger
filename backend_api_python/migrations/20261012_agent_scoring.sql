-- Immutable decision inputs and idempotent post-hoc observations.
CREATE TABLE IF NOT EXISTS qd_agent_decision_scores (
    run_id BIGINT NOT NULL REFERENCES qd_agent_automation_runs(id) ON DELETE CASCADE,
    task_id BIGINT NOT NULL REFERENCES qd_agent_automations(id) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    horizon TEXT NOT NULL CHECK (horizon IN ('5m','30m','close','next_close')),
    decision_at TIMESTAMPTZ NOT NULL,
    due_at TIMESTAMPTZ NOT NULL,
    inputs JSONB NOT NULL,
    outcome JSONB,
    checked_at TIMESTAMPTZ,
    PRIMARY KEY(run_id,symbol,horizon)
);
CREATE INDEX IF NOT EXISTS idx_agent_scores_due ON qd_agent_decision_scores(due_at) WHERE outcome IS NULL;
CREATE TABLE IF NOT EXISTS qd_agent_daily_reports (
    task_id BIGINT NOT NULL REFERENCES qd_agent_automations(id) ON DELETE CASCADE,
    day DATE NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    report JSONB NOT NULL,
    PRIMARY KEY(task_id,day)
);
CREATE TABLE IF NOT EXISTS qd_agent_risk_events (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL REFERENCES qd_agent_automations(id) ON DELETE CASCADE,
    occurred_at TIMESTAMPTZ NOT NULL,
    event JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_agent_risk_events_time ON qd_agent_risk_events(task_id,occurred_at);
