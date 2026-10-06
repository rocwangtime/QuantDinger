-- Observed forward performance only; no synthetic backtest history.
CREATE TABLE IF NOT EXISTS qd_agent_automation_samples (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NOT NULL REFERENCES qd_agent_automations(id) ON DELETE CASCADE,
    sampled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    report JSONB NOT NULL,
    UNIQUE(task_id, sampled_at)
);
CREATE INDEX IF NOT EXISTS idx_agent_samples_task_time
    ON qd_agent_automation_samples(task_id, sampled_at DESC);
