CREATE TABLE IF NOT EXISTS qd_agent_automations (
 id BIGSERIAL PRIMARY KEY,
 user_id INTEGER NOT NULL REFERENCES qd_users(id) ON DELETE CASCADE,
 name VARCHAR(120) NOT NULL,
 config JSONB NOT NULL,
 active BOOLEAN NOT NULL DEFAULT FALSE,
 revision INTEGER NOT NULL DEFAULT 1,
 token_id INTEGER NOT NULL REFERENCES qd_agent_tokens(id),
 state JSONB NOT NULL DEFAULT '{}',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS qd_agent_automation_runs (
 id BIGSERIAL PRIMARY KEY,
 task_id BIGINT NOT NULL REFERENCES qd_agent_automations(id),
 user_id INTEGER NOT NULL REFERENCES qd_users(id),
 revision INTEGER NOT NULL,
 event_key VARCHAR(120) NOT NULL,
 status VARCHAR(30) NOT NULL DEFAULT 'queued',
 phase TEXT NOT NULL DEFAULT '',
 preview BOOLEAN NOT NULL DEFAULT FALSE,
 cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
 evidence JSONB NOT NULL DEFAULT '{}',
 result JSONB NOT NULL DEFAULT '{}',
 draft TEXT NOT NULL DEFAULT '',
 execute_at TIMESTAMPTZ,
 expires_at TIMESTAMPTZ NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 finished_at TIMESTAMPTZ,
 UNIQUE(task_id,event_key)
);
CREATE INDEX IF NOT EXISTS idx_automation_runs_task ON qd_agent_automation_runs(task_id,id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_automation_one_running ON qd_agent_automation_runs(task_id)
 WHERE status IN ('queued','researching','executing');
