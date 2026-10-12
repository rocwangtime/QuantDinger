CREATE TABLE IF NOT EXISTS qd_agent_symbol_configs (
 task_id BIGINT NOT NULL REFERENCES qd_agent_automations(id) ON DELETE CASCADE,
 symbol VARCHAR(20) NOT NULL, params JSONB NOT NULL,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY(task_id,symbol)
);
CREATE TABLE IF NOT EXISTS qd_agent_symbol_config_audit (
 id BIGSERIAL PRIMARY KEY, task_id BIGINT NOT NULL REFERENCES qd_agent_automations(id),
 symbol VARCHAR(20) NOT NULL, user_id BIGINT NOT NULL REFERENCES qd_users(id),
 changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), field VARCHAR(100) NOT NULL,
 old_value JSONB, new_value JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_agent_symbol_audit ON qd_agent_symbol_config_audit(task_id,symbol,id DESC);
