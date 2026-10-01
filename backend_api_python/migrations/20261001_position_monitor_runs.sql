CREATE TABLE IF NOT EXISTS qd_position_monitor_runs (
    id BIGSERIAL PRIMARY KEY,
    monitor_id INTEGER NOT NULL REFERENCES qd_position_monitors(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES qd_users(id) ON DELETE CASCADE,
    status VARCHAR(16) NOT NULL,
    result_json TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_position_monitor_runs_owner
    ON qd_position_monitor_runs(user_id, monitor_id, id DESC);
