-- Default-deny, durable Futu SIMULATE order permission.
CREATE TABLE IF NOT EXISTS qd_futu_automation_state (
    acc_id BIGINT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES qd_users(id) ON DELETE CASCADE,
    credential_id INTEGER NOT NULL REFERENCES qd_exchange_credentials(id) ON DELETE CASCADE,
    min_pending_order_id BIGINT NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    state VARCHAR(24) NOT NULL DEFAULT 'paused',
    last_error TEXT NOT NULL DEFAULT '',
    armed_at TIMESTAMPTZ,
    paused_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE qd_futu_automation_state
  ADD COLUMN IF NOT EXISTS min_pending_order_id BIGINT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_futu_automation_user ON qd_futu_automation_state(user_id);
