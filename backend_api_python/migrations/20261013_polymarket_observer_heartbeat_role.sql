-- Allow the dedicated observer to publish worker heartbeats.
ALTER TABLE qd_worker_heartbeats
    DROP CONSTRAINT IF EXISTS qd_worker_heartbeats_role_check;
ALTER TABLE qd_worker_heartbeats
    ADD CONSTRAINT qd_worker_heartbeats_role_check
    CHECK (role IN ('api', 'trading', 'scheduler', 'celery', 'celery-beat', 'polymarket-observer'));
