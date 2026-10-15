-- Discard the legacy transient cache: it cannot identify a captured session.
ALTER TABLE qd_us_screener_quote_cache ADD COLUMN IF NOT EXISTS session_date DATE;
DELETE FROM qd_us_screener_quote_cache WHERE session_date IS NULL;
ALTER TABLE qd_us_screener_quote_cache ALTER COLUMN session_date SET NOT NULL;
ALTER TABLE qd_us_screener_quote_cache DROP CONSTRAINT IF EXISTS qd_us_screener_quote_cache_pkey;
ALTER TABLE qd_us_screener_quote_cache ADD PRIMARY KEY (symbol, session_date);
CREATE TABLE IF NOT EXISTS qd_us_screener_quote_captures (
 session_date DATE PRIMARY KEY, status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), finished_at TIMESTAMPTZ
);
-- Repair legacy empty completions so shortlist resolution can use a prior valid run.
UPDATE qd_us_screener_runs r
SET status='partial', error='no_eligible_symbols',
    regime=regime || '{"reason":"no_eligible_symbols"}'::jsonb
WHERE status='completed' AND NOT EXISTS (
 SELECT 1 FROM qd_us_screener_symbols s
 WHERE s.run_id=r.id AND s.included=TRUE AND s.exclusion_reasons='[]'::jsonb
);
