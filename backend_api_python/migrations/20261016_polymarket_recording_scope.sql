-- Wider public Up/Down recording scope (paper only). Repeatable; no data changes.
-- Binance-candle windows (1h/1d) settle on single prints: window_seconds=0 keeps
-- them out of the Chainlink 60 s TWAP joins. Existing rows keep their value (60).
DO $$
DECLARE item RECORD;
BEGIN
    FOR item IN SELECT conname FROM pg_constraint
                WHERE conrelid = 'qd_polymarket_observer_windows'::regclass AND contype = 'c'
                  AND conname <> 'qd_pm_windows_settlement_window'
                  AND pg_get_constraintdef(oid) LIKE '%window_seconds%'
    LOOP
        EXECUTE format('ALTER TABLE qd_polymarket_observer_windows DROP CONSTRAINT %I', item.conname);
    END LOOP;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'qd_polymarket_observer_windows'::regclass
                     AND conname = 'qd_pm_windows_settlement_window') THEN
        ALTER TABLE qd_polymarket_observer_windows
            ADD CONSTRAINT qd_pm_windows_settlement_window CHECK (window_seconds IN (0, 60));
    END IF;
END $$;
