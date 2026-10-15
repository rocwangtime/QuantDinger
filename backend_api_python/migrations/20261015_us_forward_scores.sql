-- Research score horizons only; no task or activation writes.
ALTER TABLE qd_agent_decision_scores DROP CONSTRAINT IF EXISTS qd_agent_decision_scores_horizon_check;
ALTER TABLE qd_agent_decision_scores ADD CONSTRAINT qd_agent_decision_scores_horizon_check
 CHECK (horizon IN ('5m','30m','close','next_close','1d','5d'));
