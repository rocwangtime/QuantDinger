-- Seed one inactive, plan-only AAPL task per existing US automation account.
-- No account discovery, activation, PAPER_AUTO authorization, or broker calls.
DO $seed$
DECLARE source RECORD; actor INTEGER; task BIGINT; params JSONB := '{"budget": 15000, "per_order_cap": 3000, "daily_buy_cap": 6000, "cash_reserve_pct": 20, "max_position": 9000, "prev_close_pct": 2, "short_move_pct": 1, "window_minutes": 5, "cooldown_minutes": 30, "max_triggers_day": 8, "blackout_auction": true, "blackout_lunch": false, "blackout_close_minutes": 5, "stop_loss_pct": 5, "daily_loss_pct": 2, "max_drawdown_pct": 8, "model": "deepseek-v4-1-flash", "mode": "plan_only", "lot_size": 1, "reference_price": null, "max_slippage_pct": 0.5, "spread_limit_pct": 1, "limit_offset_ticks": null, "blackout_open_minutes": 5, "extended_hours": false}'::jsonb;
BEGIN
 FOR source IN SELECT DISTINCT user_id, config->'credential_id' AS credential_id
 FROM qd_agent_automations WHERE config->>'market'='USStock'
 LOOP
  IF EXISTS (SELECT 1 FROM qd_agent_automations WHERE user_id=source.user_id
      AND config->'credential_id'=source.credential_id
      AND config->>'market'='USStock' AND config->>'kind'='price_trigger'
      AND config->'symbols'='["AAPL"]'::jsonb) THEN CONTINUE; END IF;
  INSERT INTO qd_agent_tokens(user_id,name,token_prefix,token_hash,scopes,markets,instruments,
      paper_only,max_order_notional,max_daily_notional,status)
  VALUES(source.user_id,'automation-task','seed-us-aapl',
      md5(random()::text || clock_timestamp()::text) || md5(random()::text),
      'T','USStock','AAPL',TRUE,3000,6000,'internal') RETURNING id INTO actor;
  INSERT INTO qd_agent_automations(user_id,name,config,active,token_id)
  VALUES(source.user_id,'AAPL price trigger', '{"kind": "price_trigger", "market": "USStock", "symbols": ["AAPL"], "execution_mode": "plan_only", "budget": 15000, "max_weight": 0.6, "reserve_ratio": 0.2, "max_order_notional": 3000, "max_daily_notional": 6000, "before_open_minutes": 60, "execute_after_open_minutes": 5, "cooldown_seconds": 1800, "brief": "", "manage_existing": false, "decision_timeout_seconds": 15, "risk": {"enabled": true, "stop_loss_pct": 0.05, "max_daily_loss_pct": 0.02, "max_drawdown_pct": 0.08, "daily_loss_budget": true}, "research": {"mode": "snapshot", "max_model_calls": 3, "max_tool_requests": 6, "max_output_tokens": 7000, "max_decisions_per_day": 8}, "trigger": {"type": "price_above", "price": 1.0}, "llm_selection": {"provider": "deepseek", "model": "deepseek-v4-1-flash", "reasoning_effort": "default"}, "symbol_params": {"budget": 15000, "per_order_cap": 3000, "daily_buy_cap": 6000, "cash_reserve_pct": 20, "max_position": 9000, "prev_close_pct": 2, "short_move_pct": 1, "window_minutes": 5, "cooldown_minutes": 30, "max_triggers_day": 8, "blackout_auction": true, "blackout_lunch": false, "blackout_close_minutes": 5, "stop_loss_pct": 5, "daily_loss_pct": 2, "max_drawdown_pct": 8, "model": "deepseek-v4-1-flash", "mode": "plan_only", "lot_size": 1, "reference_price": null, "max_slippage_pct": 0.5, "spread_limit_pct": 1, "limit_offset_ticks": null, "blackout_open_minutes": 5, "extended_hours": false}}'::jsonb ||
      jsonb_build_object('credential_id',source.credential_id),FALSE,actor)
  RETURNING id INTO task;
  INSERT INTO qd_agent_symbol_configs(task_id,symbol,params) VALUES(task,'AAPL',params);
  INSERT INTO qd_agent_symbol_config_audit(task_id,symbol,user_id,field,old_value,new_value)
  SELECT task,'AAPL',source.user_id,key,'null'::jsonb,value FROM jsonb_each(params);
 END LOOP;
END $seed$;
