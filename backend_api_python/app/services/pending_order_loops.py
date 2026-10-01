"""Independent order dispatch and exchange reconciliation loops."""
from app.utils.logger import get_logger

logger = get_logger(__name__)


class PendingOrderLoops:
    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception as e:
                logger.warning(f"PendingOrderWorker tick error: {e}")
            self._stop_event.wait(self.poll_interval_sec)

    def _run_sync_loop(self) -> None:
        while not self._stop_event.is_set():
            for sync in (self._sync_quick_trade_orders, self._sync_alpaca_sent_orders,
                         self._sync_futu_sent_orders, self._sync_futu_agent_intents,
                         self._sync_live_sent_orders, self._maybe_sync_positions):
                if self._stop_event.is_set() or (self.lease_guard and not self.lease_guard()):
                    break
                try:
                    sync()
                except Exception:
                    logger.warning("Order reconciliation failed: %s", sync.__name__, exc_info=True)
            self._stop_event.wait(self.poll_interval_sec)

    def _sync_futu_agent_intents(self) -> None:
        """Read-only broker recovery for Agent SIMULATE; never resubmit."""
        from app.services.futu_agent_execution import reconcile_simulate_intent
        from app.utils.db import get_db_connection

        with get_db_connection() as db:
            cur = db.cursor()
            try:
                cur.execute(
                    """SELECT DISTINCT ON (account_ref) id, user_id, account_ref
                       FROM qd_agent_trade_intents
                       WHERE broker='futu' AND status IN
                         ('EXECUTING','UNCERTAIN','SUBMITTED','PARTIALLY_FILLED')
                         AND (last_reconciled_at IS NULL OR
                              last_reconciled_at < NOW() - INTERVAL '10 seconds')
                       ORDER BY account_ref, last_reconciled_at ASC NULLS FIRST, id ASC
                       LIMIT 20"""
                )
                rows = cur.fetchall() or []
            finally:
                cur.close()
        for row in rows:
            if self._stop_event.is_set() or (self.lease_guard and not self.lease_guard()):
                break
            try:
                reconcile_simulate_intent(int(row["user_id"]), int(row["id"]))
            except Exception:
                logger.warning("Futu Agent order reconciliation unavailable intent_id=%s", row["id"])

    def _tick(self) -> None:
        if self.lease_guard and not self.lease_guard():
            return
        orders = self._fetch_pending_orders(limit=self.batch_size)
        if not orders:
            return

        for o in orders:
            if self._stop_event.is_set() or (self.lease_guard and not self.lease_guard()):
                break
            oid = o.get("id")
            if not oid:
                continue

            # Mark processing (best-effort)
            if not self._mark_processing(order_id=int(oid)):
                continue

            try:
                self._dispatch_one(o)
            except Exception as e:
                self._mark_failed(order_id=int(oid), error=str(e))
