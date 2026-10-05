"""Durable order identity helpers for ambiguous broker submissions."""

from __future__ import annotations

from typing import Any, Callable

from app.services.live_trading.contracts import OrderIntent

from app.services.pending_orders.error_classification import classify_exchange_order_error
from app.utils.db import get_db_connection


class SubmissionRecoveryMixin:
    def _broker_submission_preparer(
        self,
        *,
        order_id: int,
        exchange_id: str,
        market_type: str,
        client_order_id: str,
    ) -> Callable[[], None]:
        def prepare() -> None:
            self._prepare_submission(
                order_id=order_id,
                exchange_id=exchange_id,
                market_type=market_type,
                client_order_id=client_order_id,
            )

        return prepare

    def _recover_stale_submissions(self, stale_sec: int) -> None:
        """Move precommitted submissions to reconciliation after a worker crash."""
        if int(stale_sec or 0) <= 0:
            return
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute(
                """
                UPDATE pending_orders
                SET status = 'sent',
                    updated_at = NOW(),
                    dispatch_note = 'reconcile_stale_submission'
                WHERE status = 'processing'
                  AND COALESCE(client_order_id, '') <> ''
                  AND (updated_at IS NULL OR updated_at < NOW() - INTERVAL '%s seconds')
                """,
                (int(stale_sec),),
            )
            db.commit()
            cur.close()

    def _submission_preparer(
        self,
        *,
        order_id: int,
        exchange_id: str,
        market_type: str,
    ) -> Callable[[OrderIntent], None]:
        def prepare(intent: OrderIntent) -> None:
            self._prepare_submission(
                order_id=order_id,
                exchange_id=exchange_id,
                market_type=market_type,
                client_order_id=str(intent.client_order_id or ""),
            )

        return prepare

    def _prepare_submission(
        self,
        *,
        order_id: int,
        exchange_id: str,
        market_type: str,
        client_order_id: str,
        credential_id: int | None = None,
    ) -> None:
        """Persist the broker identity before the external submit call."""
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute(
                """
                UPDATE pending_orders
                SET exchange_id = %s,
                    credential_id = COALESCE(%s, credential_id),
                    client_order_id = %s,
                    dispatch_note = 'submission_prepared',
                    updated_at = NOW()
                WHERE id = %s AND status = 'processing'
                """,
                (
                    str(exchange_id or ""),
                    credential_id,
                    str(client_order_id or ""),
                    int(order_id),
                ),
            )
            if int(getattr(cur, "rowcount", 1) or 0) != 1:
                cur.close()
                raise RuntimeError("pending_order_submission_lease_lost")
            db.commit()
            cur.close()
        self._register_pending_order_binding(
            order_id=order_id,
            client_order_id=client_order_id,
            exchange_order_id="",
            exchange_id=exchange_id,
            market_type=market_type,
            observed_filled=0.0,
        )

    def _bind_reconciled_exchange_order_id(
        self,
        *,
        order_id: int,
        exchange_id: str,
        market_type: str,
        client_order_id: str,
        exchange_order_id: str,
        observed_filled: float = 0.0,
    ) -> None:
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute(
                """
                UPDATE pending_orders
                SET exchange_order_id = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (str(exchange_order_id or ""), int(order_id)),
            )
            db.commit()
            cur.close()
        self._register_pending_order_binding(
            order_id=order_id,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            exchange_id=exchange_id,
            market_type=market_type,
            observed_filled=observed_filled,
        )

    def _mark_submit_unknown(self, *, order_id: int, error: str) -> None:
        """Keep an ambiguous submission in reconciliation instead of retrying it."""
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute(
                """
                UPDATE pending_orders
                SET status = 'sent',
                    last_error = %s,
                    dispatch_note = 'submit_outcome_unknown',
                    sent_at = COALESCE(sent_at, NOW()),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (str(error or "submit_outcome_unknown"), int(order_id)),
            )
            cur.execute(
                """
                UPDATE strategy_order_intents soi
                SET status = 'submitted', updated_at = NOW()
                FROM pending_orders po
                WHERE po.id = %s AND po.order_intent_id = soi.id
                """,
                (int(order_id),),
            )
            db.commit()
            cur.close()

    @staticmethod
    def _is_ambiguous_submit_error(error: Any) -> bool:
        return classify_exchange_order_error(error).get("category") == "transport"
