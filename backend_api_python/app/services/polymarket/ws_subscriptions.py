"""Nonblocking snapshot admission, shared by active and warming public sockets."""
import json
import threading
from collections import deque
from datetime import datetime, timezone

from .ws_balance import setting


class SubscriptionPacer:
    def __init__(self):
        self.lock = threading.Lock()
        self.next_at = 0
        self.waiting = deque()

    def enqueue(self, state, assets, initial=False):
        with state['lock']:
            if initial:
                state['subscribe_pending'] = []
                state['subscribe_initial'] = True
            pending = state.setdefault('subscribe_pending', [])
            pending[:] = [a for a in pending if a in state['assets']]
            pending.extend(a for a in assets if a not in pending)
            if pending:
                state['subscriptions_complete_at'] = None
                with self.lock:
                    if not any(s is state for s in self.waiting):
                        self.waiting.append(state)

    def forget(self, state):
        with self.lock:
            self.waiting = deque(s for s in self.waiting if s is not state)

    def eligible(self, state, now):
        return (state.get('connected') and not state.get('retired') and state.get('subscribe_pending')
                and not (state.get('inbox') and state['inbox'].invalid.is_set())
                and (state.get('subscribe_initial') or
                     now >= state.get('subscribe_pause_until', 0) and not self.pressured(state)))

    @staticmethod
    def pressure_reasons(state):
        inbox = state.get('inbox')
        transport = getattr(getattr(state.get('socket'), 'transport', None), 'last', {})
        reasons = []
        if inbox:
            reasons.extend(name for name, flag in (('invalid', inbox.invalid.is_set()),
                ('resync', inbox.resync.is_set()), ('delivery_gap', inbox.delivery_gap),
                ('queue_depth', inbox.queue.qsize() > 32),
                ('queue_lag', inbox.queue.qsize() and inbox.lag_seconds > .25)) if flag)
        if (transport.get('tcp_pending_bytes') or 0) >= 65536:
            reasons.append('tcp_pending')
        if (transport.get('scheduler_lag_seconds') or 0) > .1:
            reasons.append('scheduler_lag')
        return reasons

    @classmethod
    def pressured(cls, state):
        return bool(cls.pressure_reasons(state))

    def drain(self, state, now):
        # First frame is synchronous in on_open; only later batches share the
        # round-robin clock. Small shards fit their entire subscription in it.
        with state['lock']:
            pending = state.get('subscribe_pending', [])
            if not self.eligible(state, now):
                return
            with self.lock:
                # Reserve one small first batch per new connection. It cannot
                # wait behind the shared budget (the server expects subscribe
                # before any heartbeat). Subsequent batches rotate fairly.
                if not state.get('subscribe_initial'):
                    turn = next((s for s in self.waiting if self.eligible(s, now)), None)
                    if now < self.next_at or turn is not state:
                        return
                size = int(setting('SUBSCRIBE_BATCH_ASSETS', 2, 1))
                if state.get('subscribe_initial'):
                    size = max(size, int(setting('SUBSCRIBE_INITIAL_ASSETS', 12, 1)))
                batch = pending[:size]
                if not state.get('subscribe_initial'):
                    self.next_at = now + max(setting('SUBSCRIBE_BATCH_SECONDS', .5, .05),
                                            len(batch) / setting('SUBSCRIBE_ASSETS_PER_S', 4, .1))
                self.waiting = deque(s for s in self.waiting if s is not state)
                self.waiting.append(state)
            command = dict(assets_ids=batch)
            if state.get('subscribe_initial'):
                command['type'] = 'market'
            else:
                command['operation'] = 'subscribe'
            inbox = state.get('inbox')
            if inbox:
                if not hasattr(inbox, 'activated'):
                    inbox.activated = {}
                for asset in batch:
                    inbox.activated[asset] = datetime.now(timezone.utc)
            state['socket'].send(json.dumps(command))
            ready = getattr(state['socket'], 'subscription_ready', None)
            if ready is not None:
                ready.set()
            del pending[:len(batch)]
            state['subscribe_initial'] = False
            state['subscribe_batches'] = state.get('subscribe_batches', 0) + 1
            if not pending:
                self.forget(state)
                state['subscriptions_complete_at'] = now
                state['warm_since'] = now

    def settled(self, state, now):
        completed = state.get('subscriptions_complete_at')
        transport = getattr(getattr(state.get('socket'), 'transport', None), 'last', {})
        healthy_pong = (not transport or (transport.get('pongs', 0) > 0
                        and transport.get('pong_age_seconds') is not None
                        and transport['pong_age_seconds'] <= 15))
        if completed is None or state.get('subscribe_pending') or self.pressured(state):
            state['warm_since'] = None
            return False
        if state.get('warm_since') is None:
            state['warm_since'] = max(completed, now)
        warm = ('ROTATE_BUSY_WARM_SECONDS' if state.get('busy') else 'ROTATE_WARM_SECONDS')
        return healthy_pong and now - state['warm_since'] >= setting(warm, 3 if state.get('busy') else 30, 0)
