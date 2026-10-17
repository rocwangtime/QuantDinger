"""Nonblocking snapshot admission, shared by active and warming public sockets."""
import json
import threading
from datetime import datetime, timezone

from .ws_balance import setting


class SubscriptionPacer:
    def __init__(self):
        self.lock = threading.Lock()
        self.next_at = 0

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

    @staticmethod
    def pressured(state):
        inbox = state.get('inbox')
        transport = getattr(getattr(state.get('socket'), 'transport', None), 'last', {})
        return bool(inbox and (inbox.invalid.is_set() or inbox.resync.is_set() or inbox.delivery_gap
                    or inbox.queue.qsize() > 32 or inbox.queue.qsize() and inbox.lag_seconds > .25)
                    or (transport.get('tcp_pending_bytes') or 0) >= 65536
                    or (transport.get('scheduler_lag_seconds') or 0) > .1)

    def drain(self, state, now):
        # No sleep on on_open, the reader or supervisor. A shared clock admits
        # at most one small batch, including cold starts on different sockets.
        with state['lock']:
            pending = state.get('subscribe_pending', [])
            if (not state.get('connected') or state.get('retired') or not pending
                    or now < state.get('subscribe_pause_until', 0) or self.pressured(state)):
                return
            with self.lock:
                if now < self.next_at:
                    return
                batch = pending[:int(setting('SUBSCRIBE_BATCH_ASSETS', 2, 1))]
                self.next_at = now + max(setting('SUBSCRIBE_BATCH_SECONDS', .5, .05),
                                        len(batch) / setting('SUBSCRIBE_ASSETS_PER_S', 4, .1))
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
            del pending[:len(batch)]
            state['subscribe_initial'] = False
            state['subscribe_batches'] = state.get('subscribe_batches', 0) + 1
            if not pending:
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
        return healthy_pong and now - state['warm_since'] >= setting('ROTATE_WARM_SECONDS', 30, 0)
