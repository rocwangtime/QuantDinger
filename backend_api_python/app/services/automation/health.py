"""Process-local Agent loop diagnostics published by the scheduler heartbeat."""
import threading
import time

_lock = threading.Lock()
_state = {'status': 'stopped'}
_generation = 0


def started():
    global _generation
    with _lock:
        _generation += 1
        _state.clear()
        _state.update(status='running', started_at=time.time(), last_tick_at=None, last_success_at=None)


def tick(success):
    with _lock:
        _state['last_tick_at'] = time.time()
        if success:
            _state['last_success_at'] = _state['last_tick_at']
        _state['last_tick_ok'] = bool(success)


def stopped():
    global _generation
    with _lock:
        _generation += 1
        _state['status'] = 'stopped'


def snapshot(now=None):
    now = time.time() if now is None else now
    with _lock:
        state = dict(_state)
    if state['status'] == 'running':
        last = state.get('last_tick_at') or state.get('started_at', 0)
        if not 0 <= now-last <= 15:
            state['status'] = 'stalled'
        elif state.get('last_tick_ok') is False:
            state['status'] = 'degraded'
    return state


def generation():
    with _lock:
        return _generation
