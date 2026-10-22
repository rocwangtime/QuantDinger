import threading
import time
from queue import Empty

from app.services.polymarket.observer import ReceiveQueue
from tests.test_polymarket_liveness_silence import _wired


def test_close_wakes_idle_consumer_without_enqueuing_or_dropping_frames():
    manager, primary, spare, *_ = _wired()
    inbox = spare['inbox']
    began_wait = threading.Event()
    woke = threading.Event()

    def wait():
        began_wait.set()
        try:
            inbox.next_frame(timeout=10)
        except Empty:
            woke.set()

    worker = threading.Thread(target=wait)
    worker.start()
    assert began_wait.wait(1)
    # Healthy primary notifications have no effect.
    manager.wake_backups(0, primary)
    assert not woke.wait(.02)
    primary['connected'] = False
    started = time.monotonic()
    manager.wake_backups(0, primary)
    assert woke.wait(.1)
    worker.join(1)
    assert time.monotonic()-started < .15
    assert inbox.queue.unfinished_tasks == inbox.dropped == 0
    assert not inbox.resync.is_set()


def test_wake_preserves_fifo_and_task_accounting_even_at_capacity():
    inbox = ReceiveQueue(1, 1)
    assert inbox.put('frame')
    inbox.wake()
    raw, *_ = inbox.next_frame()
    assert raw == 'frame'
    inbox.queue.task_done()
    assert inbox.queue.unfinished_tasks == 0 and inbox.dropped == 0


def test_retired_generation_cannot_wake_or_promote_a_healthy_replacement():
    manager, primary, spare, *_ = _wired()
    retired = dict(primary, connected=False)
    manager.wake_backups(0, retired)
    assert not spare['inbox'].wake_pending and manager.states[0] is primary
