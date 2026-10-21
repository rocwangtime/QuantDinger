import threading
import time
from types import SimpleNamespace
from app.services.polymarket import observer
from app.services.polymarket.ws_group_process import replay_drain, _Feed


def test_drain_waits_for_slow_producer_then_inflight_work():
    inbox = observer.ReceiveQueue(4)
    manager = SimpleNamespace(lifecycle_lock=threading.RLock(), states={0:dict(inbox=inbox)}, backup_items=lambda: [])
    def produce():
        time.sleep(.03)
        inbox.put('frame')
        def consume():
            inbox.queue.get()
            time.sleep(.04)
            inbox.queue.task_done()
        threading.Thread(target=consume).start()
    producer = threading.Thread(target=produce)
    producer.start()
    at = time.monotonic()
    assert replay_drain(manager,producer,.5)
    assert time.monotonic()-at >= .06
    assert inbox.queue.unfinished_tasks == 0


def test_discard_preserves_outstanding_count_for_already_popped_frame():
    inbox = observer.ReceiveQueue(4)
    inbox.put('one'); inbox.put('two')
    inbox.queue.get()
    inbox.discard()
    assert inbox.queue.unfinished_tasks == 1
    inbox.queue.task_done()
    assert inbox.queue.unfinished_tasks == 0


def test_urgent_drain_ack_cannot_finish_before_parent_applies_final_sequence():
    child = _Feed.__new__(_Feed)
    child.drain_token=42
    child.drain_ready=threading.Event()
    child.applied_seq=5
    child.drain_target=None
    child._apply=lambda rows: None
    child._handle(dict(op='drained',token=42,seq=8,complete=True))
    assert not child.drain_ready.is_set()
    child._handle(dict(op='pub',items=[dict(seq=6),dict(seq=8)]))
    assert child.drain_ready.is_set() and child.drain_complete


def test_replay_spare_has_independent_realistic_text_heartbeat():
    from app.services.polymarket.ws_group_process import ReplaySocket
    socket = ReplaySocket('offline', on_open=lambda sock: sock.send('{"assets_ids":["a"],"type":"market"}'))
    socket.heartbeat_phase = .01
    thread = threading.Thread(target=socket.run_forever)
    thread.start()
    deadline = time.monotonic()+.5
    while socket.transport.pongs == 0 and time.monotonic() < deadline:
        time.sleep(.005)
    socket.close(); thread.join(1)
    assert socket.text_heartbeat_active and socket.transport.pings == socket.transport.pongs == 1
