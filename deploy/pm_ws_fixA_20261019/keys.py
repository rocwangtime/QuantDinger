import json, sys, threading, time
from types import SimpleNamespace
from unittest.mock import patch
from app.services.polymarket import observer
def paths(o, pre=''):
    out = set()
    if isinstance(o, dict):
        for k, v in o.items():
            k = str(k)
            if k.isdigit() or pre.endswith('ws_local_100ms') : k = '*'
            out.add(pre + '.' + k); out |= paths(v, pre + '.' + k)
    return out
sockets = []
class Socket:
    def __init__(self, url, **cb): self.cb = cb; self.stopped = threading.Event(); sockets.append(self); self.assets=[]
    def run_forever(self, **kw): self.cb['on_open'](self); self.stopped.wait(30); self.cb['on_close'](self, None, None)
    def send(self, raw):
        if raw != 'PING': self.assets = json.loads(raw).get('assets_ids', [])
    def close(self): self.stopped.set()
with patch.dict('sys.modules', {'websocket': SimpleNamespace(WebSocketApp=Socket)}), \
     patch.object(observer, '_STOP', threading.Event()), \
     patch.object(observer, 'STATUS', dict(observer.STATUS)), \
     patch.dict('os.environ', {'POLYMARKET_OBSERVER_WS_SHARDS': '2', 'POLYMARKET_OBSERVER_WS_HOT_BACKUP_STAGGER_SECONDS': '0'}):
    p = observer.Pipeline(); p.paper_account = None; p.recording.enabled = False
    markets = [dict(id=str(i), yesAssetId=f'{i}y', noAssetId=f'{i}n', market_type='up_down', asset='BTC', interval=iv) for i, iv in enumerate(('5m', '15m'))]
    with patch.object(observer, '_PIPELINE', p):
        m = observer.MarketConnections()
        m.snapshot_recovery.request = lambda *a, **k: False
        m.sync(markets)
        time.sleep(.3)
        for s in list(sockets):
            for a in s.assets:
                s.cb['on_message'](s, json.dumps(dict(event_type='book', asset_id=a, timestamp=int(time.time()*1000), bids=[], asks=[dict(price='.6', size='1')])))
        time.sleep(.3)
        for _ in range(3):
            m.backup_next_connect_at = 0; m.tick(); time.sleep(.2)
        snap = json.loads(json.dumps(observer.snapshot(), default=str))
        m.close()
print(json.dumps(sorted(paths(snap))))
