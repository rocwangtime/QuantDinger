"""Independent, bounded WS ladders and common-watermark handoff checks."""
from collections import OrderedDict
from threading import RLock
from .ws_rotation import shadow_locked

from . import recording
from .ws_rotation import ShadowBooks


class BookCheckpoints:
    """Only book-changing hashes count; trade timestamps cannot prove a ladder."""
    def __init__(self):
        self.lock = RLock()
        self.history = {}
        self.latest = {}

    @shadow_locked
    def receive(self, event):
        for kind, payload in recording.events(event):
            if kind not in ('book', 'price_change'):
                continue
            at = recording.exchange_time(payload.get('timestamp'))
            rows = (payload.get('price_changes', payload.get('priceChanges', []))
                    if kind == 'price_change' else [payload])
            final = {str(row.get('asset_id') or row.get('assetId') or ''): row for row in rows}
            for row in final.values():
                asset = str(row.get('asset_id') or row.get('assetId') or '')
                digest = row.get('hash')
                if not asset or at is None or not isinstance(digest, str) or not 0 < len(digest) <= 128:
                    continue
                mark = (at, digest)
                history = self.history.setdefault(asset, OrderedDict())
                history[mark] = None
                history.move_to_end(mark)
                while len(history) > 2048:
                    history.popitem(last=False)
                if asset not in self.latest or at >= self.latest[asset][0]:
                    self.latest[asset] = mark

    @shadow_locked
    def prune(self, assets):
        self.history = {a: h for a, h in self.history.items() if a in assets}
        self.latest = {a: m for a, m in self.latest.items() if a in assets}


    @shadow_locked
    def charged_bytes(self):
        return 1024 * sum(map(len, self.history.values()))


class WarmBook(ShadowBooks):
    def __init__(self, assets):
        super().__init__(assets)
        self.checkpoints = BookCheckpoints()

    def receive(self, event, received):
        with self.lock:
            for kind, payload in recording.events(event):
                if kind == 'book' and (not isinstance(payload.get('bids'), list)
                        or not isinstance(payload.get('asks'), list)
                        or recording.exchange_time(payload.get('timestamp')) is None):
                    raise ValueError('incomplete warm book snapshot')
            super().receive(event, received)
            self.checkpoints.receive(event)

    def prune(self, assets):
        with self.lock:
            super().prune(assets)
            self.checkpoints.prune(set(assets))

    def baseline(self, asset, primary, live_book, live_at):
        """Return a checked book event, 'retained', or None (needs REST).

        If behind, keep the merged primary ladder and let FeedMerge fence
        replayed deltas. If ahead, a common primary watermark proves overlap.
        No checkpoint, snapshot, timestamp or intact generation => no shortcut.
        """
        if primary is None:
            return None
        with self.lock, primary.lock:
            book = self.books.get(asset)
            mark = self.checkpoints.latest.get(asset)
            peer = primary.latest.get(asset) if primary else None
            at = self.recorder.metadata.get(asset, (None,))[0]
            if book is None or live_book is None or not mark or not peer or at is None or live_at is None:
                return None
            # Hash must describe the current ladder, not a previous message.
            if at != mark[0] or live_at != peer[0]:
                return None
            if at == live_at:
                return 'retained' if mark == peer and book == live_book else None
            if at < live_at:
                return 'retained' if mark in primary.history.get(asset, {}) else None
            if peer not in self.checkpoints.history.get(asset, {}):
                return None
            return dict(event_type='book', asset_id=asset, timestamp=round(at.timestamp() * 1000),
                        hash=mark[1], bids=[dict(price=p, size=q) for p, q in book['bids'].items()],
                        asks=[dict(price=p, size=q) for p, q in book['asks'].items()])

    def handoff_watermarks(self, merge, assets):
        """An assembled ladder is not a new exchange snapshot of every level.

        Keep the original snapshot floor plus each level's actual timestamp,
        so later out-of-order deltas for untouched levels remain applicable.
        """
        with self.lock:
            for asset in assets:
                merge.snapshots[asset] = self.merge.snapshots[asset]
                for key in list(merge.levels):
                    if key[0] == asset:
                        del merge.levels[key]
                for key, at in self.merge.levels.items():
                    if key[0] == asset:
                        merge.levels[key] = at
