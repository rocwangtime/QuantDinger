import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

from app.services.polymarket.ws_payloads import ParsedFrames, decoded_envelope, loads, cheap_envelope
from app.services.polymarket.ws_warm_book import WarmBook


def test_single_decode_preserves_real_frame_envelopes_and_ladders():
    path = Path(__file__).parent / 'fixtures/polymarket/btc5m_frames_20261020.json.gz'
    frames = json.loads(gzip.decompress(path.read_bytes()))['frames']
    reference, optimized = WarmBook(['ay', 'an']), WarmBook(['ay', 'an'])
    parser = ParsedFrames(cache=False)
    now = datetime.now(timezone.utc)
    for i, template in enumerate(frames):
        raw = (template.replace('__A0__', 'ay').replace('__A1__', 'an')
               .replace('__H', f'{i}x').replace('__TS__', str(1792485000000+i)))
        decoded = loads(raw)
        assert decoded_envelope(decoded) == cheap_envelope(raw)
        reference.receive(parser.parse(raw), now)
        optimized.receive(decoded, now)
        assert reference.books == optimized.books
        assert reference.checkpoints.latest == optimized.checkpoints.latest
