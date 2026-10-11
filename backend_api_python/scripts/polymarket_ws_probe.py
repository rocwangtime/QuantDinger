"""Public observer smoke/load probe in a disposable localhost-only PG schema."""
import argparse
import json
import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('load', 'live'))
    parser.add_argument('--seconds', type=int, default=150)
    args = parser.parse_args()
    dsn = os.environ.get('QD_TEST_POSTGRES_DSN', '')
    if dsn != 'postgresql://qd_test:qd_test@127.0.0.1:5432/qd_automation_test':
        parser.error('requires the explicitly approved disposable localhost DSN')
    for key in ('DATABASE_URL', 'AUTOMATION_TEST_DATABASE_URL'):
        os.environ[key] = dsn
    os.environ.update(POLYMARKET_OBSERVER_ENABLED='true', POLYMARKET_OBSERVER_UPDOWN_ENABLED='true',
                      POLYMARKET_OBSERVER_MARKET_COUNT='20', POLYMARKET_OBSERVER_NEAR_MISS_BPS='10000',
                      POLYMARKET_OBSERVER_WRITE_QUEUE_MAX='128')
    from app.services.polymarket import observer
    from app.utils.db import get_db_connection

    schema = 'ws_probe_' + uuid4().hex
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(f'CREATE SCHEMA {schema}')
        cur.execute(f'SET LOCAL search_path TO {schema}')
        for name in ('20261011_polymarket_observer.sql', '20261011_polymarket_updown.sql'):
            cur.execute((ROOT / 'migrations' / name).read_text())
        db.commit()
        cur.close()

    @contextmanager
    def isolated_connection():
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute(f'SET search_path TO {schema},public')
            cur.close()
            try:
                yield db
            finally:
                db.rollback()
                cur = db.cursor()
                cur.execute('SET search_path TO public')
                db.commit()
                cur.close()

    observer.get_db_connection = isolated_connection
    pipeline = None
    try:
        if args.mode == 'live':
            began, cpu = time.monotonic(), time.process_time()
            observer.start()
            for _ in range(args.seconds // 10):
                time.sleep(10)
                print(json.dumps(observer.snapshot()), flush=True)
            observer.stop(20)
            print(json.dumps(dict(result=observer.snapshot(), wall_seconds=time.monotonic()-began,
                                  cpu_seconds=time.process_time()-cpu)), flush=True)
        else:
            markets = [dict(id=f'load-{i}', slug=f'load-{i}', question='synthetic', feeRate='0',
                            yesAssetId=f'{i}y', noAssetId=f'{i}n', market_type='up_down', asset='BTC',
                            interval='15m', endDate=(datetime.now(timezone.utc)+timedelta(minutes=15)).isoformat())
                       for i in range(50)]
            pipeline = observer.Pipeline()
            pipeline.set_markets(markets)
            for m in markets:
                for token in observer.subscription_assets([m]):
                    pipeline.receive(json.dumps(dict(event_type='book', asset_id=token, bids=[dict(price='.5', size='100')], asks=[dict(price='.55', size='100')])))
            raws = [json.dumps(dict(event_type='price_change', price_changes=[dict(asset_id=f'{i}y', side='SELL', price='.55', size='100')])) for i in range(50)]
            began, cpu = time.monotonic(), time.process_time()
            for i in range(100000):
                pipeline.receive(raws[i % 50])
            elapsed, used = time.monotonic()-began, time.process_time()-cpu
            print(json.dumps(dict(receive_benchmark_messages=100000, messages_per_second=100000/elapsed,
                                  wall_seconds=elapsed, cpu_seconds=used)), flush=True)
            pipeline.start()
            began, cpu, sent = time.monotonic(), time.process_time(), 0
            while time.monotonic()-began < 10:
                for i in range(100):
                    pipeline.receive(raws[i % 50])
                sent += 100
                target = began + sent / 2500
                time.sleep(max(0, target - time.monotonic()))
            elapsed, used = time.monotonic()-began, time.process_time()-cpu
            pipeline.close(20)
            print(json.dumps(dict(sustained_messages=sent, messages_per_second=sent/elapsed,
                                  wall_seconds=elapsed, cpu_seconds=used, metrics=pipeline.metrics())), flush=True)
        with isolated_connection() as db:
            cur = db.cursor()
            for table in ('episodes', 'near_misses', 'samples'):
                cur.execute(f'SELECT count(*) count FROM qd_polymarket_observer_{table}')
                print(table, cur.fetchone(), flush=True)
            cur.close()
    finally:
        if pipeline:
            pipeline.close(20)
        if args.mode == 'live':
            observer.stop(20)
        with get_db_connection() as db:
            cur = db.cursor()
            cur.execute(f'DROP SCHEMA {schema} CASCADE')
            db.commit()
            cur.close()
        print('Disposable schema and all probe rows removed.', flush=True)


if __name__ == '__main__':
    main()
