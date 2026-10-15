"""Research screener math, safety, scheduling and PostgreSQL contracts."""
import ast
from datetime import date, datetime, timezone
from pathlib import Path
import math
import os
import subprocess
import sys
from unittest.mock import patch, MagicMock

import pytest
from app.services.us_screener import core, store
from app.services.us_screener.service import parameters, execute

ROOT = Path(__file__).resolve().parents[1]
DSN = 'postgresql://qd_test:qd_test@127.0.0.1:5432/qd_automation_test'


def bars(n=220, growth=1.001):
    return [{'close': 100*growth**i, 'high': 101*growth**i, 'low': 99*growth**i, 'volume': 1_000_000} for i in range(n)]


def test_features():
    f = core.features(bars())
    assert f['momentum20'] == pytest.approx(1.001**20-1)
    assert f['momentum60'] == pytest.approx(1.001**60-1)
    assert f['volatility20'] == pytest.approx(0, abs=1e-12)
    expected = sum(2*1.001**i for i in range(206,220))/14/(1.001**219)
    assert f['atr14_pct'] == pytest.approx(expected)
    assert f['distance_ma50'] > 0
    assert f['avg_dollar_volume20'] > 100_000_000
    with pytest.raises(ValueError):
        core.features(bars(60))
    invalid = bars(); invalid[-1]['close'] = math.nan
    with pytest.raises(ValueError):
        core.features(invalid)


def test_regime():
    assert core.regime(bars())['flag'] == 'risk_on'
    assert core.regime(bars(growth=.999))['flag'] == 'risk_off'
    assert core.regime(bars(100))['flag'] == 'unknown'


@pytest.mark.parametrize('stamp,expected', [
    ('2026-03-06T21:29:00+00:00', None), ('2026-03-06T21:30:00+00:00', date(2026,3,6)),
    ('2026-03-09T20:29:00+00:00', None), ('2026-03-09T20:30:00+00:00', date(2026,3,9)),
    ('2026-07-03T21:30:00+00:00', None), ('2026-03-08T21:30:00+00:00', None),
    ('2026-11-02T21:30:00+00:00', date(2026,11,2)),
])
def test_schedule(stamp, expected):
    assert core.scheduled_day(datetime.fromisoformat(stamp)) == expected


def test_earnings_sessions_and_unknown():
    # July 3 holiday and weekend do not count as sessions.
    assert core.earnings_reasons([date(2026,7,8)], date(2026,7,2))[0] == ['earnings_blackout']
    assert core.earnings_reasons([date(2026,7,9)], date(2026,7,2))[0] == []
    assert core.earnings_reasons([], date(2026,7,2)) == ([], ['earnings_unknown'])
    assert core.earnings_reasons([], date(2026,7,2), policy='closed')[0] == ['earnings_unknown']
    assert core.earnings_reasons([date(2026,6,2)], date(2026,7,2))[1] == ['earnings_unknown']


def test_filters():
    now = datetime(2026,10,14,15,59,tzinfo=core.ZoneInfo('America/New_York'))
    p = parameters()
    quote = {'last_price': 100, 'bid_price': 99.99, 'ask_price':100.01, 'update_time': now.replace(tzinfo=None).isoformat()}
    r = core.evaluate('A', bars(), quote, [], now.date(), p)
    assert not r['exclusion_reasons'] and r['flags'] == ['earnings_unknown']
    assert 'spread_too_wide' in core.evaluate('A', bars(), {**quote, 'ask_price':101}, [], now.date(), p)['exclusion_reasons']
    cheap = bars()
    cheap[-1]['close'] = 4
    assert 'price_below_min' in core.evaluate('A', cheap, quote, [], now.date(), p)['exclusion_reasons']
    thin = bars()
    for b in thin:
        b['volume'] = 1
    assert 'illiquid' in core.evaluate('A', thin, quote, [], now.date(), p)['exclusion_reasons']
    for q in ({**quote,'bid_price':None}, {**quote,'update_time':'2000-01-01 16:00:00'}):
        assert core.evaluate('A', bars(), q, [], now.date(), p)['features']['spread_source'] == 'corwin_schultz_daily'


def test_scoring_and_tie_break():
    rows = [dict(symbol=s, features={'momentum20':m,'momentum60':m,'volatility20':.2}, score=None,rank=None,included=False,exclusion_reasons=[]) for s,m in [('B',1),('A',1),('C',0)]]
    core.rank(rows,1)
    assert rows[1]['rank'] == 1 and rows[1]['included']
    assert rows[0]['score'] == rows[1]['score'] > rows[2]['score']
    assert rows[0]['exclusion_reasons'] == ['below_top_n']


def test_no_execution_dependencies():
    paths = list((ROOT/'app/services/us_screener').glob('*.py')) + [ROOT/'app/tasks/us_screener.py', ROOT/'app/commands/us_screener.py', ROOT/'app/routes/us_screener.py']
    for path in paths:
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                names = [a.name for a in node.names] + ([node.module] if isinstance(node, ast.ImportFrom) else [])
                if isinstance(node, ast.ImportFrom) and node.module == 'app.services.futu_trading.config':
                    continue
                assert not any(any(x in (name or '') for x in ['trading','automation','agent_trade','order','execution']) for name in names)
    code = "import sys; from app.services.us_screener.service import parameters; from app.services.us_screener.data import FutuData; from app.services.universe import UniverseService; from app.services.fundamental_data import screener_earnings_dates; parameters(); assert not any(n.startswith(('app.services.automation','app.services.futu_trading','app.services.agent_trade_intents','app.services.live_trading')) for n in sys.modules)"
    subprocess.run([sys.executable,'-c',code],cwd=ROOT,check=True)


def test_migration_registration():
    source = (ROOT/'app/utils/db.py').read_text()
    for name in ('20261015_us_screener','20261015_nasdaq100_seed','20261015_nasdaq100_seed_effective_date'):
        assert f'name="{name}"' in source
        assert f'"{name}.sql"' in source


@pytest.fixture
def database():
    if os.getenv('QD_TEST_POSTGRES_DSN') != DSN:
        pytest.skip('requires explicit local disposable PG')
    import psycopg2
    connection = psycopg2.connect(DSN)
    with connection.cursor() as cur:
        cur.execute((ROOT/'migrations/20261015_us_screener.sql').read_text())
        cur.execute((ROOT/'migrations/20261015_us_screener_quality.sql').read_text())
        cur.execute((ROOT/'migrations/20261015_us_screener_rth.sql').read_text())
    connection.commit()
    yield connection
    connection.close()


def test_persistence_and_partial_failure(database):
    with patch.dict(os.environ, DATABASE_URL=DSN):
        run = store.create(date(2026,10,14), parameters(), 'test:'+str(datetime.now(timezone.utc)))
        rows = [dict(symbol='AAA',features={'momentum20':.1},score=1,rank=1,included=True,exclusion_reasons=[],flags=['earnings_unknown'])]
        store.finish(run['id'],rows,{'flag':'risk_on'},'completed')
        result = store.read(run['id'])
        assert result['symbols'][0]['features']['momentum20'] == .1
        assert result['counts']['included'] == 1
        assert store.create(date(2026,10,14),parameters(), result['run_key']) is None
        store.query('DELETE FROM qd_us_screener_runs WHERE id=%s',(run['id'],))


def test_execute_records_every_symbol(database):
    class Provider:
        def __init__(self, deadline): pass
        def quotes(self, symbols, as_of=None): return {}, {'BAD':'quote_batch_failed'}
        def bars(self, symbol, day): return bars()
        def close(self): pass
    with patch.dict(os.environ,DATABASE_URL=DSN):
        run=store.create(date(2026,10,14),parameters(),'test:'+str(datetime.now(timezone.utc)))
        with patch('app.services.universe.UniverseService.resolve_members',return_value=[{'symbol':'BAD'},{'symbol':'MISSING'}]), patch('app.services.fundamental_data.screener_earnings_dates', return_value=[]):
            execute(run['id'],Provider)
        result=store.read(run['id'])
        assert result['status']=='partial'
        assert len(result['symbols'])==2
        assert next(r for r in result['symbols'] if r['symbol'] == 'MISSING')['included']
        assert 'spread_unverified' in next(r for r in result['symbols'] if r['symbol'] == 'MISSING')['flags']
        store.query('DELETE FROM qd_us_screener_runs WHERE id=%s',(run['id'],))


def test_futu_batching_cache_and_read_only_allowlist():
    import pandas as pd
    from app.services.us_screener.data import FutuData
    client = FutuData.__new__(FutuData)
    calls = []
    def call(method, codes):
        calls.append((method, codes))
        return 0, pd.DataFrame([{'code':c,'last_price':100,'bid_price':99.99,'ask_price':100.01,'update_time':'2026-10-14 16:30:00'} for c in codes])
    client.call = call
    with patch.object(store,'query',return_value=[]):
        quotes, errors = client.quotes([f'A{i}' for i in range(103)])
    assert len(quotes)==103 and not errors
    assert [len(c[1]) for c in calls]==[50,50,3]
    assert all(c[0]=='get_market_snapshot' for c in calls)
    with patch.object(store,'query',return_value={'bars':bars()}):
        assert len(client.bars('AAA',date(2026,10,14)))==220
    actual=FutuData.__new__(FutuData)
    with pytest.raises(ValueError,match='read_only_api_required'):
        actual.call('place_order')
    actual.last_call=10
    actual.deadline=100
    actual.context=type('Context',(),{'get_market_snapshot':lambda self,*a:(0,[])})()
    with patch('app.services.us_screener.data.time.monotonic',return_value=10), patch('app.services.us_screener.data.time.sleep') as sleep:
        actual.call('get_market_snapshot',[])
        sleep.assert_called_once_with(2.1)
    actual.deadline=11
    with patch('app.services.us_screener.data.time.monotonic',return_value=10):
        with pytest.raises(TimeoutError):
            actual.call('get_market_snapshot',[])


def test_api_read_and_admin_trigger(client):
    headers={'Authorization':'Bearer test'}
    with patch('app.utils.auth.verify_token',return_value={'user_id':1,'_verified_user_role':'viewer'}), patch.object(store,'read',return_value={'as_of':date(2026,10,14),'created_at':datetime(2026,10,14,tzinfo=timezone.utc),'symbols':[]}), patch('app.services.us_screener.service.enqueue') as enqueue:
        assert client.get('/api/us-screener/latest').status_code==401
        response=client.get('/api/us-screener/latest',headers=headers)
        assert response.status_code==200
        assert response.json['data']['as_of']=='2026-10-14'
        assert client.post('/api/us-screener/runs',headers=headers).status_code==403
        enqueue.assert_not_called()
    with patch('app.utils.auth.verify_token',return_value={'user_id':1,'_verified_user_role':'admin'}), patch('app.services.us_screener.service.enqueue',return_value={'id':1}) as enqueue:
        assert client.post('/api/us-screener/runs',headers=headers).status_code==202
        enqueue.assert_called_once_with()


def test_latest_closed_day():
    from app.services.us_screener.service import latest_closed_day
    assert latest_closed_day(datetime.fromisoformat('2026-10-14T19:00:00+00:00'))==date(2026,10,13)
    assert latest_closed_day(datetime.fromisoformat('2026-10-14T20:30:00+00:00'))==date(2026,10,14)


def test_seed_resolution_idempotency(database):
    # Test the real resolver without altering any task rows or populated universe.
    from app.services.universe import UniverseService
    with patch.dict(os.environ,DATABASE_URL=DSN):
        universe=store.query("SELECT id FROM qd_universes WHERE code='nasdaq100' AND is_system=TRUE",one=True)
        assert universe
        with database.cursor() as cur:
            for _ in range(2):
                cur.execute((ROOT/'migrations/20261015_nasdaq100_seed.sql').read_text())
        database.commit()
        members=UniverseService().resolve_members(0,universe['id'],as_of='2026-10-14')
        assert 95<=len(members)<=105
        assert len({m['symbol'] for m in members})==len(members)


def test_earnings_cached_metadata_no_provider_call(database):
    from app.services.fundamental_data import screener_earnings_dates
    import json
    with database.cursor() as cur:
        cur.execute("INSERT INTO qd_fundamental_snapshots(market,symbol,period_end,available_at,frequency,source,metadata_json,ingested_at) VALUES ('USStock','SCREEN_TEST',DATE '2026-06-30',DATE '2026-07-30','quarterly','manual',%s,TIMESTAMPTZ '2026-10-14 12:00:00+00:00') RETURNING id",
                    (json.dumps({'earnings_dates':['2026-10-16','invalid']}),))
        snapshot_id=cur.fetchone()[0]
    database.commit()
    try:
        with patch.dict(os.environ,DATABASE_URL=DSN):
            assert screener_earnings_dates('SCREEN_TEST',date(2026,10,14))==[date(2026,10,16)]
            assert screener_earnings_dates('SCREEN_TEST',date(2026,10,23))==[]
    finally:
        with database.cursor() as cur:
            cur.execute('DELETE FROM qd_fundamental_snapshots WHERE id=%s',(snapshot_id,))
        database.commit()


def test_seed_empty_path_and_no_backfill(database):
    with database.cursor() as cur:
        cur.execute("SELECT id FROM qd_universes WHERE code='nasdaq100' AND is_system=TRUE")
        universe_id=cur.fetchone()[0]
        cur.execute('DELETE FROM qd_universe_members WHERE universe_id=%s',(universe_id,))
        for _ in range(2):
            cur.execute((ROOT/'migrations/20261015_nasdaq100_seed.sql').read_text())
        cur.execute('SELECT COUNT(*),MIN(valid_from),COUNT(DISTINCT symbol) FROM qd_universe_members WHERE universe_id=%s',(universe_id,))
        assert cur.fetchone()==(100,date(2026,10,14),100)
        cur.execute('SELECT metadata_json FROM qd_universes WHERE id=%s',(universe_id,))
        assert cur.fetchone()[0]['verification']=='flagged_incomplete_public_list'
    database.rollback()


@pytest.mark.parametrize('host,port,expected', [
    ('host.docker.internal', '11112', ('host.docker.internal', 11112)),
    ('', '', ('127.0.0.1', 11111)),
])
def test_futu_connection_config(host, port, expected):
    from app.services.us_screener.data import FutuData
    context = MagicMock()
    from types import SimpleNamespace
    with patch.dict(os.environ, FUTU_OPEND_HOST=host, FUTU_OPEND_PORT=port), \
         patch.dict(sys.modules, futu=SimpleNamespace(OpenQuoteContext=context)):
        client = FutuData(100)
        context.assert_called_once_with(host=expected[0], port=expected[1])
        context.return_value.set_sync_query_connect_timeout.assert_called_once_with(10)
        client.close()


def test_execute_as_of_precedes_members(database):
    run = store.create(date(2026,10,13), {**parameters(), 'manual_fallback': []},
                       'test:'+str(datetime.now(timezone.utc)))
    assert run
    try:
        universe = store.query("SELECT id,metadata_json FROM qd_universes WHERE code='nasdaq100' AND is_system=TRUE", one=True)
        queued_run = store.read(run['id'])
        with patch.object(store, 'read', return_value=queued_run), \
             patch('app.services.universe.UniverseService.resolve_members', return_value=[]), \
             patch.object(store, 'query', side_effect=[{'id': run['id']}, universe, {'first_valid_from': date(2026,10,14)}]), \
             patch('app.services.us_screener.data.FutuData') as provider:
            execute(run['id'])
        result = store.read(run['id'])
        assert result['status'] == 'failed'
        assert result['error'] == 'as_of_precedes_universe_members'
        assert result['regime']['universe_first_valid_from'] == '2026-10-14'
        provider.assert_not_called()
    finally:
        store.query('DELETE FROM qd_us_screener_runs WHERE id=%s', (run['id'],))


def test_seed_effective_date_correction(database):
    with database.cursor() as cur:
        cur.execute("SELECT id FROM qd_universes WHERE code='nasdaq100' AND is_system=TRUE")
        universe_id = cur.fetchone()[0]
        cur.execute('DELETE FROM qd_universe_members WHERE universe_id=%s', (universe_id,))
        cur.execute((ROOT/'migrations/20261015_nasdaq100_seed.sql').read_text())
        cur.execute('SELECT symbol,metadata_json FROM qd_universe_members WHERE universe_id=%s ORDER BY symbol', (universe_id,))
        metadata = cur.fetchall()
        cur.execute("UPDATE qd_universe_members SET source_version='other-provider' WHERE universe_id=%s AND symbol='AAPL'", (universe_id,))
        for _ in range(2):
            cur.execute((ROOT/'migrations/20261015_nasdaq100_seed_effective_date.sql').read_text())
        cur.execute('SELECT valid_from,COUNT(*) FROM qd_universe_members WHERE universe_id=%s GROUP BY valid_from ORDER BY valid_from', (universe_id,))
        assert cur.fetchall() == [(date(2026,10,9),99), (date(2026,10,14),1)]
        cur.execute('SELECT symbol,metadata_json FROM qd_universe_members WHERE universe_id=%s ORDER BY symbol', (universe_id,))
        assert cur.fetchall() == metadata
        cur.execute('SELECT COUNT(*) FROM qd_universe_members WHERE universe_id=%s AND valid_from<=%s', (universe_id,date(2026,10,13)))
        assert cur.fetchone()[0] == 99
    database.rollback()


def test_regular_spread_ignores_extended_and_early_close():
    quote = {'bid_price': 99, 'ask_price': 101, 'update_time': '2026-11-27 14:00:00'}
    narrow = [{**b, 'high': b['close']*1.00001, 'low': b['close']*.99999} for b in bars()]
    spread, source = core.regular_spread(narrow, quote, date(2026,11,27))
    assert source == 'corwin_schultz_daily' and spread < 10
    quote['update_time'] = '2026-11-27 12:59:00'
    assert core.regular_spread(narrow, quote, date(2026,11,27)) == (200, 'rth_snapshot')
    quote['update_time'] = '2026-11-26 12:59:00'
    assert core.regular_spread(narrow, quote, date(2026,11,27))[1] == 'corwin_schultz_daily'


def test_unknown_exclude_policy():
    with patch.dict(os.environ, US_SCREENER_EARNINGS_UNKNOWN='exclude'):
        row = core.evaluate('A', bars(), {}, [], date(2026,10,14), parameters())
    assert row['features']['earnings_status'] == 'unknown'
    assert 'earnings_unknown' in row['flags'] and 'earnings_unknown' in row['exclusion_reasons']


@pytest.mark.parametrize('failed,threshold,status', [(1,.9,'completed'), (2,.9,'partial'), (1,1,'partial')])
def test_coverage_threshold(database, failed, threshold, status):
    symbols = ['T'+str(i) for i in range(10)]
    class Provider:
        def __init__(self, deadline): pass
        def quotes(self, symbols, as_of=None): return {}, {s:'quote_batch_failed' for s in symbols[:failed]}
        def bars(self, symbol, day): return bars()
        def close(self): pass
    run = store.create(date(2026,10,14), {**parameters(), 'min_coverage':threshold}, 'coverage:'+str(datetime.now(timezone.utc)))
    try:
        with patch('app.services.universe.UniverseService.resolve_members', return_value=[{'symbol':s} for s in symbols]), patch('app.services.fundamental_data.screener_earnings_dates', return_value=[]):
            execute(run['id'], Provider)
        result = store.read(run['id'])
        assert result['status'] == status
        assert result['counts']['failed'] == failed
        assert result['counts']['coverage_ratio'] == (10-failed)/10
        assert all(not r['included'] for r in result['symbols'] if 'market_data_failed' in r['exclusion_reasons'])
    finally:
        store.query('DELETE FROM qd_us_screener_runs WHERE id=%s', (run['id'],))


@pytest.mark.parametrize('text', ['expected* to report earnings on  11/02/2026', 'estimated to report earnings on  11/02/2026'])
def test_nasdaq_earnings_fetch_and_cache(database, text):
    from app.services.fundamental_data import screener_earnings_dates
    today = datetime.now(timezone.utc).date()
    store.query("DELETE FROM qd_us_screener_earnings_cache WHERE symbol='NASDAQ_TEST'")
    response = MagicMock()
    response.json.return_value = {'data':{'reportText':text}}
    try:
        with patch('requests.get',return_value=response) as get, patch('time.sleep'):
            assert screener_earnings_dates('NASDAQ_TEST',today) == [date(2026,11,2)]
            assert screener_earnings_dates('NASDAQ_TEST',today) == [date(2026,11,2)]
            assert get.call_count == 1
        row = store.query("SELECT * FROM qd_us_screener_earnings_cache WHERE symbol='NASDAQ_TEST'", one=True)
        assert row['fetched_at'] and row['source'] == 'nasdaq_expected_earnings'
    finally:
        store.query("DELETE FROM qd_us_screener_earnings_cache WHERE symbol='NASDAQ_TEST'")


@pytest.mark.parametrize('value', ['0', '1.1', 'nan', '-0.1'])
def test_invalid_coverage(value):
    with patch.dict(os.environ, US_SCREENER_MIN_COVERAGE=value):
        with pytest.raises(ValueError, match='invalid_min_coverage'):
            parameters()


def test_nasdaq_failure_is_cached_unknown(database):
    import requests
    from app.services.fundamental_data import screener_earnings_dates
    today = datetime.now(timezone.utc).date()
    store.query("DELETE FROM qd_us_screener_earnings_cache WHERE symbol='NASDAQ_FAILURE'")
    try:
        with patch('requests.get', side_effect=requests.HTTPError('429')) as get, patch('time.sleep'):
            assert screener_earnings_dates('NASDAQ_FAILURE', today) == []
            assert screener_earnings_dates('NASDAQ_FAILURE', today) == []
            assert get.call_count == 1
    finally:
        store.query("DELETE FROM qd_us_screener_earnings_cache WHERE symbol='NASDAQ_FAILURE'")


@pytest.mark.parametrize('stamp,expected', [
    ('2026-10-14T19:29:00+00:00', None),
    ('2026-10-14T19:30:00+00:00', date(2026,10,14)),
    ('2026-10-14T19:58:00+00:00', None),
    ('2026-11-27T17:30:00+00:00', date(2026,11,27)),
    ('2026-11-27T17:58:00+00:00', None),
    ('2026-11-26T17:30:00+00:00', None),
    ('2026-03-06T20:30:00+00:00', date(2026,3,6)),
    ('2026-03-09T19:30:00+00:00', date(2026,3,9)),
])
def test_rth_capture_window(stamp, expected):
    assert core.capture_session(datetime.fromisoformat(stamp)) == expected


def test_unverified_spread_policy():
    row = core.evaluate('A', bars(), {}, [], date(2026,10,14), parameters())
    assert row['features']['spread_estimate_bps'] > 10
    assert row['features']['spread_bps'] is None
    assert 'spread_unverified' in row['flags']
    assert not row['exclusion_reasons']
    with patch.dict(os.environ, US_SCREENER_UNVERIFIED_SPREAD='exclude'):
        row = core.evaluate('A', bars(), {}, [], date(2026,10,14), parameters())
    assert row['exclusion_reasons'] == ['spread_unverified']
    with patch.dict(os.environ, US_SCREENER_UNVERIFIED_SPREAD='invalid'):
        with pytest.raises(ValueError):
            parameters()


def test_capture_persistence_once_and_session_isolation(database):
    from app.services.us_screener.capture import capture
    from app.services.us_screener.data import FutuData
    now = datetime.fromisoformat('2026-10-14T19:31:00+00:00')
    quote = {'bid_price':'99.99','ask_price':'100.01','update_time':'2026-10-14 15:30:30'}
    factory = MagicMock()
    factory.return_value.quotes.return_value = ({'RTH_TEST':quote}, {})
    store.query('DELETE FROM qd_us_screener_quote_captures WHERE session_date=%s', (now.date(),))
    try:
        with patch.dict(os.environ, US_SCREENER_RTH_CAPTURE_ENABLED='true'), patch('app.services.universe.UniverseService.resolve_members', return_value=[{'symbol':'RTH_TEST'}]):
            assert capture(now, provider_factory=factory)['captured'] == 1
            assert capture(now, provider_factory=factory)['reason'] == 'already_captured'
        factory.assert_called_once()
        factory.return_value.close.assert_called_once()
        client = FutuData.__new__(FutuData)
        assert client.quotes(['RTH_TEST'], as_of=now.date()) == ({'RTH_TEST':quote}, {})
        assert client.quotes(['RTH_TEST'], as_of=date(2026,10,13)) == ({}, {})
    finally:
        store.query("DELETE FROM qd_us_screener_quote_cache WHERE symbol='RTH_TEST'")
        store.query('DELETE FROM qd_us_screener_quote_captures WHERE session_date=%s', (now.date(),))


def test_no_eligible_is_partial(database):
    class Provider:
        def __init__(self, deadline): pass
        def quotes(self, symbols, as_of=None): return {}, {}
        def bars(self, symbol, day): return bars()
        def close(self): pass
    run = store.create(date(2026,10,14), {**parameters(), 'unverified_spread':'exclude'}, 'empty:'+str(datetime.now(timezone.utc)))
    try:
        with patch('app.services.universe.UniverseService.resolve_members', return_value=[{'symbol':'A'}]), patch('app.services.fundamental_data.screener_earnings_dates', return_value=[]):
            execute(run['id'], Provider)
        result = store.read(run['id'])
        assert result['status'] == 'partial'
        assert result['error'] == result['regime']['reason'] == 'no_eligible_symbols'
        assert result['counts']['included'] == 0
    finally:
        store.query('DELETE FROM qd_us_screener_runs WHERE id=%s', (run['id'],))


def test_capture_disabled_and_outside_session():
    from app.services.us_screener.capture import capture
    with patch.dict(os.environ, US_SCREENER_ENABLED='false', US_SCREENER_RTH_CAPTURE_ENABLED='false'), patch.object(store, 'query') as query:
        assert capture()['reason'] == 'capture_disabled'
        query.assert_not_called()
    with patch.dict(os.environ, US_SCREENER_RTH_CAPTURE_ENABLED='true'), patch.object(store, 'query') as query:
        assert capture(datetime.fromisoformat('2026-10-14T21:00:00+00:00'), scheduled=False)['reason'] == 'outside_capture_window'
        query.assert_not_called()


def test_rth_migration_repairs_legacy_empty_completion(database):
    run = store.create(date(2026,10,14), parameters(), 'legacy:'+str(datetime.now(timezone.utc)))
    try:
        store.finish(run['id'], [], {}, 'completed')
        with database.cursor() as cur:
            for _ in range(2):
                cur.execute((ROOT/'migrations/20261015_us_screener_rth.sql').read_text())
        database.commit()
        result = store.read(run['id'])
        assert result['status'] == 'partial'
        assert result['error'] == 'no_eligible_symbols'
    finally:
        store.query('DELETE FROM qd_us_screener_runs WHERE id=%s', (run['id'],))
