"""Screener-only persistence; never writes automation or order tables."""
import json
from app.utils.db import get_db_connection


def query(sql, args=(), one=False):
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute(sql, args)
        result = (cur.fetchone() if one else cur.fetchall()) if sql.lstrip().upper().startswith('SELECT') or 'RETURNING' in sql.upper() else None
        db.commit()
        cur.close()
        return result


def create(as_of, params, key):
    # Serialize admission across API/CLI/beat workers, without agent leases.
    with get_db_connection() as db:
        cur = db.cursor()
        cur.execute('SELECT pg_advisory_xact_lock(20261015, 100)')
        cur.execute("SELECT id FROM qd_us_screener_runs WHERE status IN ('queued','running') AND created_at > NOW()-INTERVAL '15 minutes' LIMIT 1")
        if cur.fetchone():
            cur.close()
            db.commit()
            return None
        cur.execute("INSERT INTO qd_us_screener_runs(as_of,params,run_key,status) VALUES (%s,%s,%s,'queued') ON CONFLICT(run_key) DO NOTHING RETURNING id",
                    (as_of, json.dumps(params), key))
        result = cur.fetchone()
        db.commit()
        cur.close()
        return result


def finish(run_id, rows, market_regime, status, error=''):
    with get_db_connection() as db:
        cur = db.cursor()
        for r in rows:
            cur.execute('INSERT INTO qd_us_screener_symbols(run_id,symbol,features,score,rank,included,exclusion_reasons,flags) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(run_id,symbol) DO UPDATE SET features=EXCLUDED.features,score=EXCLUDED.score,rank=EXCLUDED.rank,included=EXCLUDED.included,exclusion_reasons=EXCLUDED.exclusion_reasons,flags=EXCLUDED.flags',
                        (run_id, r['symbol'], json.dumps(r['features']), r['score'], r['rank'], r['included'], json.dumps(r['exclusion_reasons']), json.dumps(r['flags'])))
        counts = {'universe': len(rows), 'included': sum(r['included'] for r in rows),
                  'excluded': sum(not r['included'] for r in rows), 'flagged': sum(bool(r['flags']) for r in rows)}
        cur.execute("UPDATE qd_us_screener_runs SET regime=%s,status=%s,counts=%s,error=%s,finished_at=CASE WHEN %s='running' THEN NULL ELSE NOW() END WHERE id=%s",
                    (json.dumps(market_regime), status, json.dumps(counts), error, status, run_id))
        db.commit()
        cur.close()


def read(run_id=None):
    run = query('SELECT * FROM qd_us_screener_runs WHERE id=%s' if run_id else 'SELECT * FROM qd_us_screener_runs ORDER BY id DESC LIMIT 1',
                (run_id,) if run_id else (), one=True)
    if not run:
        return None
    run = dict(run)
    run['symbols'] = query('SELECT * FROM qd_us_screener_symbols WHERE run_id=%s ORDER BY rank NULLS LAST,symbol', (run['id'],))
    return run
