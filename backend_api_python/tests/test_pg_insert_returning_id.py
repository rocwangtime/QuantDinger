"""INSERT on tables without an id column must not append RETURNING id (DB log spam)."""
import os
from uuid import uuid4

import pytest

from app.utils import db_postgres as pg


class _MissingId(Exception):
    pass


class _FakeCursor:
    """Raises like PostgreSQL when RETURNING id targets a table without id."""

    def __init__(self, id_tables=()):
        self.id_tables = set(id_tables)
        self.sent = []
        self.rowcount = 1
        self._row = None

    def execute(self, query, args=None):
        self.sent.append(query)
        self._row = None
        if query.endswith(' RETURNING id'):
            table = pg._insert_table(query)
            if table not in self.id_tables:
                raise _MissingId('column "id" does not exist')
            self._row = {'id': 41}

    def fetchone(self):
        return self._row


def test_insert_table_name_parsing():
    assert pg._insert_table('INSERT INTO qd_polymarket_ws_daily(day) VALUES (%s)') == 'qd_polymarket_ws_daily'
    assert pg._insert_table('  insert into public.QD_X (a) values (1)') == 'qd_x'
    assert pg._insert_table('INSERT INTO "Odd"."Name" VALUES (1)') == 'Name'
    assert pg._insert_table('UPDATE t SET a=1') is None


def test_known_no_id_tables_skip_returning_id():
    fake = _FakeCursor()
    cur = pg.PostgresCursor(fake)
    for table in ('qd_polymarket_observer_status', 'qd_polymarket_paper_accounts',
                  'qd_polymarket_paper_daily', 'qd_polymarket_ws_daily'):
        cur.execute(f'INSERT INTO {table}(a) VALUES (?) ON CONFLICT(a) DO UPDATE SET a=EXCLUDED.a', (1,))
        assert cur.lastrowid is None
    assert fake.sent == [q for q in fake.sent if 'RETURNING' not in q and 'SAVEPOINT' not in q]
    assert len(fake.sent) == 4


def test_unknown_no_id_table_is_learned_once(monkeypatch):
    monkeypatch.setattr(pg, '_NO_ID_TABLES', set(pg._NO_ID_TABLES))
    fake = _FakeCursor()
    cur = pg.PostgresCursor(fake)
    table = 'qd_learned_' + uuid4().hex[:8]
    cur.execute(f'INSERT INTO {table}(k) VALUES (?)', ('a',))
    first = list(fake.sent)
    assert any(q.endswith('RETURNING id') for q in first)
    cur.execute(f'INSERT INTO {table}(k) VALUES (?)', ('b',))
    assert fake.sent[len(first):] == [f'INSERT INTO {table}(k) VALUES (%s)']
    assert table in pg._NO_ID_TABLES


def test_tables_with_id_still_return_lastrowid(monkeypatch):
    monkeypatch.setattr(pg, '_NO_ID_TABLES', set(pg._NO_ID_TABLES))
    fake = _FakeCursor(id_tables={'qd_with_id'})
    cur = pg.PostgresCursor(fake)
    cur.execute('INSERT INTO qd_with_id(k) VALUES (?)', ('a',))
    assert cur.lastrowid == 41 and 'qd_with_id' not in pg._NO_ID_TABLES


def test_postgres_upserts_without_id_send_no_failing_statement(monkeypatch):
    dsn = os.environ.get('QD_TEST_POSTGRES_DSN')
    if not dsn:
        pytest.skip('QD_TEST_POSTGRES_DSN is required')
    import psycopg2
    from psycopg2.extras import RealDictCursor
    monkeypatch.setattr(pg, '_NO_ID_TABLES', set(pg._NO_ID_TABLES))
    schema = 'qd_retid_test_' + uuid4().hex
    conn = psycopg2.connect(dsn, options=f'-c search_path={schema}')
    try:
        with conn.cursor() as raw:
            raw.execute(f'CREATE SCHEMA {schema}')
            raw.execute('CREATE TABLE qd_polymarket_ws_daily (day date, session_id text, n int, '
                        'PRIMARY KEY (day, session_id))')
            raw.execute('CREATE TABLE qd_learn_me (k text PRIMARY KEY, v int)')
            raw.execute('CREATE TABLE qd_has_id (id bigserial PRIMARY KEY, k text)')
        conn.commit()
        sent = []

        class Spy:
            def __init__(self, inner):
                self.inner = inner

            def execute(self, query, args=None):
                sent.append(query)
                return self.inner.execute(query, args)

            def __getattr__(self, name):
                return getattr(self.inner, name)

        cur = pg.PostgresCursor(Spy(conn.cursor(cursor_factory=RealDictCursor)))
        upsert = ('INSERT INTO qd_polymarket_ws_daily(day,session_id,n) VALUES (%s,%s,%s) '
                  'ON CONFLICT(day,session_id) DO UPDATE SET n=EXCLUDED.n')
        cur.execute(upsert, ('2026-10-20', 's', 1))
        cur.execute(upsert, ('2026-10-20', 's', 2))
        assert sent == [upsert, upsert]
        cur.execute('INSERT INTO qd_learn_me(k,v) VALUES (%s,%s)', ('a', 1))
        before = len(sent)
        cur.execute('INSERT INTO qd_learn_me(k,v) VALUES (%s,%s)', ('b', 2))
        assert sent[before:] == ['INSERT INTO qd_learn_me(k,v) VALUES (%s,%s)']
        cur.execute('INSERT INTO qd_has_id(k) VALUES (%s)', ('x',))
        assert cur.lastrowid == 1
        conn.commit()
        with conn.cursor() as raw:
            raw.execute('SELECT n FROM qd_polymarket_ws_daily')
            assert raw.fetchone()[0] == 2
            raw.execute('SELECT count(*) FROM qd_learn_me')
            assert raw.fetchone()[0] == 2
    finally:
        conn.rollback()
        with conn.cursor() as raw:
            raw.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
        conn.commit()
        conn.close()
