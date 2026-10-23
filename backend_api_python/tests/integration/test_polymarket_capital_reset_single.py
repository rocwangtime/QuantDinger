"""Actual PG18 SQL/transaction smoke via single-user mode (no sockets needed)."""
import json
import os
import subprocess
from contextlib import contextmanager
from pathlib import Path

import pytest
from psycopg2.extensions import adapt

from app.services.polymarket import paper_account as paper
from tests.test_polymarket_maker import NOW
from tests.test_polymarket_paper_capital_reset_command import Database

pytestmark = pytest.mark.integration


@pytest.mark.parametrize('abort', [False, True])
def test_reset_sql_on_postgres18_without_sockets(tmp_path, monkeypatch, abort):
    binary = os.getenv('QD_TEST_POSTGRES_SINGLE', '')
    if not binary:
        pytest.skip('QD_TEST_POSTGRES_SINGLE required (local PG18 binary)')
    binary = Path(binary)
    data = tmp_path / 'pgdata'
    subprocess.run([str(binary.with_name('initdb')), '-D', str(data), '-A', 'trust', '-U', 'postgres'],
                   check=True, capture_output=True)
    account = paper.Account()
    account.state['free'] = '4964.34027'
    db = Database(account.state)

    @contextmanager
    def connect():
        yield db

    monkeypatch.setattr(paper, 'get_db_connection', connect)
    paper.Store().reset_capital({}, NOW)
    migration = Path(__file__).parents[2] / 'migrations/20261011_polymarket_paper_account.sql'
    schema = '\n'.join(line for line in migration.read_text().splitlines() if not line.startswith('--'))
    statements = [' '.join(sql.split()) + ';' for sql in schema.split(';') if sql.strip()]
    literal = lambda value: adapt(value).getquoted().decode()
    statements += ["INSERT INTO qd_polymarket_paper_accounts(account_id,state) VALUES ('global', "
                   + literal(json.dumps(account.state)) + '::jsonb);',
                   "INSERT INTO qd_polymarket_paper_trades(id,account_id,market_id,market_type,status,signal_at,result,evidence,evidence_hash) VALUES ('old','global','42','yes_no','not_filled','2020-01-01','{}','',repeat('a',64));",
                   "INSERT INTO qd_polymarket_paper_legs(trade_id,role,detail) VALUES ('old','0','{}');",
                   "INSERT INTO qd_polymarket_paper_daily(account_id,day,stats) VALUES ('global','2020-01-01','{}');",
                   "INSERT INTO qd_polymarket_paper_ledger(account_id,observed_at,event,free,locked,positions,equity,detail) VALUES ('global','2020-01-01','old_audit',4964.34027,0,0,4964.34027,'{}');",
                   'BEGIN;']
    for sql, args in db.queries:
        statements.append(sql % tuple(literal(value) for value in args) + ';')
    if abort:
        # A ledger constraint failure must also roll back the preceding account UPDATE.
        statements.append("INSERT INTO qd_polymarket_paper_ledger(account_id,observed_at,event,free,locked,positions,equity,detail) VALUES ('global',NOW(),'bad',-1,0,0,-1,'{}');")
    statements += ['COMMIT;', "SELECT state->>'free' AS reset_free FROM qd_polymarket_paper_accounts WHERE account_id='global';",
                   "SELECT count(*) AS reset_ledgers FROM qd_polymarket_paper_ledger WHERE event='capital_reset';",
                   "SELECT (SELECT count(*) FROM qd_polymarket_paper_trades WHERE id='old') + (SELECT count(*) FROM qd_polymarket_paper_legs WHERE trade_id='old') + (SELECT count(*) FROM qd_polymarket_paper_daily WHERE day='2020-01-01') + (SELECT count(*) FROM qd_polymarket_paper_ledger WHERE event='old_audit') AS old_rows;"]
    result = subprocess.run([str(binary), '--single', '-D', str(data), 'postgres'],
                            input='\n'.join(statements) + '\n', text=True, capture_output=True, check=True)
    assert 'old_rows = \"4\"' in result.stdout
    if abort:
        assert 'violates check constraint' in result.stderr
        assert '4964.34027' in result.stdout
        assert 'reset_ledgers = "0"' in result.stdout
    else:
        assert 'ERROR' not in result.stderr
        assert 'reset_free = "5000"' in result.stdout
        assert 'reset_ledgers = "1"' in result.stdout
