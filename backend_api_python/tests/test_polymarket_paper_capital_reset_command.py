"""Offline command/persistence contracts: no production connections."""
import copy
import json
from contextlib import contextmanager

import pytest

from app.commands import polymarket_paper_capital_reset as command
from app.services.polymarket import paper_account as paper
from tests.test_polymarket_maker import NOW


class Database:
    def __init__(self, state):
        self.state, self.acquired, self.fail = state, True, False
        self.queries = []
        self.committed = self.rolled_back = False

    def cursor(self):
        return self

    def execute(self, sql, args):
        self.queries.append((sql, args))
        if self.fail and sql.startswith('INSERT INTO qd_polymarket_paper_ledger'):
            raise RuntimeError('injected ledger failure')

    def fetchone(self):
        return {'acquired': self.acquired} if 'advisory' in self.queries[-1][0] else (
            {'state': copy.deepcopy(self.state)} if self.state is not None else None)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


@pytest.fixture
def database(monkeypatch):
    account = paper.Account()
    account.state['free'] = '4964.34027'
    account.state['days']['2026-01-01'] = {'old': True}
    db = Database(account.state)

    @contextmanager
    def connect():
        yield db

    monkeypatch.setattr(paper, 'get_db_connection', connect)
    return db


def test_store_reset_atomically_keeps_history_and_appends_ledger(database):
    detail = paper.Store().reset_capital({}, NOW)
    assert database.committed and not database.rolled_back
    assert not any('DELETE' in sql for sql, _ in database.queries)
    assert 'FOR UPDATE' in database.queries[1][0]
    state = json.loads(database.queries[2][1][0])
    assert state['days']['2026-01-01'] == {'old': True}
    ledger = next(args for sql, args in database.queries if 'INSERT INTO qd_polymarket_paper_ledger' in sql)
    assert ledger[2] == 'capital_reset' and ledger[3:7] == ('5000', '0', '0', '5000')
    assert json.loads(ledger[-1]) == detail
    assert state['events'][-1]['detail'] == detail


@pytest.mark.parametrize('case', ['worker', 'missing', 'exposure', 'ledger'])
def test_store_refuses_or_rolls_back_all_changes(database, case):
    if case == 'worker':
        database.acquired = False
    elif case == 'missing':
        database.state = None
    elif case == 'exposure':
        database.state['holdings'] = {'x': {}}
    else:
        database.fail = True
    with pytest.raises((ValueError, RuntimeError)):
        paper.Store().reset_capital({}, NOW)
    assert database.rolled_back and not database.committed
    if case != 'ledger':
        assert not any(sql.startswith(('UPDATE', 'INSERT')) for sql, _ in database.queries)


def test_store_also_protects_contaminated_id(database):
    with pytest.raises(ValueError, match='contaminated'):
        paper.Store('global-contaminated-20261014').reset_capital({}, NOW)
    assert database.queries == []


def test_command_defaults_summary_and_config(monkeypatch, database, capsys):
    monkeypatch.setattr('sys.argv', ['polymarket_paper_capital_reset'])
    command.main()
    result = json.loads(capsys.readouterr().out)
    assert result['account_id'] == 'global'
    assert result['before']['equity'] == '4964.34027' and result['after']['equity'] == '5000'
    custom = paper.Account({'initial_capital': '6000'})
    assert custom.reset_capital(NOW)['after']['equity'] == '6000'


def test_command_rejects_contaminated_before_database(monkeypatch, database):
    monkeypatch.setattr('sys.argv', ['reset', '--account-id', 'global-contaminated-20261014'])
    with pytest.raises(SystemExit) as exc:
        command.main()
    assert exc.value.code == 2 and database.queries == []
