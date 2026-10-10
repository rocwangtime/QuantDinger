"""Scheduler SQL must survive the legacy PostgreSQL placeholder adapter."""
from unittest.mock import Mock

import pytest

from app.services.automation import store, worker
from app.utils import db_postgres as pg


def test_scheduler_query_survives_placeholder_conversion():
    query = worker.MONITORED_AUTOMATIONS_QUERY
    cursor = pg.PostgresCursor(Mock())
    assert '?' not in query
    assert "jsonb_exists(state, 'performance')" in query
    assert cursor._convert_placeholders(query) == query


@pytest.mark.integration
def test_scheduler_query_selects_monitored_rows_through_real_wrapper(db, monkeypatch):
    # Reuse the isolated schema and migrations from the automation replay suite,
    # but replace its raw psycopg2 connection with the actual application wrapper.
    from contextlib import contextmanager

    from tests.test_agent_automations_db import create

    raw_connection = store.get_db_connection

    @contextmanager
    def wrapped_connection():
        with raw_connection() as connection:
            yield pg.PostgresConnection(connection)

    monkeypatch.setattr(pg, '_get_connection_pool', lambda: None)
    monkeypatch.setattr(store, 'get_db_connection', wrapped_connection)
    cases = [
        ('paper_auto', False, {'performance': {}}, True),
        ('paper_auto', False, {'performance': None}, True),
        ('paper_auto', False, {}, False),
        ('paper_auto', False, {'other': {}}, False),
        ('paper_auto', True, {}, True),
        ('plan_only', False, {'performance': {}}, False),
        ('plan_only', True, {'performance': {}}, False),
    ]
    expected = []
    for mode, active, state, included in cases:
        row = create(execution_mode=mode)
        store.query('UPDATE qd_agent_automations SET active=%s,state=%s::jsonb WHERE id=%s RETURNING id',
                    (active, store.dumps(state), row['id']), one=True)
        if included:
            expected.append(row['id'])
    # The scheduler also observes active event portfolios, regardless of mode.
    for active in (True, False):
        row = create(kind='event_portfolio')
        store.query('UPDATE qd_agent_automations SET active=%s WHERE id=%s RETURNING id',
                    (active, row['id']), one=True)
        if active:
            expected.append(row['id'])
    assert [row['id'] for row in store.query(worker.MONITORED_AUTOMATIONS_QUERY)] == expected


# Shared fixture creates and drops a fresh schema in qd_automation_test.
from tests.test_agent_automations_db import db  # noqa: F401,E402
