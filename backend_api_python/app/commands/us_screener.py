"""Run locally: python -m app.commands.us_screener (research only)."""
import multiprocessing
import os
from datetime import datetime
from zoneinfo import ZoneInfo


def child(run_id):
    from app.services.us_screener.service import execute
    execute(run_id)


def main():
    os.environ['QD_PROCESS_ROLE'] = 'cli'
    from app.services.us_screener import store
    from app.services.us_screener.service import parameters, latest_closed_day
    import uuid
    day = latest_closed_day(datetime.now(ZoneInfo('America/New_York')))
    run = store.create(day, parameters(), 'cli:'+str(uuid.uuid4()))
    if not run:
        raise SystemExit('Screener busy; another research run is pending.')
    worker = multiprocessing.get_context('spawn').Process(target=child, args=(run['id'],))
    worker.start()
    worker.join(540)
    if worker.is_alive():
        worker.terminate()
        worker.join(5)
        if worker.is_alive():
            worker.kill()
            worker.join(5)
        store.query("UPDATE qd_us_screener_runs SET status='failed',error='runtime_budget',finished_at=NOW() WHERE id=%s", (run['id'],))
    print('Research run:', run['id'])


if __name__ == '__main__':
    main()
