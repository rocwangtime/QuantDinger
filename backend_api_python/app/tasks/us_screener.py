"""Bounded, isolated read-only research jobs on the maintenance queue."""
import os
from app.celery_app import celery_app


@celery_app.task(name='quantdinger.tasks.us_screener_tick', soft_time_limit=30, time_limit=40)
def screener_tick():
    from app.services.us_screener import store
    # Recover jobs interrupted by a worker hard timeout; retain pending coverage.
    store.query("UPDATE qd_us_screener_runs SET status='failed',error='worker_timeout',finished_at=NOW() WHERE status IN ('running','queued') AND created_at < NOW()-INTERVAL '15 minutes'")
    store.query("DELETE FROM qd_us_screener_bar_cache WHERE as_of<CURRENT_DATE-14")
    store.query("DELETE FROM qd_us_screener_quote_cache WHERE session_date<CURRENT_DATE-14")
    store.query("DELETE FROM qd_us_screener_quote_captures WHERE session_date<CURRENT_DATE-14")
    store.query("UPDATE qd_us_screener_quote_captures SET status='failed',error='worker_timeout',finished_at=NOW() WHERE status='running' AND created_at<NOW()-INTERVAL '5 minutes'")
    from app.services.us_screener.capture import enabled
    if enabled():
        from app.services.us_screener.core import capture_session
        from datetime import datetime, timezone
        if capture_session(datetime.now(timezone.utc)) is not None:
            capture_rth_quotes.delay()
    if os.getenv('US_SCREENER_ENABLED', 'false').lower() == 'true':
        from app.services.us_screener.service import enqueue
        row = enqueue(scheduled=True)
        return row['id'] if row else None


@celery_app.task(name='quantdinger.tasks.us_screener', soft_time_limit=510, time_limit=540)
def run_screener(run_id):
    from app.services.us_screener.service import execute
    execute(run_id)


@celery_app.task(name='quantdinger.tasks.us_screener_rth_capture', soft_time_limit=85, time_limit=95)
def capture_rth_quotes():
    from app.services.us_screener.capture import capture
    return capture()
