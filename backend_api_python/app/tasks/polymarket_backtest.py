"""Optional on-demand bounded offline research; no periodic scheduling."""

from app.celery_app import celery_app
from app.services.polymarket.backtest import run


@celery_app.task(name="quantdinger.tasks.polymarket_backtest", soft_time_limit=240, time_limit=300)
def polymarket_backtest(config):
    return run(config)
