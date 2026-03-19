"""
Lightweight daily scheduler for the EquityLens pipeline.
Runs the full ETL automatically after US market close.

Schedule:
  - 06:00 UTC (11:30 IST) → Extract + load (pre-market data refresh)
  - 21:30 UTC (03:00 IST) → Full run after NYSE close (4PM EST)

Usage:
  python -m src.pipeline.scheduler        # Start the scheduler daemon
  Ctrl+C to stop

Production equivalent: Apache Airflow DAG with schedule_interval='30 21 * * 1-5'
(weekdays only, after NYSE close). Here we use the 'schedule' library as a
lightweight substitute that behaves identically for a single-machine setup.

Deployment: On Streamlit Community Cloud, this won't run as a daemon.
Instead, use GitHub Actions (free) to trigger the pipeline on a cron schedule.
We provide the GitHub Actions workflow in Phase 6.
"""

import time
import schedule
from datetime import datetime

from config.logging_config import setup_logging
from src.pipeline.main_pipeline import run as run_pipeline

logger = setup_logging("pipeline.scheduler")


def morning_run() -> None:
    """
    Pre-market refresh at 06:00 UTC (11:30 IST).
    Uses cached data — just re-runs transforms and loads any updates.
    """
    logger.info(f"Scheduled morning run triggered — {datetime.utcnow()} UTC")
    try:
        run_pipeline(force=False, load_only=True)
    except Exception as exc:
        logger.error(f"Morning run failed: {exc}")


def eod_run() -> None:
    """
    End-of-day full run at 21:30 UTC (03:00 IST / 4:30 PM EST).
    Forces fresh data pull after NYSE market close.
    This is the primary daily pipeline run.
    """
    logger.info(f"Scheduled EOD run triggered — {datetime.utcnow()} UTC")
    try:
        run_pipeline(force=True, load_only=False)
    except Exception as exc:
        logger.error(f"EOD run failed: {exc}")


def start() -> None:
    """
    Start the scheduler daemon.
    Blocks indefinitely — run in a background terminal or as a service.

    Production equivalent:
      - Airflow: schedule_interval='30 21 * * 1-5' on the DAG decorator
      - GitHub Actions: cron: '30 21 * * 1-5' in .github/workflows/pipeline.yml
      - We provide the GitHub Actions version in Phase 6 (free, cloud-native)
    """
    logger.info("=" * 60)
    logger.info("EquityLens Scheduler started")
    logger.info("  Morning refresh : 06:00 UTC (11:30 IST) daily")
    logger.info("  EOD full run    : 21:30 UTC (03:00 IST) Mon-Fri")
    logger.info("  Press Ctrl+C to stop")
    logger.info("=" * 60)

    # Register jobs
    schedule.every().day.at("06:00").do(morning_run)

    # Weekdays only for EOD — no point running on weekends
    schedule.every().monday.at("21:30").do(eod_run)
    schedule.every().tuesday.at("21:30").do(eod_run)
    schedule.every().wednesday.at("21:30").do(eod_run)
    schedule.every().thursday.at("21:30").do(eod_run)
    schedule.every().friday.at("21:30").do(eod_run)

    # Heartbeat log every hour so you can confirm the scheduler is alive
    schedule.every().hour.do(
        lambda: logger.info(
            f"Scheduler heartbeat — next job: "
            f"{schedule.next_run().strftime('%Y-%m-%d %H:%M UTC')}"
        )
    )

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    start()
