"""
options_scheduler.py
--------------------
APScheduler-based scheduler for the Iron Condor options system.

Schedule (all times US Eastern):
  - Every Monday at 09:30  → fetch_sp500_symbols (symbol list update)
  - Every weekday at 17:00 → prefetch_options_datasp (1h after market close at 16:00)

Usage: call start_options_scheduler() once at app startup.
"""

from __future__ import annotations

import logging
from datetime import date

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.services.options_service import run_fetch_sp500, run_optsp, run_prefetch

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


# ---------------------------------------------------------------------------
# Job functions
# ---------------------------------------------------------------------------

def _job_monday_open() -> None:
    """Monday 09:30 ET: refresh S&P 500 list.
    Recommendations (optsp) are generated manually by the user via the dashboard button.
    """
    logger.info("[options-scheduler] Monday open: fetching S&P 500 symbols...")
    result = run_fetch_sp500()
    if result["ok"]:
        logger.info(
            "[options-scheduler] S&P 500 symbols updated. "
            "diff: " + str(result.get("summary", ""))
        )
    else:
        logger.error(f"[options-scheduler] fetch_sp500 failed: {result['stderr'][-500:]}")


def _job_daily_prefetch() -> None:
    """Weekday 16:30 ET: update options data cache for current year."""
    current_year = date.today().year
    logger.info(f"[options-scheduler] Daily prefetch for year {current_year}...")
    result = run_prefetch(years=[current_year], workers=4)
    if result["ok"]:
        logger.info("[options-scheduler] Prefetch completed successfully.")
    else:
        logger.error(f"[options-scheduler] Prefetch failed: {result['stderr'][-500:]}")


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def start_options_scheduler() -> None:
    """Start the background scheduler. Safe to call multiple times (idempotent)."""
    global _scheduler

    if _scheduler is not None and _scheduler.running:
        logger.info("[options-scheduler] Already running, skipping start.")
        return

    _scheduler = BackgroundScheduler(timezone="America/New_York")

    # Monday 09:30 ET — fetch symbols + run optsp
    _scheduler.add_job(
        _job_monday_open,
        trigger=CronTrigger(day_of_week="mon", hour=9, minute=30, timezone="America/New_York"),
        id="options_monday_open",
        name="Options Monday Open (fetch symbols + optsp)",
        replace_existing=True,
        misfire_grace_time=300,  # 5-min window; prevents stale fires on server restarts
    )

    # Monday–Friday 17:00 ET — prefetch options data (1h after market close at 16:00)
    _scheduler.add_job(
        _job_daily_prefetch,
        trigger=CronTrigger(day_of_week="mon-fri", hour=17, minute=0, timezone="America/New_York"),
        id="options_daily_prefetch",
        name="Options Daily Prefetch (1h after market close)",
        replace_existing=True,
        misfire_grace_time=300,  # 5-min window; prevents stale fires on server restarts
    )

    _scheduler.start()
    logger.info(
        "[options-scheduler] Started. "
        "Monday 09:30 ET → fetch SP500 symbols | "
        "Mon-Fri 17:00 ET → prefetch (1h after close)"
    )


def stop_options_scheduler() -> None:
    """Gracefully stop the scheduler (e.g. on app shutdown)."""
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("[options-scheduler] Stopped.")
    _scheduler = None


def get_scheduler_jobs() -> list:
    """Return info about scheduled jobs (for status endpoint)."""
    if _scheduler is None or not _scheduler.running:
        return []
    jobs = []
    for job in _scheduler.get_jobs():
        next_run = job.next_run_time
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": next_run.isoformat() if next_run else None,
        })
    return jobs
