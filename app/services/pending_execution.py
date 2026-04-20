"""
pending_execution.py — 5-minute countdown before sending to IBKR (manual use only).

The app does NOT create a pending job after generating recommendations.
Sending to IBKR is always manual (Execute / Send to IBKR buttons).

Flow when a pending job exists (e.g. created by a future "Schedule send" action):
  1. Client calls POST /options/pending-execution (manual only)
  2. A job is created with a 5-minute countdown
  3. The Flutter app polls GET /options/pending-execution and shows a banner
  4. User can: Send Now, Cancel, or let it auto-fire after 5 minutes
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

COUNTDOWN_SECONDS = 300  # 5 minutes


class PendingExecutionJob:
    def __init__(self, job_id: str, rec_date: str, config: Dict[str, Any]) -> None:
        self.job_id = job_id
        self.rec_date = rec_date
        self.config = config
        self.created_at = datetime.now(timezone.utc)
        # status: pending | confirmed | cancelled | executed | failed
        self.status = "pending"
        self.executed_at: Optional[datetime] = None
        self.result: Optional[Dict[str, Any]] = None
        self._task: Optional[asyncio.Task] = None

    def remaining_seconds(self) -> int:
        if self.status != "pending":
            return 0
        elapsed = (datetime.now(timezone.utc) - self.created_at).total_seconds()
        return max(0, COUNTDOWN_SECONDS - int(elapsed))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "rec_date": self.rec_date,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "remaining_seconds": self.remaining_seconds(),
            "countdown_total": COUNTDOWN_SECONDS,
            "result": self.result,
        }


# ── Global state (single active job at a time) ─────────────────────────────

_current_job: Optional[PendingExecutionJob] = None


def get_current_job() -> Optional[PendingExecutionJob]:
    return _current_job


async def create_pending_job(rec_date: str, config: Dict[str, Any]) -> PendingExecutionJob:
    global _current_job

    # Cancel any existing pending job before creating a new one
    if _current_job and _current_job.status == "pending":
        await cancel_job()

    job = PendingExecutionJob(
        job_id=str(uuid.uuid4())[:8],
        rec_date=rec_date,
        config=config,
    )
    _current_job = job

    # Schedule auto-execution after countdown
    job._task = asyncio.create_task(_auto_execute(job))
    logger.info(
        f"[pending] Job {job.job_id} created for {rec_date} — "
        f"auto-fires in {COUNTDOWN_SECONDS}s"
    )
    return job


async def confirm_job(job_id: str) -> Optional[PendingExecutionJob]:
    """Execute immediately (user tapped 'Send Now')."""
    global _current_job
    job = _current_job
    if not job or job.job_id != job_id or job.status != "pending":
        return None

    # Cancel the auto-fire task so it doesn't double-execute
    if job._task and not job._task.done():
        job._task.cancel()

    job.status = "confirmed"
    asyncio.create_task(_execute_job(job))
    return job


async def cancel_job(job_id: Optional[str] = None) -> bool:
    """Cancel the pending job (user tapped 'Cancel')."""
    global _current_job
    job = _current_job
    if not job or (job_id and job.job_id != job_id) or job.status != "pending":
        return False

    if job._task and not job._task.done():
        job._task.cancel()

    job.status = "cancelled"
    logger.info(f"[pending] Job {job.job_id} cancelled")
    return True


# ── Internal helpers ────────────────────────────────────────────────────────

async def _auto_execute(job: PendingExecutionJob) -> None:
    try:
        await asyncio.sleep(COUNTDOWN_SECONDS)
        if job.status == "pending":
            logger.info(f"[pending] Auto-executing job {job.job_id} after countdown")
            job.status = "confirmed"
            await _execute_job(job)
    except asyncio.CancelledError:
        pass


async def _execute_job(job: PendingExecutionJob) -> None:
    from app.services import options_service as svc

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: svc.run_exeopt(
                rec_date=job.rec_date,
                dry_run=job.config.get("dry_run", False),
                port=job.config.get("port", 7497),
                client_id=job.config.get("client_id", 1),
                stop_loss_pct=job.config.get("stop_loss_pct", 1.0),
                take_profit_pct=job.config.get("take_profit_pct", 0.50),
            ),
        )
        job.status = "executed"
        job.result = result
        job.executed_at = datetime.now(timezone.utc)
        logger.info(f"[pending] Job {job.job_id} executed — ok={result.get('ok')}")
    except Exception as exc:
        job.status = "failed"
        job.result = {"ok": False, "error": str(exc)}
        logger.error(f"[pending] Job {job.job_id} failed: {exc}")
