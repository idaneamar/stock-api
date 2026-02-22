import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.program import Program
from app.models.strategy import Strategy
from app.models.settings import Settings
from app.schemas.response import StandardResponse
from app.schemas.program import ProgramUpsertRequest, ProgramApplyRequest


router = APIRouter(
    prefix="/programs",
    tags=["Programs"],
    responses={404: {"description": "Not found"}},
)


def _apply_program_to_db(db: Session, program: Program) -> Dict[str, Any]:
    """Apply a program by toggling Strategy.enabled and persisting active config in Settings."""

    config = program.config or {}
    enabled_names = set((config.get("enabled_strategy_names") or []))

    # Toggle strategies
    strategies = db.query(Strategy).all()
    changed = 0
    for s in strategies:
        desired = (s.name in enabled_names) if enabled_names else bool(s.enabled)
        if bool(s.enabled) != bool(desired):
            s.enabled = bool(desired)
            changed += 1

    # Persist active program + config
    Settings.set_active_program(db, program.program_id, config)
    db.commit()

    return {
        "applied_program_id": program.program_id,
        "strategies_changed": changed,
        "enabled_strategy_names": sorted(list(enabled_names)),
        "config": config,
    }


@router.get("/", response_model=StandardResponse)
async def list_programs(db: Session = Depends(get_db)):
    try:
        items = (
            db.query(Program)
            .order_by(Program.is_baseline.desc(), Program.program_id.asc())
            .all()
        )
        data = [
            {
                "id": int(p.id),
                "program_id": p.program_id,
                "name": p.name,
                "is_baseline": bool(p.is_baseline),
                "config": p.config or {},
            }
            for p in items
        ]
        active = Settings.get_active_program(db)
        return StandardResponse(
            success=True,
            status=200,
            message="Programs retrieved successfully",
            data={"items": data, "total": len(data), "active_program": active},
        )
    except Exception as e:
        logging.error(f"Error listing programs: {e}")
        raise HTTPException(status_code=500, detail="Error listing programs")


@router.post("/", response_model=StandardResponse)
async def upsert_program(payload: ProgramUpsertRequest, db: Session = Depends(get_db)):
    try:
        program = db.query(Program).filter(Program.program_id == payload.program_id).first()
        if program is None:
            program = Program(program_id=payload.program_id, name=payload.name)
            db.add(program)

        program.name = payload.name
        program.is_baseline = bool(payload.is_baseline)
        program.config = payload.config or {}

        # Ensure only one baseline
        if program.is_baseline:
            db.query(Program).filter(Program.program_id != program.program_id).update(
                {Program.is_baseline: False}
            )

        db.commit()
        db.refresh(program)

        return StandardResponse(
            success=True,
            status=200,
            message="Program saved successfully",
            data={
                "id": int(program.id),
                "program_id": program.program_id,
                "name": program.name,
                "is_baseline": bool(program.is_baseline),
                "config": program.config or {},
            },
        )
    except Exception as e:
        db.rollback()
        logging.error(f"Error saving program: {e}")
        raise HTTPException(status_code=500, detail="Error saving program")


@router.post("/apply", response_model=StandardResponse)
async def apply_program(payload: ProgramApplyRequest, db: Session = Depends(get_db)):
    try:
        program = db.query(Program).filter(Program.program_id == payload.program_id).first()
        if not program:
            raise HTTPException(status_code=404, detail=f"Program '{payload.program_id}' not found")

        out = _apply_program_to_db(db, program) if payload.persist_as_active else {
            "applied_program_id": program.program_id,
            "config": program.config or {},
            "note": "Not persisted as active",
        }

        return StandardResponse(
            success=True,
            status=200,
            message=f"Program '{program.program_id}' applied successfully",
            data=out,
        )
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logging.error(f"Error applying program: {e}")
        raise HTTPException(status_code=500, detail="Error applying program")


@router.delete("/{program_id}", response_model=StandardResponse)
async def delete_program(program_id: str, db: Session = Depends(get_db)):
    """Delete a user-created program. The built-in baseline program cannot be deleted."""
    try:
        program = (
            db.query(Program)
            .filter(Program.program_id == program_id, Program.is_baseline.is_(False))
            .first()
        )
        if not program:
            raise HTTPException(
                status_code=404,
                detail=f"Program '{program_id}' not found or is the protected baseline program",
            )
        db.delete(program)
        db.commit()
        return StandardResponse(
            success=True,
            status=200,
            message=f"Program '{program_id}' deleted successfully",
            data={"program_id": program_id, "deleted": True},
        )
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logging.error(f"Error deleting program: {e}")
        raise HTTPException(status_code=500, detail="Error deleting program")


@router.post("/revert-baseline", response_model=StandardResponse)
async def revert_to_baseline(db: Session = Depends(get_db)):
    try:
        baseline = db.query(Program).filter(Program.is_baseline.is_(True)).first()
        if not baseline:
            raise HTTPException(status_code=404, detail="Baseline program not found")

        out = _apply_program_to_db(db, baseline)
        return StandardResponse(
            success=True,
            status=200,
            message=f"Reverted to baseline '{baseline.program_id}'",
            data=out,
        )
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logging.error(f"Error reverting baseline: {e}")
        raise HTTPException(status_code=500, detail="Error reverting baseline")
