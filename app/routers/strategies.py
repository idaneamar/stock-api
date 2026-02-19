import logging
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.models.database import get_db
from app.models.strategy import Strategy
from app.schemas.response import StandardResponse
from app.schemas.strategy import StrategyCreateRequest, StrategyUpdateRequest


router = APIRouter(
    prefix="/strategies",
    tags=["Strategies"],
    responses={404: {"description": "Not found"}},
)


@router.get("/", response_model=StandardResponse)
async def list_strategies(
    enabled_only: bool = Query(False, description="Return only enabled strategies"),
    db: Session = Depends(get_db),
):
    try:
        query = db.query(Strategy)
        if enabled_only:
            query = query.filter(Strategy.enabled.is_(True))

        items = [
            {"id": int(s.id), "name": s.name, "enabled": bool(s.enabled), "config": s.config or {}}
            for s in query.order_by(Strategy.name.asc(), Strategy.id.asc()).all()
        ]
        return StandardResponse(
            success=True,
            status=200,
            message="Strategies retrieved successfully",
            data={"items": items, "total": len(items)},
        )
    except Exception as e:
        logging.error(f"Error listing strategies: {e}")
        raise HTTPException(status_code=500, detail="Error listing strategies")


@router.post("/", response_model=StandardResponse)
async def create_strategy(payload: StrategyCreateRequest, db: Session = Depends(get_db)):
    try:
        strategy = Strategy(
            name=payload.name,
            enabled=payload.enabled,
            config=payload.config or {},
        )
        db.add(strategy)
        db.commit()
        db.refresh(strategy)

        return StandardResponse(
            success=True,
            status=201,
            message="Strategy created successfully",
            data={"id": int(strategy.id), "name": strategy.name, "enabled": bool(strategy.enabled), "config": strategy.config or {}},
        )
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logging.error(f"Error creating strategy: {e}")
        raise HTTPException(status_code=500, detail="Error creating strategy")


@router.get("/{strategy_id}", response_model=StandardResponse)
async def get_strategy(strategy_id: int, db: Session = Depends(get_db)):
    strategy = db.query(Strategy).filter(Strategy.id == strategy_id).first()
    if not strategy:
        raise HTTPException(status_code=404, detail=f"Strategy '{strategy_id}' not found")
    return StandardResponse(
        success=True,
        status=200,
        message="Strategy retrieved successfully",
        data={"id": int(strategy.id), "name": strategy.name, "enabled": bool(strategy.enabled), "config": strategy.config or {}},
    )


@router.put("/{strategy_id}", response_model=StandardResponse)
async def update_strategy(strategy_id: int, payload: StrategyUpdateRequest, db: Session = Depends(get_db)):
    try:
        strategy = db.query(Strategy).filter(Strategy.id == strategy_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail=f"Strategy '{strategy_id}' not found")

        if payload.name is not None:
            strategy.name = payload.name
        if payload.enabled is not None:
            strategy.enabled = payload.enabled
        if payload.config is not None:
            strategy.config = payload.config

        db.commit()
        db.refresh(strategy)

        return StandardResponse(
            success=True,
            status=200,
            message="Strategy updated successfully",
            data={"id": int(strategy.id), "name": strategy.name, "enabled": bool(strategy.enabled), "config": strategy.config or {}},
        )
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logging.error(f"Error updating strategy '{strategy_id}': {e}")
        raise HTTPException(status_code=500, detail="Error updating strategy")


@router.delete("/{strategy_id}", response_model=StandardResponse)
async def delete_strategy(strategy_id: int, db: Session = Depends(get_db)):
    try:
        strategy = db.query(Strategy).filter(Strategy.id == strategy_id).first()
        if not strategy:
            raise HTTPException(status_code=404, detail=f"Strategy '{strategy_id}' not found")

        db.delete(strategy)
        db.commit()
        return StandardResponse(success=True, status=200, message="Strategy deleted successfully", data={"id": int(strategy_id)})
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logging.error(f"Error deleting strategy '{strategy_id}': {e}")
        raise HTTPException(status_code=500, detail="Error deleting strategy")
