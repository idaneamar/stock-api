import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/ai",
    tags=["AI"],
    responses={404: {"description": "Not found"}},
)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class ChatMessage(BaseModel):
    role: str   # "user" | "assistant"
    content: str


class AiChatRequest(BaseModel):
    scan_id: int
    scan_date: Optional[str] = None
    portfolio_size: Optional[float] = None
    trades: List[Dict[str, Any]] = []
    messages: List[ChatMessage]


class AiChatResponse(BaseModel):
    reply: str
    scan_id: int


# ---------------------------------------------------------------------------
# System prompt builder
# ---------------------------------------------------------------------------

def _build_system_prompt(
    scan_id: int,
    scan_date: Optional[str],
    portfolio_size: Optional[float],
    trades: List[Dict[str, Any]],
) -> str:
    date_str = scan_date or "unknown date"
    portfolio_str = (
        f"${portfolio_size:,.0f}" if portfolio_size else "not specified"
    )

    lines = [
        "You are a professional trading assistant analyzing algorithmic stock scan results.",
        f"Scan ID: #{scan_id} | Date: {date_str} | Portfolio size: {portfolio_str}",
        "",
        "RECOMMENDATIONS FROM THIS SCAN:",
        "─" * 60,
    ]

    if not trades:
        lines.append("No trade recommendations available for this scan.")
    else:
        for i, t in enumerate(trades, 1):
            symbol = t.get("symbol", "?")
            rec = t.get("recommendation", "?")
            entry = t.get("entry_price", 0)
            stop = t.get("stop_loss", 0)
            target = t.get("take_profit", 0)
            size = t.get("position_size", 0)
            rr = t.get("risk_reward_ratio", 0)
            strategy = t.get("strategy", "?")
            score = t.get("score", "N/A")

            # Calculate investment per position
            investment = entry * size if entry and size else 0
            risk_per_share = entry - stop if entry and stop and entry > stop else 0
            total_risk = risk_per_share * size if risk_per_share and size else 0

            lines.append(
                f"{i}. {symbol} — {rec}"
                f"\n   Entry: ${entry:.2f} | Stop: ${stop:.2f} | Target: ${target:.2f}"
                f"\n   Shares: {size} | Investment: ${investment:,.0f} | Risk: ${total_risk:,.0f}"
                f"\n   R/R: {rr:.1f}x | Strategy: {strategy} | Score: {score}"
            )

    total_invested = sum(
        (t.get("entry_price", 0) or 0) * (t.get("position_size", 0) or 0)
        for t in trades
    )
    total_risk = sum(
        max(0, (t.get("entry_price", 0) or 0) - (t.get("stop_loss", 0) or 0))
        * (t.get("position_size", 0) or 0)
        for t in trades
    )

    lines += [
        "─" * 60,
        f"SUMMARY: {len(trades)} recommendations | Total invested: ${total_invested:,.0f} | Total risk: ${total_risk:,.0f}",
        "",
        "YOUR ROLE:",
        "- Provide clear, practical analysis of these recommendations",
        "- Answer allocation questions (e.g. 'I only have $30K, how to split?')",
        "- Highlight strongest and weakest setups by R/R and score",
        "- Keep answers concise and actionable",
        "- Always remind the user these are algorithmic signals, not guaranteed profits",
        "- Do NOT invent data — only use the numbers provided above",
    ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.post("/chat", response_model=AiChatResponse)
async def ai_chat(request: AiChatRequest):
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="AI service not configured. Set OPENAI_API_KEY on the server.",
        )

    try:
        from openai import OpenAI  # lazy import so missing package fails gracefully

        client = OpenAI(api_key=api_key)

        system_prompt = _build_system_prompt(
            scan_id=request.scan_id,
            scan_date=request.scan_date,
            portfolio_size=request.portfolio_size,
            trades=request.trades,
        )

        openai_messages = [{"role": "system", "content": system_prompt}]
        for msg in request.messages:
            openai_messages.append({"role": msg.role, "content": msg.content})

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=openai_messages,
            max_tokens=1024,
            temperature=0.4,
        )

        reply = response.choices[0].message.content or ""
        return AiChatResponse(reply=reply, scan_id=request.scan_id)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"AI chat error: {e}")
        raise HTTPException(status_code=500, detail=f"AI error: {str(e)}")
