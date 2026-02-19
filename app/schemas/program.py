from pydantic import BaseModel, Field
from typing import Any, Dict, Optional


class ProgramUpsertRequest(BaseModel):
    program_id: str = Field(..., description="Stable id for the program (e.g., 'str_code_9')")
    name: str = Field(..., description="Human readable name")
    is_baseline: bool = Field(default=False, description="Whether this program is the baseline")
    config: Dict[str, Any] = Field(default_factory=dict, description="Full program config")


class ProgramApplyRequest(BaseModel):
    program_id: str = Field(..., description="Program id to apply")
    persist_as_active: bool = Field(
        default=True,
        description="If true, mark this program as the active program in Settings (global).",
    )
