"""Central model route resolution for workflow LLM calls."""

from __future__ import annotations

from dataclasses import dataclass
import os

DEFAULT_MODEL = os.getenv("STOCK_SELECTION_DEFAULT_MODEL", "volcengine-plan/ark-code-latest")
FALLBACK_MODEL = os.getenv("STOCK_SELECTION_FALLBACK_MODEL", "openai/gpt-5.6-sol")
SECONDARY_FALLBACK_MODEL = os.getenv("STOCK_SELECTION_SECONDARY_FALLBACK_MODEL", "minimax-portal/MiniMax-M3")


@dataclass(frozen=True)
class ModelRoute:
    primary: str
    fallback: str
    secondary: str


def resolve_model_route(model: str = "", fallback_model: str = "", secondary_fallback_model: str = "") -> ModelRoute:
    primary = model or DEFAULT_MODEL
    fallback = fallback_model or FALLBACK_MODEL
    secondary = secondary_fallback_model or SECONDARY_FALLBACK_MODEL
    if fallback == primary:
        fallback = ""
    if secondary in {primary, fallback}:
        secondary = ""
    return ModelRoute(primary=primary, fallback=fallback, secondary=secondary)
