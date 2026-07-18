"""Checkpoint helpers for stock-selection workflow resume state."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from workflow_common import write_json_atomic


def new_checkpoint_state(
    *,
    trading_day: str,
    candidates: List[Dict[str, Any]],
    screening_signature: str,
    candidate_signature: str,
    version_meta: Dict[str, str],
) -> Dict[str, Any]:
    return {
        "date": trading_day,
        "completed": [],
        "failed": [],
        "results": {},
        "candidates": candidates,
        "screening_signature": screening_signature,
        "candidate_signature": candidate_signature,
        "version_meta": dict(version_meta or {}),
        "checkpoint_schema_version": "2026-07-10.node-state-v2",
        "node_status": {},
        "created_at": datetime.now().isoformat(),
    }


def checkpoint_version_matches(cp: Dict[str, Any], version_meta: Dict[str, str]) -> bool:
    return (cp.get("version_meta") or {}) == dict(version_meta or {})


def refresh_checkpoint_metadata(
    cp: Dict[str, Any],
    *,
    trading_day: str,
    candidates: List[Dict[str, Any]],
    screening_signature: str,
    candidate_signature: str,
    version_meta: Dict[str, str],
) -> Dict[str, Any]:
    cp["date"] = trading_day
    if screening_signature:
        cp["screening_signature"] = screening_signature
    cp["candidate_signature"] = candidate_signature
    cp["version_meta"] = dict(version_meta or {})
    cp["checkpoint_schema_version"] = "2026-07-10.node-state-v2"
    cp["candidates"] = candidates
    cp["updated_at"] = datetime.now().isoformat()
    return cp


def write_checkpoint(path: Path, cp: Dict[str, Any]) -> None:
    write_json_atomic(path, cp)
