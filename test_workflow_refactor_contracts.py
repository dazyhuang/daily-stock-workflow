#!/usr/bin/env python3
import logging
from datetime import date

from workflow_checkpoint import checkpoint_version_matches, new_checkpoint_state
from pm_schema_docs import pm_json_field_instructions
from stock_selection_debate.providers import PortfolioManagerOutput


def test_pm_schema_contract_has_execution_gate_fields():
    fields = set(PortfolioManagerOutput.model_fields.keys())
    required = {"allow_direct_buy", "needs_intraday_confirmation", "entry_condition", "block_buy_reason"}
    assert required.issubset(fields), fields
    doc = pm_json_field_instructions()
    for name in required:
        assert name in doc


def test_checkpoint_version_contract():
    version = {"scoring_version": "a", "prompt_version": "b"}
    cp = new_checkpoint_state(
        trading_day="20260707",
        candidates=[{"stock": "000001"}],
        screening_signature="s",
        candidate_signature="c",
        version_meta=version,
    )
    assert checkpoint_version_matches(cp, version)
    assert not checkpoint_version_matches(cp, {"scoring_version": "changed"})


def test_intraday_import_has_no_file_handler_side_effect():
    import intraday_executor
    logger = logging.getLogger("intraday_executor")
    assert not any(isinstance(h, logging.FileHandler) for h in logger.handlers)
    s = intraday_executor._normalize_buy_signal({
        "stock": "000001",
        "signal": "WATCH",
        "buy_score": 75,
        "execution_gate": "INTRADAY_CONFIRMATION_REQUIRED",
        "allow_direct_buy": "false",
        "needs_intraday_confirmation": "true",
        "entry_condition": "放量突破确认",
        "signal_blockers": ["需确认"],
    })
    assert s["allow_direct_buy"] is False
    assert s["needs_intraday_confirmation"] is True
    assert s["intraday_entry_condition"] == "放量突破确认"


if __name__ == "__main__":
    test_pm_schema_contract_has_execution_gate_fields()
    test_checkpoint_version_contract()
    test_intraday_import_has_no_file_handler_side_effect()
    print("workflow refactor contract tests passed")
