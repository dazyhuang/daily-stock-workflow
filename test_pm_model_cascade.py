#!/usr/bin/env python3
from urllib.error import URLError

from stock_selection_debate import debate_engine, providers


def _pm_result(reason: str):
    return {
        "signal": "WATCH",
        "buy_score": 63,
        "confidence": 70,
        "position_ratio": 0.15,
        "reason": reason,
        "evidence_refs": [],
        "missing_data_used": [],
        "unsupported_claims": [],
    }


def main():
    original_openai = providers._call_structured_openai_responses
    original_retries = providers.effective_llm_retries
    original_engine_structured = debate_engine._call_structured
    try:
        providers.effective_llm_retries = lambda node_name="default", default=3: 1
        debate_engine._pm_primary_broken = False
        debate_engine._secondary_broken = False

        calls = []

        def sol_success(*args, **kwargs):
            model = kwargs.get("model") or args[0]
            effort = kwargs.get("reasoning_effort", "max")
            calls.append((model, effort))
            return _pm_result("GPT-5.6 Sol success")

        providers._call_structured_openai_responses = sol_success
        result, source = debate_engine._call_portfolio_manager_structured(
            "single-stock-test",
            debate_engine._call_structured,
            providers.PortfolioManagerOutput,
            "单股级联测试",
        )
        assert source == "Structured:GPT-5.6-Sol", source
        assert result.reason == "GPT-5.6 Sol success"
        assert calls == [
            ("openai/gpt-5.6-sol", "max"),
        ], calls

        calls.clear()
        debate_engine._pm_primary_broken = False
        debate_engine._secondary_broken = False

        def openai_failed(*args, **kwargs):
            model = kwargs.get("model") or args[0]
            calls.append(model)
            raise URLError(f"simulated failure: {model}")

        def minimax_success(*args, **kwargs):
            assert kwargs.get("model") == "minimax-portal/MiniMax-M3"
            return providers.PortfolioManagerOutput(**_pm_result("MiniMax fallback success"))

        providers._call_structured_openai_responses = openai_failed
        debate_engine._call_structured = minimax_success
        result, source = debate_engine._call_portfolio_manager_structured(
            "single-stock-test",
            minimax_success,
            providers.PortfolioManagerOutput,
            "单股级联测试",
        )
        assert source == "Structured:MiniMax-M3", source
        assert result.reason == "MiniMax fallback success"
        assert calls == ["openai/gpt-5.6-sol"], calls
    finally:
        providers._call_structured_openai_responses = original_openai
        providers.effective_llm_retries = original_retries
        debate_engine._call_structured = original_engine_structured
        debate_engine._pm_primary_broken = False
        debate_engine._secondary_broken = False

    print("PM model cascade tests passed")


if __name__ == "__main__":
    main()
