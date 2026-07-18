#!/usr/bin/env python3

import gzip
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import intraday_buy_weekly_review as review_mod
import intraday_executor as executor


def _write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def _write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _write_market(path: Path, day: str, stocks):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump({"schema_version": 1, "date": day, "stocks": stocks}, handle, ensure_ascii=False)


def _bars(day: str, start: float, closes):
    base = datetime.fromisoformat(f"{day}T09:31:00")
    result = []
    previous = start
    for idx, close in enumerate(closes):
        result.append({
            "time": (base + timedelta(minutes=idx)).isoformat(),
            "open": previous,
            "high": max(previous, close) * 1.003,
            "low": min(previous, close) * 0.997,
            "close": close,
            "volume": 1000,
        })
        previous = close
    return result


def _event(day, hhmm, stock, event_type, *, action=None, trigger=None, price=None, llm_status=None, order=None):
    decision = None
    if action:
        decision = {
            "action": action,
            "technical_trigger": trigger,
            "llm_status": llm_status,
            "llm_model": "test-model" if event_type == "LLM_DECISION" else None,
        }
    return {
        "schema_version": 1,
        "event_id": f"{day}:{hhmm}:{stock}:{event_type}",
        "date": day,
        "time": f"{day}T{hhmm}:00",
        "event_type": event_type,
        "stock": stock,
        "decision": decision,
        "market": {"price": price, "ask1": price, "limit_up": price * 1.1 if price else None},
        "order": order,
    }


def test_weekly_review_distinguishes_execution_outcomes_and_is_idempotent():
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "output"
        day = "2026-07-13"
        key = "20260713"
        state = {
            "date": day,
            "finished_at": f"{day}T14:57:00",
            "selected_stocks": ["000001", "000002", "000003", "000004"],
            "selected_signals": [
                {"stock": "000001", "name": "开盘买入"},
                {"stock": "000002", "name": "等待错过"},
                {"stock": "000003", "name": "模型失败"},
                {"stock": "000004", "name": "挂单未成"},
            ],
            "stocks": {
                "000001": {"status": "filled", "filled_price": 10.0, "filled_quantity": 1000, "filled_at": f"{day}T09:31:30", "submitted_order_count": 1, "last_decision": {"technical_trigger": "OPENING_STRONG"}},
                "000002": {"status": "open"},
                "000003": {"status": "open"},
                "000004": {"status": "open", "submitted_order_count": 1},
            },
        }
        _write_json(out / f"intraday_buy_timing_{key}.json", state)
        rows = [
            _event(day, "09:31", "000001", "RULE_DECISION", action="BUY_NOW", trigger="OPENING_STRONG", price=10.0),
            _event(day, "09:31", "000001", "ORDER_FILL", order={"filled_price": 10.0, "filled_quantity": 1000}),
            _event(day, "10:00", "000002", "LLM_DECISION", action="WAIT", trigger="MA120_CROSS_UP", price=10.0, llm_status="ok"),
            _event(day, "10:10", "000003", "LLM_DECISION", action="WAIT", trigger="PULLBACK_RESUME", price=10.0, llm_status="failed"),
            _event(day, "10:20", "000004", "LLM_DECISION", action="BUY_NOW", trigger="MA120_CROSS_UP", price=10.0, llm_status="ok"),
            _event(day, "10:20", "000004", "ORDER_SUBMIT", price=10.0, order={"order_price": 10.15, "quantity": 1000, "status": "submitted"}),
        ]
        _write_jsonl(out / f"intraday_buy_events_{key}.jsonl", rows)
        rising = [10.0 + idx * 0.03 for idx in range(80)]
        _write_market(out / f"intraday_buy_market_{key}.json.gz", day, {
            "000001": {"bars": _bars(day, 10.0, [10.0, 10.1, 10.2, 10.3])},
            "000002": {"bars": _bars(day, 10.0, rising)},
            "000003": {"bars": _bars(day, 10.0, [10.0, 9.9, 9.8, 9.7])},
            "000004": {"bars": _bars(day, 10.0, rising)},
        })

        review = review_mod.build_intraday_buy_weekly_review(
            output_dir=out,
            selection_items=[],
            now=datetime(2026, 7, 18, 8, 0),
        )
        records = {row["stock"]: row for row in review["records"]}
        assert review["summary"]["full_quality_count"] == 4
        assert "OPENING_CHASE_GOOD" in records["000001"]["attribution_labels"]
        assert "WAITED_TOO_LONG" in records["000002"]["attribution_labels"]
        assert "LLM_DECISION_FAILED" in records["000003"]["attribution_labels"]
        assert "ORDER_NOT_FILLED" in records["000004"]["attribution_labels"]
        assert review["summary"]["rule_change_gate"]["automatic_change_allowed"] is False
        assert "records" not in review_mod.compact_intraday_review_for_llm(review)

        assert review_mod.update_intraday_policy_memory(review, out) == 1
        assert review_mod.update_intraday_policy_memory(review, out) == 1
        memory_rows = (out / "intraday_buy_policy_memory.jsonl").read_text(encoding="utf-8").splitlines()
        assert len(memory_rows) == 1


def test_legacy_state_is_partial_and_future_maturity_is_preserved():
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "output"
        day = "2026-07-10"
        _write_json(out / "intraday_buy_timing_20260710.json", {
            "date": day,
            "finished_at": f"{day}T14:57:00",
            "selected_stocks": ["000001"],
            "selected_signals": [{"stock": "000001", "name": "旧样本"}],
            "stocks": {"000001": {"status": "open", "last_decision": {"time": f"{day}T14:50:00", "action": "WAIT"}}},
        })
        review = review_mod.build_intraday_buy_weekly_review(
            output_dir=out,
            selection_items=[{
                "date": day,
                "stock": "000001",
                "future_returns_pct": {"d1": 2.0, "d3": 5.0, "d5": 8.0},
                "future_return_complete": {"d1": True, "d3": False, "d5": False},
            }],
            now=datetime(2026, 7, 11, 8, 0),
        )
        record = review["records"][0]
        assert record["evidence_quality"] == review_mod.PARTIAL_QUALITY
        assert record["primary_attribution"] == "DATA_QUALITY_ISSUE"
        assert record["future_returns"]["d1"] == 2.0
        assert record["future_returns"]["d3"] is None
        assert record["future_returns"]["d5"] is None


def test_unfinished_day_is_unavailable_and_partial_samples_do_not_enter_policy_comparison():
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "output"
        for day, finished, filled_price in (
            ("2026-07-13", True, None),
            ("2026-07-14", False, 10.0),
        ):
            key = day.replace("-", "")
            state = {
                "date": day,
                "selected_stocks": ["000001"],
                "selected_signals": [{"stock": "000001", "name": "样本"}],
                "stocks": {
                    "000001": {
                        "status": "filled" if filled_price else "open",
                        "filled_price": filled_price,
                        "filled_at": f"{day}T09:31:30" if filled_price else None,
                    }
                },
            }
            if finished:
                state["finished_at"] = f"{day}T14:57:00"
            _write_json(out / f"intraday_buy_timing_{key}.json", state)
            _write_jsonl(out / f"intraday_buy_events_{key}.jsonl", [
                _event(day, "09:31", "000001", "RULE_DECISION", action="BUY_NOW", trigger="OPENING_STRONG", price=10.0),
            ])
            _write_market(out / f"intraday_buy_market_{key}.json.gz", day, {
                "000001": {"bars": _bars(day, 10.0, [10.0, 10.1])},
            })

        review = review_mod.build_intraday_buy_weekly_review(
            output_dir=out,
            selection_items=[],
            now=datetime(2026, 7, 14, 12, 0),
        )
        summary = review["summary"]
        records = {row["date"]: row for row in review["records"]}
        assert summary["trading_day_count"] == 2
        assert summary["completed_task_day_count"] == 1
        assert summary["task_availability_pct"] == 50.0
        assert records["2026-07-13"]["evidence_quality"] == review_mod.FULL_QUALITY
        assert records["2026-07-14"]["evidence_quality"] == review_mod.PARTIAL_QUALITY
        assert summary["policy_comparison"]["current_actual"]["evaluated_count"] == 1


def test_missing_task_state_is_detected_from_selection_items():
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "output"
        day = "2026-07-13"
        review = review_mod.build_intraday_buy_weekly_review(
            output_dir=out,
            selection_items=[{
                "date": day,
                "stock": "000001",
                "name": "未启动样本",
                "signal": "BUY",
                "confidence": 70,
                "buy_score": 75,
            }],
            now=datetime(2026, 7, 18, 8, 0),
        )
        record = review["records"][0]
        assert review["summary"]["trading_day_count"] == 1
        assert review["summary"]["completed_task_day_count"] == 0
        assert review["summary"]["task_availability_pct"] == 0.0
        assert record["evidence_quality"] == review_mod.INVALID_QUALITY
        assert record["primary_attribution"] == "PROCESS_FAILURE"


def test_intraday_executor_writes_audit_event_and_market_package():
    with tempfile.TemporaryDirectory() as td:
        original_output = executor.OUTPUT_DIR
        original_buffer = executor._BUY_TIMING_MARKET_BUFFER
        original_last_flush = executor._BUY_TIMING_MARKET_LAST_FLUSH
        try:
            executor.OUTPUT_DIR = Path(td) / "output"
            executor._BUY_TIMING_MARKET_BUFFER = {}
            executor._BUY_TIMING_MARKET_LAST_FLUSH = 0.0
            now = datetime(2026, 7, 13, 10, 0)
            entry = {"stock": "000001", "status": "open"}
            executor._record_buy_timing_decision(entry, {
                "action": "WAIT",
                "reason": "测试",
                "quote_price": 10.0,
                "_llm_model": "test-model",
                "_llm_path": "test",
                "_llm_status": "ok",
                "market_snapshot": {"price": 10.0, "ma120": 9.9},
            }, now)
            event_path = executor.OUTPUT_DIR / "intraday_buy_events_20260713.jsonl"
            event = json.loads(event_path.read_text(encoding="utf-8").splitlines()[0])
            assert event["event_type"] == "LLM_DECISION"
            assert event["decision"]["llm_status"] == "ok"
            assert event["market"]["ma120"] == 9.9

            executor._cache_buy_timing_market("000001", [{
                "time": now,
                "open": 10.0,
                "high": 10.1,
                "low": 9.9,
                "close": 10.05,
                "volume": 100,
            }], {"price": 10.05}, now)
            executor._flush_buy_timing_market(now.date(), force=True)
            market_path = executor.OUTPUT_DIR / "intraday_buy_market_20260713.json.gz"
            with gzip.open(market_path, "rt", encoding="utf-8") as handle:
                market = json.load(handle)
            assert len(market["stocks"]["000001"]["bars"]) == 1
        finally:
            executor.OUTPUT_DIR = original_output
            executor._BUY_TIMING_MARKET_BUFFER = original_buffer
            executor._BUY_TIMING_MARKET_LAST_FLUSH = original_last_flush
