#!/usr/bin/env python3
"""Offline regression checks for money-flow robustness improvements."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path


def test_pool_seed_fallback():
    from stock_selection_debate.run_debate_phase import _apply_pool_money_flow_seed

    packet = {
        "money_flow": {
            "main_net_flow": None,
            "super_net_flow": None,
            "ddx_5": None,
            "ddy_10": None,
            "source": "qmt+mx",
        },
        "data_quality_flags": ["MONEY_FLOW_MISSING", "MONEY_FLOW_FETCH_FAILED"],
    }
    candidate = {"pool_score_detail": {"main_flow_value": 157000000}}
    _apply_pool_money_flow_seed(packet, candidate)

    assert packet["money_flow"]["main_net_flow"] == 1.57
    assert packet["money_flow"]["source"] == "qmt+mx+pool_seed"
    assert "MONEY_FLOW_MISSING" not in packet["data_quality_flags"]
    assert "MONEY_FLOW_FETCH_FAILED" not in packet["data_quality_flags"]
    assert "MONEY_FLOW_PARTIAL" in packet["data_quality_flags"]


def test_pool_seed_no_override_existing_main():
    from stock_selection_debate.run_debate_phase import _apply_pool_money_flow_seed

    packet = {
        "money_flow": {
            "main_net_flow": 2.22,
            "super_net_flow": None,
            "ddx_5": None,
            "ddy_10": None,
            "source": "qmt",
        },
        "data_quality_flags": ["MONEY_FLOW_PARTIAL"],
    }
    candidate = {"pool_score_detail": {"main_flow_value": 157000000}}
    _apply_pool_money_flow_seed(packet, candidate)

    assert packet["money_flow"]["main_net_flow"] == 2.22
    assert packet["money_flow"]["source"] == "qmt"
    assert packet["data_quality_flags"] == ["MONEY_FLOW_PARTIAL"]


def test_data_quality_summary_source_counts():
    from stock_selection_debate.run_debate_phase import _summarize_data_quality

    rows = [
        {
            "stock": "000001",
            "name": "A",
            "money_flow": {"source": "qmt+mx"},
            "data_quality_flags": ["MONEY_FLOW_PARTIAL"],
        },
        {
            "stock": "000002",
            "name": "B",
            "money_flow": {"source": "qmt+mx+ak"},
            "data_quality_flags": ["MONEY_FLOW_FETCH_FAILED"],
        },
        {
            "stock": "000003",
            "name": "C",
            "money_flow": {"source": "pool_seed"},
            "data_quality_flags": [],
        },
    ]
    s = _summarize_data_quality(rows)
    assert s["money_flow_source_counts"]["qmt+mx"] == 1
    assert s["money_flow_source_counts"]["qmt+mx+ak"] == 1
    assert s["money_flow_source_counts"]["pool_seed"] == 1
    assert s["core_flag_counts"]["MONEY_FLOW_FETCH_FAILED"] == 1


def test_quality_script_includes_fetch_failed():
    from check_money_flow_quality import summarize

    sample = {
        "phase2": {
            "ranked_candidates": [{"stock": "000001"}] * 10,
            "data_quality_summary": {
                "flag_counts": {
                    "MONEY_FLOW_MISSING": 2,
                    "MONEY_FLOW_PARTIAL": 3,
                    "MONEY_FLOW_FETCH_FAILED": 1,
                }
            },
        }
    }
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "daily_report_20990101.json"
        p.write_text(json.dumps(sample, ensure_ascii=False), encoding="utf-8")
        row = summarize(p)
    assert row["missing"] == 2
    assert row["partial"] == 3
    assert row["fetch_failed"] == 1
    assert abs(row["ratio"] - 0.5) < 1e-9


def test_retry_main_net_flow_once_uses_rank_fallback():
    import stock_selection_debate.data_fetcher as df

    old_mx = df._fetch_money_flow_via_mx
    old_em = df._fetch_money_flow_via_eastmoney_direct
    old_rank = df._fetch_money_flow_via_akshare_rank
    old_sleep = df.time.sleep
    try:
        df._fetch_money_flow_via_mx = lambda _s: {}
        df._fetch_money_flow_via_eastmoney_direct = lambda _s: {}
        df._fetch_money_flow_via_akshare_rank = lambda _s: {"main_net_flow": 0.88}
        df.time.sleep = lambda _x: None
        flow, src = df._retry_main_net_flow_once("000001")
        assert flow.get("main_net_flow") == 0.88
        assert "ak_rank_retry" in src
    finally:
        df._fetch_money_flow_via_mx = old_mx
        df._fetch_money_flow_via_eastmoney_direct = old_em
        df._fetch_money_flow_via_akshare_rank = old_rank
        df.time.sleep = old_sleep


def test_eastmoney_direct_parse_and_aggregate():
    import stock_selection_debate.data_fetcher as df

    old_retry = df.retry_call
    try:
        payload = {
            "data": {
                "klines": [
                    "2026-05-26,10000000,0,0,0,3000000,0,0,0,0,0,0,0,0,0",
                    "2026-05-27,-5000000,0,0,0,-1000000,0,0,0,0,0,0,0,0,0",
                    "2026-05-28,2000000,0,0,0,800000,0,0,0,0,0,0,0,0,0",
                ]
            }
        }
        df.retry_call = lambda *_a, **_k: payload
        flow = df._fetch_money_flow_via_eastmoney_direct("000001")
        assert round(flow["main_net_flow"], 2) == 0.02
        assert round(flow["super_net_flow"], 3) == 0.008
        assert round(flow["main_net_flow_5d"], 2) == 0.07
        assert round(flow["main_net_flow_10d"], 2) == 0.07
        assert flow["source"] == "eastmoney"
        assert flow["as_of"] == "20260528"
    finally:
        df.retry_call = old_retry


def test_prefetch_pool_seed_fills_main_flow():
    import stock_selection_debate.data_fetcher as df

    old_load = df._load_debate_data_cache
    old_save = df._save_debate_data_cache
    old_mx = df._fetch_money_flow_via_mx
    old_em = df._fetch_money_flow_via_eastmoney_direct
    old_ak = df._fetch_money_flow_via_akshare
    old_rank = df._fetch_money_flow_via_akshare_rank
    old_mx_news = df._fetch_news_via_mxsearch
    old_sleep = df.time.sleep

    cache_holder = {}
    try:
        df._load_debate_data_cache = lambda: dict(cache_holder)
        df._save_debate_data_cache = lambda data: (cache_holder.clear(), cache_holder.update(data))
        df._fetch_money_flow_via_mx = lambda _s: {}
        df._fetch_money_flow_via_eastmoney_direct = lambda _s: {}
        df._fetch_money_flow_via_akshare = lambda _s: {}
        df._fetch_money_flow_via_akshare_rank = lambda _s: {}
        df._fetch_news_via_mxsearch = lambda *_a, **_k: []
        df.time.sleep = lambda _x: None

        candidates = [{
            "stock": "600919",
            "name": "江苏银行",
            "pool_score_detail": {"main_flow_value": 157000000},
        }]
        df._prefetch_debate_data(candidates)
        mf = cache_holder["600919"]["money_flow"]
        assert mf["main_net_flow"] == 1.57
    finally:
        df._load_debate_data_cache = old_load
        df._save_debate_data_cache = old_save
        df._fetch_money_flow_via_mx = old_mx
        df._fetch_money_flow_via_eastmoney_direct = old_em
        df._fetch_money_flow_via_akshare = old_ak
        df._fetch_money_flow_via_akshare_rank = old_rank
        df._fetch_news_via_mxsearch = old_mx_news
        df.time.sleep = old_sleep


def test_prefetch_pool_seed_handles_non_padded_stock_code():
    import stock_selection_debate.data_fetcher as df

    old_load = df._load_debate_data_cache
    old_save = df._save_debate_data_cache
    old_mx = df._fetch_money_flow_via_mx
    old_em = df._fetch_money_flow_via_eastmoney_direct
    old_ak = df._fetch_money_flow_via_akshare
    old_rank = df._fetch_money_flow_via_akshare_rank
    old_mx_news = df._fetch_news_via_mxsearch
    old_sleep = df.time.sleep

    cache_holder = {}
    try:
        df._load_debate_data_cache = lambda: dict(cache_holder)
        df._save_debate_data_cache = lambda data: (cache_holder.clear(), cache_holder.update(data))
        df._fetch_money_flow_via_mx = lambda _s: {}
        df._fetch_money_flow_via_eastmoney_direct = lambda _s: {}
        df._fetch_money_flow_via_akshare = lambda _s: {}
        df._fetch_money_flow_via_akshare_rank = lambda _s: {}
        df._fetch_news_via_mxsearch = lambda *_a, **_k: []
        df.time.sleep = lambda _x: None

        candidates = [{
            "stock": "958",
            "name": "电投产融",
            "pool_score_detail": {"main_flow_value": 7917300},
        }]
        df._prefetch_debate_data(candidates)
        mf = cache_holder["958"]["money_flow"]
        assert round(mf["main_net_flow"], 4) == 0.0792
    finally:
        df._load_debate_data_cache = old_load
        df._save_debate_data_cache = old_save
        df._fetch_money_flow_via_mx = old_mx
        df._fetch_money_flow_via_eastmoney_direct = old_em
        df._fetch_money_flow_via_akshare = old_ak
        df._fetch_money_flow_via_akshare_rank = old_rank
        df._fetch_news_via_mxsearch = old_mx_news
        df.time.sleep = old_sleep


def test_eastmoney_circuit_breaker_skips_retries_during_cooldown():
    import stock_selection_debate.data_fetcher as df

    old_retry = df.retry_call
    old_threshold = __import__("os").environ.get("EASTMONEY_FLOW_FAIL_THRESHOLD")
    old_cooldown = __import__("os").environ.get("EASTMONEY_FLOW_COOLDOWN_SEC")
    old_streak = df._EASTMONEY_FAIL_STREAK
    old_until = df._EASTMONEY_DISABLED_UNTIL
    try:
        __import__("os").environ["EASTMONEY_FLOW_FAIL_THRESHOLD"] = "1"
        __import__("os").environ["EASTMONEY_FLOW_COOLDOWN_SEC"] = "60"

        calls = {"n": 0}

        def _boom(*_a, **_k):
            calls["n"] += 1
            raise RuntimeError("boom")

        df.retry_call = _boom
        df._EASTMONEY_FAIL_STREAK = 0
        df._EASTMONEY_DISABLED_UNTIL = 0.0

        r1 = df._fetch_money_flow_via_eastmoney_direct("000001")
        assert r1 == {}
        first_calls = calls["n"]
        assert first_calls > 0

        # 冷却期内第二次调用应直接返回，不再触发 retry_call。
        r2 = df._fetch_money_flow_via_eastmoney_direct("000001")
        assert r2 == {}
        assert calls["n"] == first_calls
    finally:
        df.retry_call = old_retry
        if old_threshold is None:
            __import__("os").environ.pop("EASTMONEY_FLOW_FAIL_THRESHOLD", None)
        else:
            __import__("os").environ["EASTMONEY_FLOW_FAIL_THRESHOLD"] = old_threshold
        if old_cooldown is None:
            __import__("os").environ.pop("EASTMONEY_FLOW_COOLDOWN_SEC", None)
        else:
            __import__("os").environ["EASTMONEY_FLOW_COOLDOWN_SEC"] = old_cooldown
        df._EASTMONEY_FAIL_STREAK = old_streak
        df._EASTMONEY_DISABLED_UNTIL = old_until


def test_money_flow_rank_cache_only_uses_local_fallback():
    import stock_selection_debate.data_fetcher as df

    old_cache_dir = df.CACHE_DIR
    old_env = os.environ.get("MONEY_FLOW_RANK_CACHE_ONLY")
    old_rank_cache = dict(df._MONEY_FLOW_RANK_CACHE)
    try:
        with tempfile.TemporaryDirectory() as td:
            df.CACHE_DIR = Path(td)
            (df.CACHE_DIR / "money_flow_rank_20990101_今日.json").write_text(
                json.dumps({"000001": {"main_net_flow": 1.23}}, ensure_ascii=False),
                encoding="utf-8",
            )
            os.environ["MONEY_FLOW_RANK_CACHE_ONLY"] = "1"
            df._MONEY_FLOW_RANK_CACHE.clear()

            rank_map = df._get_money_flow_rank_map("今日")
            assert rank_map["000001"]["main_net_flow"] == 1.23
    finally:
        df.CACHE_DIR = old_cache_dir
        df._MONEY_FLOW_RANK_CACHE.clear()
        df._MONEY_FLOW_RANK_CACHE.update(old_rank_cache)
        if old_env is None:
            os.environ.pop("MONEY_FLOW_RANK_CACHE_ONLY", None)
        else:
            os.environ["MONEY_FLOW_RANK_CACHE_ONLY"] = old_env


def test_qmt_money_flow_disabled_by_default():
    import stock_selection_debate.data_fetcher as df

    # QMT HTTP 资金流端点已确认不可用，当前设计是保留空占位，
    # 后续由 mx/eastmoney/ak/rank 字段级补全。
    assert not hasattr(df, "_fetch_money_flow_batch_via_qmt_http")
    merged = df._merge_money_flow({}, {})
    for key in ("main_net_flow", "super_net_flow", "ddx_5", "ddy_10", "main_net_flow_5d", "main_net_flow_10d"):
        assert merged[key] is None
    assert merged["source"] == "none"
    assert merged["field_sources"] == {}


def test_mx_money_flow_empty_response_is_safe():
    import sys
    import types
    import stock_selection_debate.data_fetcher as df

    old_module = sys.modules.get("mx_data")
    old_streak = df._MX_MONEY_FLOW_FAIL_STREAK
    old_until = df._MX_MONEY_FLOW_DISABLED_UNTIL
    old_threshold = os.environ.get("MX_MONEY_FLOW_FAIL_THRESHOLD")
    try:
        class FakeMXData:
            def __init__(self, *_a, **_k):
                pass

            def query(self, *_a, **_k):
                return None

            def parse_result(self, result):
                return result.get("data")

        sys.modules["mx_data"] = types.SimpleNamespace(MXData=FakeMXData)
        os.environ["MX_MONEY_FLOW_FAIL_THRESHOLD"] = "99"
        df._MX_MONEY_FLOW_FAIL_STREAK = 0
        df._MX_MONEY_FLOW_DISABLED_UNTIL = 0.0

        assert df._fetch_money_flow_via_mx("000001") == {}
    finally:
        if old_module is None:
            sys.modules.pop("mx_data", None)
        else:
            sys.modules["mx_data"] = old_module
        df._MX_MONEY_FLOW_FAIL_STREAK = old_streak
        df._MX_MONEY_FLOW_DISABLED_UNTIL = old_until
        if old_threshold is None:
            os.environ.pop("MX_MONEY_FLOW_FAIL_THRESHOLD", None)
        else:
            os.environ["MX_MONEY_FLOW_FAIL_THRESHOLD"] = old_threshold


def test_mx_money_flow_parses_ah_symbol_columns():
    import sys
    import types
    import stock_selection_debate.data_fetcher as df

    old_module = sys.modules.get("mx_data")
    old_streak = df._MX_MONEY_FLOW_FAIL_STREAK
    old_until = df._MX_MONEY_FLOW_DISABLED_UNTIL
    queries = []
    try:
        class FakeMXData:
            def __init__(self, *_a, **_k):
                pass

            def query(self, query):
                queries.append(query)
                return {"query": query}

            def parse_result(self, result):
                query = result["query"]
                if "DDX" in query:
                    return ([{
                        "rows": [{
                            "date": "2026-06-01 01:47",
                            "5日DDX": "0.067",
                            "10日DDY": "0.189",
                        }]
                    }], [], 1, None)
                if "超大单" in query:
                    return ([{
                        "rows": [{
                            "date": "2026-05-29(日)",
                            "浙商银行(02016.HK)": "159.3万港元",
                            "浙商银行(601916.SH)": "100万元",
                        }]
                    }], [], 1, None)
                return ([{
                    "rows": [{
                        "date": "2026-05-29(日)",
                        "浙商银行(02016.HK)": "159.3万港元",
                        "浙商银行(601916.SH)": "3636万元",
                    }]
                }], [], 1, None)

        sys.modules["mx_data"] = types.SimpleNamespace(MXData=FakeMXData)
        df._MX_MONEY_FLOW_FAIL_STREAK = 0
        df._MX_MONEY_FLOW_DISABLED_UNTIL = 0.0

        flow = df._fetch_money_flow_via_mx("601916")
        assert flow["main_net_flow"] == 0.3636
        assert flow["super_net_flow"] == 0.01
        assert flow["ddx_5"] == 0.067
        assert flow["ddy_10"] == 0.189
        assert queries[0].startswith("601916 ")
    finally:
        if old_module is None:
            sys.modules.pop("mx_data", None)
        else:
            sys.modules["mx_data"] = old_module
        df._MX_MONEY_FLOW_FAIL_STREAK = old_streak
        df._MX_MONEY_FLOW_DISABLED_UNTIL = old_until


def test_mx_money_flow_rate_limit_does_not_circuit_break():
    import sys
    import types
    import stock_selection_debate.data_fetcher as df

    old_module = sys.modules.get("mx_data")
    old_streak = df._MX_MONEY_FLOW_FAIL_STREAK
    old_until = df._MX_MONEY_FLOW_DISABLED_UNTIL
    old_last_query = df._MX_MONEY_FLOW_LAST_QUERY_AT
    old_cooldown = os.environ.get("MX_MONEY_FLOW_RATE_LIMIT_COOLDOWN_SEC")
    old_pause = os.environ.get("MX_MONEY_FLOW_112_PAUSE_SEC")
    old_retries = os.environ.get("MX_MONEY_FLOW_112_RETRIES")
    old_delays = os.environ.get("MX_MONEY_FLOW_112_RETRY_DELAYS_SEC")
    old_interval = os.environ.get("MX_MONEY_FLOW_QUERY_INTERVAL_SEC")
    try:
        class FakeMXData:
            def __init__(self, *_a, **_k):
                pass

            def query(self, query):
                return {"query": query}

            def parse_result(self, result):
                return [], [], 0, "顶层错误: 状态码 112 - 请求频率过高，请稍后再试"

        sys.modules["mx_data"] = types.SimpleNamespace(MXData=FakeMXData)
        os.environ["MX_MONEY_FLOW_RATE_LIMIT_COOLDOWN_SEC"] = "60"
        os.environ["MX_MONEY_FLOW_112_PAUSE_SEC"] = "0"
        os.environ["MX_MONEY_FLOW_112_RETRIES"] = "2"
        os.environ["MX_MONEY_FLOW_112_RETRY_DELAYS_SEC"] = "0,0"
        os.environ["MX_MONEY_FLOW_QUERY_INTERVAL_SEC"] = "0"
        df._MX_MONEY_FLOW_FAIL_STREAK = 0
        df._MX_MONEY_FLOW_DISABLED_UNTIL = 0.0
        df._MX_MONEY_FLOW_LAST_QUERY_AT = 0.0

        flow = df._fetch_money_flow_via_mx("600063")
        assert flow["main_net_flow"] is None
        assert df._MX_MONEY_FLOW_DISABLED_UNTIL == 0.0
        assert df._MX_MONEY_FLOW_FAIL_STREAK == 0
    finally:
        if old_module is None:
            sys.modules.pop("mx_data", None)
        else:
            sys.modules["mx_data"] = old_module
        df._MX_MONEY_FLOW_FAIL_STREAK = old_streak
        df._MX_MONEY_FLOW_DISABLED_UNTIL = old_until
        df._MX_MONEY_FLOW_LAST_QUERY_AT = old_last_query
        if old_cooldown is None:
            os.environ.pop("MX_MONEY_FLOW_RATE_LIMIT_COOLDOWN_SEC", None)
        else:
            os.environ["MX_MONEY_FLOW_RATE_LIMIT_COOLDOWN_SEC"] = old_cooldown
        if old_pause is None:
            os.environ.pop("MX_MONEY_FLOW_112_PAUSE_SEC", None)
        else:
            os.environ["MX_MONEY_FLOW_112_PAUSE_SEC"] = old_pause
        if old_retries is None:
            os.environ.pop("MX_MONEY_FLOW_112_RETRIES", None)
        else:
            os.environ["MX_MONEY_FLOW_112_RETRIES"] = old_retries
        if old_delays is None:
            os.environ.pop("MX_MONEY_FLOW_112_RETRY_DELAYS_SEC", None)
        else:
            os.environ["MX_MONEY_FLOW_112_RETRY_DELAYS_SEC"] = old_delays
        if old_interval is None:
            os.environ.pop("MX_MONEY_FLOW_QUERY_INTERVAL_SEC", None)
        else:
            os.environ["MX_MONEY_FLOW_QUERY_INTERVAL_SEC"] = old_interval


def test_mx_money_flow_112_retries_then_success():
    import sys
    import types
    import stock_selection_debate.data_fetcher as df

    old_module = sys.modules.get("mx_data")
    old_streak = df._MX_MONEY_FLOW_FAIL_STREAK
    old_until = df._MX_MONEY_FLOW_DISABLED_UNTIL
    old_last_query = df._MX_MONEY_FLOW_LAST_QUERY_AT
    old_retries = os.environ.get("MX_MONEY_FLOW_112_RETRIES")
    old_delays = os.environ.get("MX_MONEY_FLOW_112_RETRY_DELAYS_SEC")
    old_interval = os.environ.get("MX_MONEY_FLOW_QUERY_INTERVAL_SEC")
    try:
        class FakeMXData:
            parse_calls = 0

            def __init__(self, *_a, **_k):
                pass

            def query(self, query):
                return {"query": query}

            def parse_result(self, result):
                FakeMXData.parse_calls += 1
                if FakeMXData.parse_calls <= 2:
                    return [], [], 0, "顶层错误: 状态码 112 - 请求频率过高，请稍后再试"
                query = result.get("query", "")
                if "DDX" in query:
                    return [[{"rows": [{"5日DDX": "0.123", "10日DDY": "0.456"}]}], [], 0, None]
                return [[{"rows": [{"主力净流入资金": "1.23亿元", "超大单净额": "0.45亿元"}]}], [], 0, None]

        sys.modules["mx_data"] = types.SimpleNamespace(MXData=FakeMXData)
        os.environ["MX_MONEY_FLOW_112_RETRIES"] = "3"
        os.environ["MX_MONEY_FLOW_112_RETRY_DELAYS_SEC"] = "0,0,0"
        os.environ["MX_MONEY_FLOW_QUERY_INTERVAL_SEC"] = "0"
        df._MX_MONEY_FLOW_FAIL_STREAK = 0
        df._MX_MONEY_FLOW_DISABLED_UNTIL = 0.0
        df._MX_MONEY_FLOW_LAST_QUERY_AT = 0.0

        flow = df._fetch_money_flow_via_mx("600063")
        assert flow["main_net_flow"] == 1.23
        assert flow["super_net_flow"] == 0.45
        assert flow["ddx_5"] == 0.123
        assert flow["ddy_10"] == 0.456
        assert FakeMXData.parse_calls == 4
        assert df._MX_MONEY_FLOW_DISABLED_UNTIL == 0.0
        assert df._MX_MONEY_FLOW_FAIL_STREAK == 0
    finally:
        if old_module is None:
            sys.modules.pop("mx_data", None)
        else:
            sys.modules["mx_data"] = old_module
        df._MX_MONEY_FLOW_FAIL_STREAK = old_streak
        df._MX_MONEY_FLOW_DISABLED_UNTIL = old_until
        df._MX_MONEY_FLOW_LAST_QUERY_AT = old_last_query
        if old_retries is None:
            os.environ.pop("MX_MONEY_FLOW_112_RETRIES", None)
        else:
            os.environ["MX_MONEY_FLOW_112_RETRIES"] = old_retries
        if old_delays is None:
            os.environ.pop("MX_MONEY_FLOW_112_RETRY_DELAYS_SEC", None)
        else:
            os.environ["MX_MONEY_FLOW_112_RETRY_DELAYS_SEC"] = old_delays
        if old_interval is None:
            os.environ.pop("MX_MONEY_FLOW_QUERY_INTERVAL_SEC", None)
        else:
            os.environ["MX_MONEY_FLOW_QUERY_INTERVAL_SEC"] = old_interval


def test_mx_money_flow_daily_quota_circuit_breaker():
    import sys
    import types
    import stock_selection_debate.data_fetcher as df

    old_module = sys.modules.get("mx_data")
    old_streak = df._MX_MONEY_FLOW_FAIL_STREAK
    old_until = df._MX_MONEY_FLOW_DISABLED_UNTIL
    old_cooldown = os.environ.get("MX_MONEY_FLOW_RATE_LIMIT_COOLDOWN_SEC")
    try:
        class FakeMXData:
            def __init__(self, *_a, **_k):
                pass

            def query(self, query):
                return {"query": query}

            def parse_result(self, result):
                return [], [], 0, "顶层错误: 状态码 113 - 今日调用次数已达上线500次"

        sys.modules["mx_data"] = types.SimpleNamespace(MXData=FakeMXData)
        os.environ["MX_MONEY_FLOW_RATE_LIMIT_COOLDOWN_SEC"] = "60"
        df._MX_MONEY_FLOW_FAIL_STREAK = 0
        df._MX_MONEY_FLOW_DISABLED_UNTIL = 0.0

        assert df._fetch_money_flow_via_mx("600063") == {}
        assert df._MX_MONEY_FLOW_DISABLED_UNTIL > 0
    finally:
        if old_module is None:
            sys.modules.pop("mx_data", None)
        else:
            sys.modules["mx_data"] = old_module
        df._MX_MONEY_FLOW_FAIL_STREAK = old_streak
        df._MX_MONEY_FLOW_DISABLED_UNTIL = old_until
        if old_cooldown is None:
            os.environ.pop("MX_MONEY_FLOW_RATE_LIMIT_COOLDOWN_SEC", None)
        else:
            os.environ["MX_MONEY_FLOW_RATE_LIMIT_COOLDOWN_SEC"] = old_cooldown


def test_mx_money_flow_auth_error_is_explicit():
    import sys
    import types
    import stock_selection_debate.data_fetcher as df

    old_module = sys.modules.get("mx_data")
    old_streak = df._MX_MONEY_FLOW_FAIL_STREAK
    old_until = df._MX_MONEY_FLOW_DISABLED_UNTIL
    old_last_query = df._MX_MONEY_FLOW_LAST_QUERY_AT
    old_interval = os.environ.get("MX_MONEY_FLOW_QUERY_INTERVAL_SEC")
    try:
        class FakeMXData:
            def __init__(self, *_a, **_k):
                pass

            def query(self, query):
                return {"query": query}

            def parse_result(self, _result):
                return [], [], 0, "顶层错误: 状态码 114 - API密钥不存在或已失效"

        sys.modules["mx_data"] = types.SimpleNamespace(MXData=FakeMXData)
        os.environ["MX_MONEY_FLOW_QUERY_INTERVAL_SEC"] = "0"
        df._MX_MONEY_FLOW_FAIL_STREAK = 0
        df._MX_MONEY_FLOW_DISABLED_UNTIL = 0.0
        df._MX_MONEY_FLOW_LAST_QUERY_AT = 0.0

        flow = df._fetch_money_flow_via_mx("600000")
        assert flow["status"] == "auth_error"
        assert flow["source"] == "mx-data"
        assert flow["diagnostics"]["auth_error"] is True
        assert "114" in flow["error"]
    finally:
        if old_module is None:
            sys.modules.pop("mx_data", None)
        else:
            sys.modules["mx_data"] = old_module
        df._MX_MONEY_FLOW_FAIL_STREAK = old_streak
        df._MX_MONEY_FLOW_DISABLED_UNTIL = old_until
        df._MX_MONEY_FLOW_LAST_QUERY_AT = old_last_query
        if old_interval is None:
            os.environ.pop("MX_MONEY_FLOW_QUERY_INTERVAL_SEC", None)
        else:
            os.environ["MX_MONEY_FLOW_QUERY_INTERVAL_SEC"] = old_interval


def test_prefetch_can_disable_live_money_flow_sources():
    import stock_selection_debate.data_fetcher as df

    old_load = df._load_debate_data_cache
    old_save = df._save_debate_data_cache
    old_mx = df._fetch_money_flow_via_mx
    old_em = df._fetch_money_flow_via_eastmoney_direct
    old_ak = df._fetch_money_flow_via_akshare
    old_rank = df._fetch_money_flow_via_akshare_rank
    old_mx_news = df._fetch_news_via_mxsearch
    old_sleep = df.time.sleep
    env_keys = [
        "ENABLE_MX_MONEY_FLOW_PREFETCH",
        "ENABLE_EASTMONEY_FLOW_PREFETCH",
        "ENABLE_AKSHARE_INDIVIDUAL_FLOW_PREFETCH",
    ]
    old_env = {k: os.environ.get(k) for k in env_keys}

    cache_holder = {}
    try:
        for key in env_keys:
            os.environ.pop(key, None)
        os.environ["ENABLE_MX_MONEY_FLOW_PREFETCH"] = "0"
        df._load_debate_data_cache = lambda: dict(cache_holder)
        df._save_debate_data_cache = lambda data: (cache_holder.clear(), cache_holder.update(data))
        df._fetch_money_flow_via_mx = lambda _s: (_ for _ in ()).throw(AssertionError("mx should be skipped"))
        df._fetch_money_flow_via_eastmoney_direct = lambda _s: (_ for _ in ()).throw(AssertionError("eastmoney should be skipped"))
        df._fetch_money_flow_via_akshare = lambda _s: (_ for _ in ()).throw(AssertionError("akshare should be skipped"))
        df._fetch_money_flow_via_akshare_rank = lambda _s: {}
        df._fetch_news_via_mxsearch = lambda *_a, **_k: []
        df.time.sleep = lambda _x: None

        df._prefetch_debate_data([{
            "stock": "600919",
            "name": "江苏银行",
            "pool_score_detail": {"main_flow_value": 157000000},
        }])
        assert cache_holder["600919"]["money_flow"]["main_net_flow"] == 1.57
    finally:
        df._load_debate_data_cache = old_load
        df._save_debate_data_cache = old_save
        df._fetch_money_flow_via_mx = old_mx
        df._fetch_money_flow_via_eastmoney_direct = old_em
        df._fetch_money_flow_via_akshare = old_ak
        df._fetch_money_flow_via_akshare_rank = old_rank
        df._fetch_news_via_mxsearch = old_mx_news
        df.time.sleep = old_sleep
        for key, value in old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_prefetch_default_uses_mx_money_flow_source():
    import stock_selection_debate.data_fetcher as df

    old_load = df._load_debate_data_cache
    old_save = df._save_debate_data_cache
    old_mx = df._fetch_money_flow_via_mx
    old_em = df._fetch_money_flow_via_eastmoney_direct
    old_ak = df._fetch_money_flow_via_akshare
    old_rank = df._fetch_money_flow_via_akshare_rank
    old_mx_news = df._fetch_news_via_mxsearch
    old_sleep = df.time.sleep
    env_keys = [
        "ENABLE_MX_MONEY_FLOW_PREFETCH",
        "ENABLE_EASTMONEY_FLOW_PREFETCH",
        "ENABLE_AKSHARE_INDIVIDUAL_FLOW_PREFETCH",
    ]
    old_env = {k: os.environ.get(k) for k in env_keys}

    cache_holder = {}
    calls = {"mx": 0}
    try:
        for key in env_keys:
            os.environ.pop(key, None)
        df._load_debate_data_cache = lambda: dict(cache_holder)
        df._save_debate_data_cache = lambda data: (cache_holder.clear(), cache_holder.update(data))

        def _mx(_s):
            calls["mx"] += 1
            return {
                "main_net_flow": 1.23,
                "super_net_flow": 0.45,
                "ddx_5": 0.1,
                "ddy_10": 0.2,
            }

        df._fetch_money_flow_via_mx = _mx
        df._fetch_money_flow_via_eastmoney_direct = lambda _s: {}
        df._fetch_money_flow_via_akshare = lambda _s: {}
        df._fetch_money_flow_via_akshare_rank = lambda _s: {}
        df._fetch_news_via_mxsearch = lambda *_a, **_k: []
        df.time.sleep = lambda _x: None

        df._prefetch_debate_data([{"stock": "600919", "name": "江苏银行"}])
        assert calls["mx"] == 1
        assert cache_holder["600919"]["money_flow"]["main_net_flow"] == 1.23
    finally:
        df._load_debate_data_cache = old_load
        df._save_debate_data_cache = old_save
        df._fetch_money_flow_via_mx = old_mx
        df._fetch_money_flow_via_eastmoney_direct = old_em
        df._fetch_money_flow_via_akshare = old_ak
        df._fetch_money_flow_via_akshare_rank = old_rank
        df._fetch_news_via_mxsearch = old_mx_news
        df.time.sleep = old_sleep
        for key, value in old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def main():
    test_pool_seed_fallback()
    test_pool_seed_no_override_existing_main()
    test_data_quality_summary_source_counts()
    test_quality_script_includes_fetch_failed()
    test_retry_main_net_flow_once_uses_rank_fallback()
    test_eastmoney_direct_parse_and_aggregate()
    test_prefetch_pool_seed_fills_main_flow()
    test_prefetch_pool_seed_handles_non_padded_stock_code()
    test_eastmoney_circuit_breaker_skips_retries_during_cooldown()
    test_money_flow_rank_cache_only_uses_local_fallback()
    test_qmt_money_flow_disabled_by_default()
    test_mx_money_flow_empty_response_is_safe()
    test_mx_money_flow_parses_ah_symbol_columns()
    test_mx_money_flow_rate_limit_does_not_circuit_break()
    test_mx_money_flow_112_retries_then_success()
    test_mx_money_flow_daily_quota_circuit_breaker()
    test_mx_money_flow_auth_error_is_explicit()
    test_prefetch_can_disable_live_money_flow_sources()
    test_prefetch_default_uses_mx_money_flow_source()
    print("money-flow pipeline tests passed")


if __name__ == "__main__":
    main()
