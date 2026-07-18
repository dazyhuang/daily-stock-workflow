from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

import llm_scorer


def _previous_day_header(label: str) -> str:
    return f"{label}[{(date.today() - timedelta(days=1)).strftime('%Y.%m.%d')}]"


def test_six_pool_configs_are_explicit_and_causal():
    configs = llm_scorer.XUANGU_SCREEN_CONFIGS
    assert [cfg["pool"] for cfg in configs] == [
        "准备启动",
        "突破新高",
        "首板追击",
        "热点龙头",
        "强势反包",
        "资金异动",
    ]
    assert llm_scorer.XUANGU_POOL_RANKING_VERSION == "pool-rank-v3-two-stage"
    assert all("上个交易日" in cfg["query"] for cfg in configs)
    assert all(int(cfg["top_n"]) > 0 for cfg in configs)
    assert all(int(cfg["recall_n"]) >= int(cfg["top_n"]) for cfg in configs)
    assert all(int(cfg["min_final_n"]) > 0 for cfg in configs)

    hotspot = next(cfg for cfg in configs if cfg["pool"] == "热点龙头")
    assert hotspot["screen_mode"] == "local_hot_sector"
    assert hotspot["sector_count"] == 3
    assert hotspot["sector_breadth_min"] == 0.60
    assert "板块涨幅排名前3" not in hotspot["query"]


def test_previous_day_value_wins_over_live_intraday_value():
    row = {
        f"涨跌幅:前复权[{date.today().strftime('%Y.%m.%d')}]": "-5.00%",
        _previous_day_header("涨跌幅:前复权"): "3.25%",
    }
    value = llm_scorer._row_previous_numeric_value(row, [["涨跌幅", "%"], ["涨跌幅"]])
    assert value == 3.25


def test_hot_sector_builder_requires_breadth_and_stock_quality():
    board = pd.DataFrame(
        [
            {"label": "a", "板块": "强行业A", "涨跌幅": 4.0},
            {"label": "b", "板块": "弱广度B", "涨跌幅": 3.8},
            {"label": "c", "板块": "强行业C", "涨跌幅": 3.0},
            {"label": "d", "板块": "强行业D", "涨跌幅": 2.0},
        ]
    )

    details = {
        "a": pd.DataFrame(
            [
                {"code": "600001", "name": "合格A", "changepercent": 5.0, "turnoverratio": 6.0, "amount": 5e8, "trade": 9.90, "high": 10.00},
                {"code": "920001", "name": "北交所", "changepercent": 5.0, "turnoverratio": 6.0, "amount": 5e8, "trade": 9.90, "high": 10.00},
                {"code": "600002", "name": "上涨陪衬", "changepercent": 0.5, "turnoverratio": 2.0, "amount": 2e8, "trade": 9.90, "high": 10.00},
            ]
        ),
        "b": pd.DataFrame(
            [
                {"code": "600010", "name": "上涨B", "changepercent": 3.0, "turnoverratio": 3.0, "amount": 3e8, "trade": 9.90, "high": 10.00},
                {"code": "600011", "name": "下跌B1", "changepercent": -1.0, "turnoverratio": 3.0, "amount": 3e8, "trade": 9.90, "high": 10.00},
                {"code": "600012", "name": "下跌B2", "changepercent": -2.0, "turnoverratio": 3.0, "amount": 3e8, "trade": 9.90, "high": 10.00},
            ]
        ),
        "c": pd.DataFrame(
            [
                {"code": "300001", "name": "合格C", "changepercent": 10.0, "turnoverratio": 8.0, "amount": 6e8, "trade": 19.60, "high": 20.00},
                {"code": "300002", "name": "低成交额", "changepercent": 4.0, "turnoverratio": 5.0, "amount": 0.5e8, "trade": 9.90, "high": 10.00},
                {"code": "300003", "name": "上涨陪衬C", "changepercent": 1.0, "turnoverratio": 5.0, "amount": 2e8, "trade": 9.90, "high": 10.00},
            ]
        ),
        "d": pd.DataFrame(
            [
                {"code": "600020", "name": "合格D", "changepercent": 3.0, "turnoverratio": 4.0, "amount": 4e8, "trade": 9.80, "high": 10.00},
                {"code": "600021", "name": "ST样本", "changepercent": 3.0, "turnoverratio": 4.0, "amount": 4e8, "trade": 9.80, "high": 10.00},
                {"code": "600022", "name": "上涨陪衬D", "changepercent": 0.2, "turnoverratio": 4.0, "amount": 2e8, "trade": 9.80, "high": 10.00},
            ]
        ),
    }
    cfg = next(cfg for cfg in llm_scorer.XUANGU_SCREEN_CONFIGS if cfg["pool"] == "热点龙头")
    candidates, stats = llm_scorer._build_hot_sector_candidates(board, details.__getitem__, cfg)

    assert [sector["name"] for sector in stats["sectors"]] == ["强行业A", "强行业C", "强行业D"]
    assert {item["stock"] for item in candidates} == {"600001", "300001", "600020"}
    assert all(item["source"] == "akshare_sector_local" for item in candidates)
    assert all(item["sector_strength"]["breadth_pct"] >= 60 for item in candidates)


def test_each_pool_score_exposes_strategy_specific_evidence():
    previous = (date.today() - timedelta(days=1)).strftime("%Y.%m.%d")
    common = {
        "股票代码": "600001",
        "股票名称": "测试股",
        f"涨跌幅:前复权(%)[{previous}]": "3.20%",
        f"换手率(%)[{previous}]": "5.00%",
        f"成交额(元)[{previous}]": "5.00亿",
        f"量比[{previous}]": "1.80",
        "主力净额合计(元)": "1.50亿",
        f"超大单净额(元)[{previous}]": "5000万",
        f"收盘价:前复权(元)[{previous}]": "9.90",
        f"最高价:前复权(元)[{previous}]": "10.00",
    }
    expected_keys = {
        "准备启动": "ma_compression",
        "突破新高": "close_near_high",
        "首板追击": "board_liquidity",
        "热点龙头": "leader_liquidity",
        "强势反包": "reversal_pct",
        "资金异动": "super_flow",
    }
    for cfg in llm_scorer.XUANGU_SCREEN_CONFIGS:
        row = dict(common)
        if cfg["pool"] == "准备启动":
            row["5日均线与10日均线绝对值"] = "0.008"
            row["5日均线与20日均线绝对值"] = "0.010"
        score, detail = llm_scorer._score_xuangu_row(row, cfg)
        assert 0 <= score <= 100
        assert expected_keys[cfg["pool"]] in detail


def _synthetic_klines(last_day: str, *, limit_up: bool = False, start: float = 10.0):
    end = datetime.strptime(last_day, "%Y%m%d").date()
    rows = []
    for index in range(80):
        day = end - timedelta(days=79 - index)
        close = start * (1.0 + index * 0.001)
        rows.append({
            "date": day.strftime("%Y%m%d"),
            "open": close * 0.995,
            "high": close * 1.01,
            "low": close * 0.99,
            "close": close,
            "volume": 20_000_000 + index * 10_000,
            "amount": 300_000_000,
        })
    if limit_up:
        previous = rows[-2]["close"]
        close = previous * 1.10
        rows[-1].update({"open": previous * 1.02, "low": previous * 1.01, "high": close, "close": close})
    return rows


def _screening_stock_packet(last_day: str, *, limit_up: bool = False, listing_date: str = "20200101"):
    return {
        "klines": _synthetic_klines(last_day, limit_up=limit_up),
        "screening_metadata": {
            "listing_date": listing_date,
            "expire_date": "99991231",
            "sector": "电子",
        },
        "data_contract": {"kline": {"source": "synthetic"}},
    }


def test_local_screening_excludes_limit_up_from_non_first_board_pool():
    day = llm_scorer._last_completed_trading_day()
    market = llm_scorer._screening_market_context(
        {"benchmark": "000300.SH", "source": "synthetic", "klines": _synthetic_klines(day)},
        day,
    )
    packet = _screening_stock_packet(day, limit_up=True)
    features = llm_scorer._build_local_screening_features("600001", "测试股", packet, market, day)
    breakout = {"stock": "600001", "name": "测试股", "pool": "突破新高", "pool_score_detail": {}}
    first_board = dict(breakout, pool="首板追击")
    assert "LIMIT_UP_POOL_EXCLUSIVE" in llm_scorer._local_screen_rejections(breakout, features, day)
    assert "LIMIT_UP_POOL_EXCLUSIVE" not in llm_scorer._local_screen_rejections(first_board, features, day)


def test_local_screening_degrades_without_adequate_kline_coverage():
    cfg = [dict(llm_scorer.XUANGU_SCREEN_CONFIGS[0], top_n=2, min_final_n=1)]
    candidates = [
        {"stock": "600001", "name": "甲", "pool": "准备启动", "pool_score": 60, "pool_score_detail": {}},
        {"stock": "600002", "name": "乙", "pool": "准备启动", "pool_score": 55, "pool_score_detail": {}},
        {"stock": "600003", "name": "丙", "pool": "准备启动", "pool_score": 50, "pool_score_detail": {}},
    ]
    with TemporaryDirectory() as tmp:
        selected = llm_scorer._refine_screening_candidates(
            candidates,
            cfg,
            Path(tmp),
            screening_packet={"stocks": {}, "benchmark": {}, "stats": {"coverage": 0.3}},
        )
    assert [item["stock"] for item in selected] == ["600001", "600002"]
    assert all("LOCAL_SCREENING_DEGRADED" in item["data_quality_flags"] for item in selected)


def test_market_regime_changes_pool_quota_without_crossing_floor():
    breakout = next(cfg for cfg in llm_scorer.XUANGU_SCREEN_CONFIGS if cfg["pool"] == "突破新高")
    first_board = next(cfg for cfg in llm_scorer.XUANGU_SCREEN_CONFIGS if cfg["pool"] == "首板追击")
    assert llm_scorer._dynamic_pool_quota(breakout, {"regime": "strong"}) > breakout["top_n"]
    assert llm_scorer._dynamic_pool_quota(breakout, {"regime": "weak"}) >= breakout["min_final_n"]
    assert llm_scorer._dynamic_pool_quota(first_board, {"regime": "weak"}) >= first_board["min_final_n"]


def test_cross_pool_merge_keeps_primary_local_evidence_aligned():
    merged = llm_scorer._merge_candidate_sources([
        {
            "stock": "600001", "name": "测试股", "pool": "准备启动", "screen_id": "a",
            "strategy_type": "capital_absorption_dip", "entry_bias": "a", "priority": 30,
            "pool_score": 55, "pool_rank": 8, "pool_scored_candidates": 10,
            "pool_score_detail": {}, "screening_features": {"marker": "a"},
            "local_screening": {"marker": "a"}, "dynamic_pool_quota": 20,
        },
        {
            "stock": "600001", "name": "测试股", "pool": "突破新高", "screen_id": "b",
            "strategy_type": "momentum_breakout", "entry_bias": "b", "priority": 20,
            "pool_score": 82, "pool_rank": 1, "pool_scored_candidates": 10,
            "pool_score_detail": {}, "screening_features": {"marker": "b"},
            "local_screening": {"marker": "b"}, "dynamic_pool_quota": 23,
        },
    ])
    assert len(merged) == 1
    assert merged[0]["pool"] == "突破新高"
    assert merged[0]["screening_features"]["marker"] == "b"
    assert merged[0]["local_screening"]["marker"] == "b"
    assert merged[0]["dynamic_pool_quota"] == 23


def test_hot_sector_history_rewards_only_consecutive_previous_day():
    current = llm_scorer._last_completed_trading_day()
    previous = llm_scorer._previous_screening_trading_day(current)
    with TemporaryDirectory() as tmp:
        output_dir = Path(tmp)
        (output_dir / "hot_sector_history.json").write_text(json.dumps({
            "schema_version": llm_scorer.HOT_SECTOR_HISTORY_VERSION,
            "history": [{"as_of": previous, "sectors": ["电子"]}],
        }, ensure_ascii=False), encoding="utf-8")
        candidates, summary = llm_scorer._apply_hot_sector_history(
            [{"stock": "600001", "sector": "电子", "pool_score": 70, "amount": 3e8, "pool_score_detail": {}, "reason": "样本"}],
            {"sectors": [{"name": "电子"}]},
            output_dir,
            current,
        )
    assert candidates[0]["sector_persistence_days"] == 2
    assert candidates[0]["sector_history_bonus"] == 2.0
    assert candidates[0]["pool_score"] == 72.0
    assert summary["persistent_sectors"] == ["电子"]


def test_announcement_risk_penalty_is_evidence_bound_and_idempotent():
    from stock_selection_debate import data_fetcher

    original_loader = data_fetcher._load_debate_data_cache
    original_base_dir = llm_scorer.BASE_DIR
    today = date.today().strftime("%Y%m%d")
    data_fetcher._load_debate_data_cache = lambda: {
        "600001": {
            "updated": today,
            "news": [
                {"title": "测试股份股东减持计划", "content": "测试股份拟减持", "time": today, "source": "公告"},
                {"title": "测试股份历史立案调查", "content": "测试股份历史事项", "time": "2026-01-01", "source": "旧闻"},
            ],
            "data_contract": {"news": {"status": "ok", "checked_at": today}},
        }
    }
    try:
        with TemporaryDirectory() as tmp:
            llm_scorer.BASE_DIR = Path(tmp)
            candidates = [{"stock": "600001", "name": "测试股份", "pool": "准备启动", "pool_score": 70, "pool_score_detail": {}}]
            first = llm_scorer.apply_announcement_risk_penalties(candidates)
            second = llm_scorer.apply_announcement_risk_penalties(candidates)
    finally:
        data_fetcher._load_debate_data_cache = original_loader
        llm_scorer.BASE_DIR = original_base_dir
    assert first["flagged"] == 1 and second["flagged"] == 1
    assert candidates[0]["pool_score"] == 66.0
    assert candidates[0]["pre_announcement_pool_score"] == 70.0
    assert candidates[0]["announcement_risk"]["severity"] == "medium"
