#!/usr/bin/env python3
"""Offline tests for per-lot cost basis, profit taking, and trailing stops."""

from __future__ import annotations

import json
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))


def assert_true(name: str, ok: bool, detail: object = "") -> None:
    if not ok:
        raise AssertionError(f"{name} failed: {detail}")
    print(f"[PASS] {name}")


def trigger_map(triggers):
    return {t["trigger"]: t for t in triggers}


def test_sync_fills_missing_buy_price_from_account_cost():
    from trade_position_sync import reconcile_trades_with_positions

    trades = {
        "records": [{
            "stock": "000001",
            "name": "测试A",
            "buy_date": "2026-05-20",
            "buy_price": 0,
            "quantity": 1000,
            "remaining_quantity": 1000,
            "sells": [],
        }]
    }
    positions = [{
        "stock": "000001",
        "name": "测试A",
        "quantity": 1000,
        "avail_quantity": 1000,
        "cost_price": 10.5,
        "current_price": 11.0,
    }]
    synced, report = reconcile_trades_with_positions(trades, positions, source="test")
    rec = synced["records"][0]
    assert_true(
        "缺买入价时用模拟账户costPrice补齐",
        rec["buy_price"] == 10.5 and rec["buy_records"][0]["price"] == 10.5,
        {"record": rec, "report": report},
    )


def test_sync_preserves_original_buy_price_after_partial_sell():
    from trade_position_sync import reconcile_trades_with_positions

    trades = {
        "records": [{
            "stock": "000002",
            "name": "测试B",
            "buy_date": "2026-05-20",
            "buy_price": 10.0,
            "quantity": 1000,
            "remaining_quantity": 700,
            "sells": [{"date": "2026-05-21", "price": 12.0, "quantity": 300, "reason": "止盈第1档"}],
            "buy_records": [{
                "date": "2026-05-20",
                "price": 10.0,
                "quantity": 1000,
                "remaining": 700,
                "source": "test",
            }],
        }]
    }
    # 模拟账户可能因已卖盈利仓位而显示摊低后的costPrice，但本地原始买入价必须保持10元。
    positions = [{
        "stock": "000002",
        "name": "测试B",
        "quantity": 700,
        "avail_quantity": 700,
        "cost_price": 8.9,
        "current_price": 11.0,
    }]
    synced, report = reconcile_trades_with_positions(trades, positions, source="test")
    rec = synced["records"][0]
    assert_true(
        "部分卖出后不被模拟账户摊低成本覆盖",
        rec["buy_price"] == 10.0 and rec["buy_records"][0]["price"] == 10.0,
        {"record": rec, "report": report},
    )


def test_sync_merges_multiple_buy_dates_as_separate_lots():
    from trade_position_sync import reconcile_trades_with_positions

    trades = {
        "records": [
            {
                "stock": "000003",
                "name": "测试C",
                "buy_date": "2026-05-20",
                "buy_price": 10.0,
                "quantity": 1000,
                "remaining_quantity": 1000,
                "sells": [],
                "buy_records": [{"date": "2026-05-20", "price": 10.0, "quantity": 1000, "remaining": 1000}],
            },
            {
                "stock": "000003",
                "name": "测试C",
                "buy_date": "2026-05-22",
                "buy_price": 12.0,
                "quantity": 500,
                "remaining_quantity": 500,
                "sells": [],
                "buy_records": [{"date": "2026-05-22", "price": 12.0, "quantity": 500, "remaining": 500}],
            },
        ]
    }
    positions = [{
        "stock": "000003",
        "name": "测试C",
        "quantity": 1500,
        "avail_quantity": 1500,
        "cost_price": 10.67,
        "current_price": 12.5,
    }]
    synced, report = reconcile_trades_with_positions(trades, positions, source="test")
    active = [r for r in synced["records"] if r["remaining_quantity"] > 0][0]
    lots = active["buy_records"]
    assert_true(
        "不同日期买入合并为持仓但保留两条原始批次",
        len(lots) == 2 and [l["price"] for l in lots] == [10.0, 12.0] and [l["remaining"] for l in lots] == [1000, 500],
        {"record": active, "report": report},
    )


def test_realtime_multi_lot_take_profit_only_hits_profitable_lot():
    from intraday_monitor_realtime import check_triggers

    info = {
        "bp": 11.0,
        "peak_price": 11.1,
        "atr": 0.3,
        "ma20": 0,
        "avail": 1500,
        "lots": [
            {"date": "2026-05-20", "buy_price": 10.0, "original_quantity": 1000, "remaining": 1000, "executed_tp_tiers": [], "peak_price": 11.1},
            {"date": "2026-05-22", "buy_price": 12.0, "original_quantity": 500, "remaining": 500, "executed_tp_tiers": [], "peak_price": 12.0},
        ],
    }
    triggers = trigger_map(check_triggers("000004", info, {"current": 11.0}))
    assert_true(
        "多批次止盈只按盈利达标批次卖一档",
        triggers.get("tp1", {}).get("quantity_rule") == 300 and "tp2" not in triggers,
        triggers,
    )


def test_realtime_multi_lot_atr_stop_only_hits_retreated_lot():
    from intraday_monitor_realtime import check_triggers

    info = {
        "bp": 11.0,
        "peak_price": 13.0,
        "atr": 0.3,
        "ma20": 0,
        "avail": 1500,
        "lots": [
            {"date": "2026-05-20", "buy_price": 10.0, "original_quantity": 1000, "remaining": 1000, "executed_tp_tiers": [], "peak_price": 13.0},
            {"date": "2026-05-22", "buy_price": 12.0, "original_quantity": 500, "remaining": 500, "executed_tp_tiers": [], "peak_price": 12.6},
        ],
    }
    triggers = trigger_map(check_triggers("000005", info, {"current": 12.5}))
    assert_true(
        "30%最高浮盈批次回落到25%触发移动止损且只卖该批",
        triggers.get("atr", {}).get("quantity_rule") == 1000,
        triggers,
    )


def test_realtime_partial_take_profit_rounding_rules():
    from intraday_monitor_realtime import check_triggers

    small = {
        "bp": 10.0,
        "peak_price": 11.0,
        "atr": 0.3,
        "ma20": 0,
        "avail": 100,
        "lots": [{"buy_price": 10.0, "original_quantity": 100, "remaining": 100, "executed_tp_tiers": [], "peak_price": 11.0}],
    }
    small_triggers = trigger_map(check_triggers("000006", small, {"current": 11.0}))

    regular = {
        "bp": 10.0,
        "peak_price": 11.0,
        "atr": 0.3,
        "ma20": 0,
        "avail": 900,
        "lots": [
            {"buy_price": 10.0, "original_quantity": 300, "remaining": 300, "executed_tp_tiers": [], "peak_price": 11.0},
            {"buy_price": 10.0, "original_quantity": 600, "remaining": 600, "executed_tp_tiers": [], "peak_price": 11.0},
        ],
    }
    regular_triggers = trigger_map(check_triggers("000007", regular, {"current": 11.0}))

    star = {
        "bp": 10.0,
        "peak_price": 11.0,
        "atr": 0.3,
        "ma20": 0,
        "avail": 500,
        "lots": [{"buy_price": 10.0, "original_quantity": 500, "remaining": 500, "executed_tp_tiers": [], "peak_price": 11.0}],
    }
    star_triggers = trigger_map(check_triggers("688001", star, {"current": 11.0}))

    assert_true(
        "普通A股100股不足分档止盈不触发",
        "tp1" not in small_triggers,
        small_triggers,
    )
    assert_true(
        "普通A股分档止盈按整百向下规整",
        regular_triggers.get("tp1", {}).get("quantity_rule") == 200,
        regular_triggers,
    )
    assert_true(
        "科创板一二档不足200股不触发",
        "tp1" not in star_triggers,
        star_triggers,
    )


def test_realtime_take_profit_marks_lot_state_after_sell():
    from intraday_monitor_realtime import _apply_confirmed_sell_to_state_lots, check_triggers

    position = {
        "bp": 6.911,
        "peak_price": 7.62,
        "atr": 0.48,
        "ma20": 0,
        "avail": 6500,
        "executed_tp_tiers": [],
        "lots": [
            {
                "date": "2026-05-28",
                "buy_price": 6.911,
                "original_quantity": 6500,
                "remaining": 6500,
                "executed_tp_tiers": [],
                "peak_price": 7.62,
            }
        ],
    }
    trigger = {"trigger": "tp1", "reason": "止盈第1档(分批合计触发)卖原始仓位30%"}
    _apply_confirmed_sell_to_state_lots(position, {"current": 7.62}, trigger, 1900)
    triggers = trigger_map(check_triggers("600863", position, {"current": 7.62}))

    assert_true(
        "实时止盈卖出后批次层标记第1档并扣减剩余，避免同一档重复触发",
        position["executed_tp_tiers"] == [1]
        and position["lots"][0]["executed_tp_tiers"] == [1]
        and position["lots"][0]["remaining"] == 4600
        and "tp1" not in triggers,
        {"position": position, "triggers": triggers},
    )


def test_realtime_take_profit_marks_sold_lot_when_confirm_quote_missing():
    from intraday_monitor_realtime import _apply_confirmed_sell_to_state_lots, check_triggers

    position = {
        "bp": 21.62,
        "peak_price": 23.84,
        "atr": 1.48,
        "ma20": 0,
        "avail": 2200,
        "executed_tp_tiers": [],
        "lots": [
            {
                "date": "2026-06-15",
                "buy_price": 21.62,
                "original_quantity": 2200,
                "remaining": 2200,
                "executed_tp_tiers": [],
                "peak_price": 23.84,
            }
        ],
    }
    trigger = {"trigger": "tp1", "reason": "止盈第1档(分批合计触发)卖原始仓位30%"}
    _apply_confirmed_sell_to_state_lots(position, {}, trigger, 600)
    position["avail"] = 1600
    triggers = trigger_map(check_triggers("603155", position, {"current": 23.83}))

    assert_true(
        "实时止盈成交确认缺行情价时也标记被卖批次，避免下一轮重复触发同一档",
        position["executed_tp_tiers"] == [1]
        and position["lots"][0]["executed_tp_tiers"] == [1]
        and position["lots"][0]["remaining"] == 1600
        and "tp1" not in triggers,
        {"position": position, "triggers": triggers},
    )


def test_realtime_restart_inherits_top_level_take_profit_tiers_to_lots():
    from intraday_monitor_realtime import _inherit_old_take_profit_tiers, check_triggers

    position = {
        "bp": 6.911,
        "peak_price": 7.62,
        "atr": 0.48,
        "ma20": 0,
        "avail": 4600,
        "buy_dates": ["2026-05-28"],
        "executed_tp_tiers": [],
        "lots": [
            {
                "date": "2026-05-28",
                "buy_price": 6.911,
                "original_quantity": 6500,
                "remaining": 4600,
                "executed_tp_tiers": [],
                "peak_price": 7.62,
            }
        ],
    }
    old_position = {
        "buy_dates": ["2026-05-28"],
        "executed_tp_tiers": [1],
        "lots": [],
    }
    _inherit_old_take_profit_tiers(position, old_position)
    triggers = trigger_map(check_triggers("600863", position, {"current": 7.62}))

    assert_true(
        "重启后把旧顶层止盈档位继承到同一买入日期的批次，避免重复卖同一档",
        position["lots"][0]["executed_tp_tiers"] == [1] and "tp1" not in triggers,
        {"position": position, "triggers": triggers},
    )


def test_realtime_sell_order_price_uses_latest_price_above_limit_down():
    from intraday_monitor_realtime import _sell_order_price_from_latest

    assert_true(
        "实时卖出报价用最新价乘0.985且只用跌停价做下限",
        _sell_order_price_from_latest(7.63, 6.57) == 7.52,
        _sell_order_price_from_latest(7.63, 6.57),
    )


def test_realtime_do_sell_passes_latest_price_not_prediscounted_price():
    import intraday_monitor_realtime as realtime

    captured = {}
    old_sell_stock = realtime.sell_stock
    old_get_limit_down = realtime._get_limit_down
    old_push = realtime._push_message
    old_sleep = realtime.time.sleep
    old_reconcile = realtime.reconcile_trades_file_with_account
    old_append = realtime._append_realtime_sell_record
    try:
        realtime.sell_stock = lambda stock_code, stock_name, price, quantity, reason: captured.update({
            "stock_code": stock_code,
            "price": price,
            "quantity": quantity,
            "reason": reason,
        }) or {"status": "submitted"}
        realtime._get_limit_down = lambda stock: 6.57
        realtime._push_message = lambda text: None
        realtime.time.sleep = lambda seconds: None
        realtime.reconcile_trades_file_with_account = lambda source: {"fixed": [], "is_consistent": True}
        realtime._append_realtime_sell_record = lambda **kwargs: False

        state = {
            "positions": {
                "600863": {
                    "avail": 6500,
                    "executed_tp_tiers": [],
                    "lots": [{
                        "buy_price": 6.911,
                        "original_quantity": 6500,
                        "remaining": 6500,
                        "executed_tp_tiers": [],
                    }],
                }
            }
        }
        info = {"name": "华能蒙电", "avail": 6500}
        quote = {"current": 7.63}
        trigger = {
            "trigger": "tp1",
            "reason": "止盈第1档(分批合计触发)卖原始仓位30%",
            "quantity_rule": 1900,
            "pct": 0.10,
        }
        realtime.do_sell_and_push("600863", info, quote, trigger, "SELL", state)
    finally:
        realtime.sell_stock = old_sell_stock
        realtime._get_limit_down = old_get_limit_down
        realtime._push_message = old_push
        realtime.time.sleep = old_sleep
        realtime.reconcile_trades_file_with_account = old_reconcile
        realtime._append_realtime_sell_record = old_append

    assert_true(
        "实时监控下单传入模拟账户最新价本身，避免二次0.985折扣",
        captured.get("price") == 7.63 and captured.get("quantity") == 1900,
        captured,
    )


def test_realtime_sell_uses_pending_order_confirmation_not_account_sync():
    import intraday_monitor_realtime as realtime

    calls = {"reconcile": 0, "saved": [], "orders": 0}
    old_sell_stock = realtime.sell_stock
    old_get_limit_down = realtime._get_limit_down
    old_push = realtime._push_message
    old_reconcile = realtime.reconcile_trades_file_with_account
    old_get_orders = realtime.get_today_orders
    old_append = realtime._append_realtime_sell_record
    try:
        realtime.sell_stock = lambda **kwargs: {"status": "submitted", "orderId": "S1"}
        realtime._get_limit_down = lambda stock: 6.57
        realtime._push_message = lambda text: None

        def fail_reconcile(source):
            calls["reconcile"] += 1
            raise AssertionError("reconcile should not run immediately after realtime sell")

        realtime.reconcile_trades_file_with_account = fail_reconcile
        realtime._append_realtime_sell_record = lambda **kwargs: calls["saved"].append(kwargs) or True

        state = {
            "positions": {
                "600863": {
                    "name": "华能蒙电",
                    "avail": 1000,
                    "executed_tp_tiers": [],
                    "lots": [{
                        "buy_price": 10.0,
                        "original_quantity": 1000,
                        "remaining": 1000,
                        "executed_tp_tiers": [],
                    }],
                }
            }
        }
        trigger = {
            "trigger": "tp1",
            "reason": "止盈第1档(分批合计触发)卖原始仓位30%",
            "quantity_rule": 300,
            "pct": 0.12,
        }
        realtime.do_sell_and_push(
            "600863",
            {"name": "华能蒙电", "avail": 1000},
            {"current": 12.0},
            trigger,
            "SELL",
            state,
        )
        pending = state["positions"]["600863"].get("pending_sell")
        assert_true(
            "实时卖出提交后只记录pending不立刻做账户同步",
            calls["reconcile"] == 0 and pending and state["positions"]["600863"]["avail"] == 1000,
            {"calls": calls, "pending": pending, "state": state},
        )

        realtime.get_today_orders = lambda force=False: {
            "buys": [],
            "sells": [{
                "stock": "600863",
                "orderId": "S1",
                "quantity": 100,
                "order_quantity": 300,
                "status": "部成",
                "drt": "sell",
            }],
            "_ok": True,
        }
        skipped = realtime._confirm_pending_sell_from_orders(
            "600863",
            state["positions"]["600863"],
            {"current": 12.0},
            pending,
        )
        assert_true(
            "实时卖出订单仍未完成时继续等待且不扣减仓位",
            skipped and state["positions"]["600863"].get("pending_sell") and state["positions"]["600863"]["avail"] == 1000,
            state,
        )

        realtime.get_today_orders = lambda force=False: {
            "buys": [],
            "sells": [{
                "stock": "600863",
                "orderId": "S1",
                "quantity": 300,
                "order_quantity": 300,
                "trade_price": 11.82,
                "status": "已成",
                "drt": "sell",
            }],
            "_ok": True,
        }
        skipped = realtime._confirm_pending_sell_from_orders(
            "600863",
            state["positions"]["600863"],
            {"current": 12.0},
            state["positions"]["600863"]["pending_sell"],
        )
        assert_true(
            "实时卖出只在订单回查确认成交后扣仓并补写记录",
            skipped
            and state["positions"]["600863"].get("pending_sell") is None
            and state["positions"]["600863"]["avail"] == 700
            and calls["saved"]
            and calls["saved"][0]["quantity"] == 300,
            {"calls": calls, "state": state},
        )
    finally:
        realtime.sell_stock = old_sell_stock
        realtime._get_limit_down = old_get_limit_down
        realtime._push_message = old_push
        realtime.reconcile_trades_file_with_account = old_reconcile
        realtime.get_today_orders = old_get_orders
        realtime._append_realtime_sell_record = old_append


def test_realtime_quote_batch_defaults_to_serial():
    import intraday_monitor_realtime as realtime

    old_get = realtime.get_xq_realtime_quote
    old_workers = realtime.QUOTE_MAX_WORKERS
    calls = []
    try:
        realtime.QUOTE_MAX_WORKERS = 1
        realtime.get_xq_realtime_quote = lambda code: calls.append(code) or {"current": 10.0}
        result = realtime._get_realtime_quote_batch(["000001", "000002", "000003"])
        assert_true(
            "实时行情默认串行获取，避免持仓多时打爆行情接口",
            calls == ["000001", "000002", "000003"] and len(result) == 3,
            {"calls": calls, "result": result},
        )
    finally:
        realtime.get_xq_realtime_quote = old_get
        realtime.QUOTE_MAX_WORKERS = old_workers


def test_realtime_poll_refreshes_account_positions_once_for_multiple_triggers():
    import intraday_monitor_realtime as realtime

    old_quote_batch = realtime._get_realtime_quote_batch
    old_get_positions = realtime.get_current_positions
    old_do_sell = realtime.do_sell_and_push
    calls = {"positions": 0, "sells": []}
    try:
        realtime._get_realtime_quote_batch = lambda stocks: {s: {"current": 12.0} for s in stocks}

        def fake_positions():
            calls["positions"] += 1
            return [
                {"stockCode": "000001", "stockName": "测试1", "totalQuantity": 1000, "availQuantity": 1000, "price": 1200, "priceDec": 2},
                {"stockCode": "000002", "stockName": "测试2", "totalQuantity": 1000, "availQuantity": 1000, "price": 1200, "priceDec": 2},
            ]

        realtime.get_current_positions = fake_positions
        realtime.do_sell_and_push = lambda stock, info, quote, trigger, decision, state: calls["sells"].append(stock)
        state = {
            "positions": {
                code: {
                    "bp": 10.0,
                    "peak_price": 12.0,
                    "quantity": 1000,
                    "original_quantity": 1000,
                    "avail": 1000,
                    "account_price": 12.0,
                    "ma20": 0.0,
                    "atr": 0.0,
                    "name": f"测试{idx}",
                    "buy_dates": ["2026-05-20"],
                    "executed_tp_tiers": [],
                    "lots": [{
                        "date": "2026-05-20",
                        "buy_price": 10.0,
                        "original_quantity": 1000,
                        "remaining": 1000,
                        "executed_tp_tiers": [],
                        "peak_price": 12.0,
                    }],
                }
                for idx, code in enumerate(["000001", "000002"], start=1)
            }
        }
        realtime.poll_once(state)
        assert_true(
            "同一轮多只股票触发卖出时只刷新一次模拟账户持仓",
            calls["positions"] == 1 and calls["sells"] == ["000001", "000002"],
            calls,
        )
    finally:
        realtime._get_realtime_quote_batch = old_quote_batch
        realtime.get_current_positions = old_get_positions
        realtime.do_sell_and_push = old_do_sell


def test_realtime_append_sell_record_reduces_trade_lots():
    import copy
    import intraday_monitor_realtime as realtime

    trades = {
        "records": [{
            "stock": "600863",
            "name": "华能蒙电",
            "buy_date": "2026-05-28",
            "buy_price": 6.911,
            "quantity": 6500,
            "remaining_quantity": 6500,
            "buy_records": [{
                "date": "2026-05-28",
                "price": 6.911,
                "quantity": 6500,
                "remaining": 6500,
            }],
            "sells": [],
        }]
    }
    saved = {}
    old_load = realtime.load_trades
    old_save = realtime.save_trades
    try:
        realtime.load_trades = lambda path=None: copy.deepcopy(trades)
        realtime.save_trades = lambda data, path=None: saved.update({"data": data})
        ok = realtime._append_realtime_sell_record(
            "600863",
            price=7.52,
            quantity=1900,
            reason="[实时监控] 止盈第1档(分批合计触发)卖原始仓位30%",
        )
    finally:
        realtime.load_trades = old_load
        realtime.save_trades = old_save

    rec = saved["data"]["records"][0]
    assert_true(
        "实时成交写入trades.json时同步扣减remaining_quantity和buy_records.remaining",
        ok
        and rec["remaining_quantity"] == 4600
        and rec["buy_records"][0]["remaining"] == 4600
        and rec["sells"][0]["quantity"] == 1900
        and rec["sells"][0]["buy_price_used"] == 6.911,
        rec,
    )


def test_realtime_find_sell_order_without_id_ignores_old_orders():
    import time
    import intraday_monitor_realtime as realtime

    sent_at = time.time()
    old_order = {
        "stock": "600863",
        "quantity": 1900,
        "order_quantity": 1900,
        "order_time": "2026-06-01T10:00:00",
    }
    pending = {"sent_at": sent_at, "quantity": 1900, "order_id": None}
    matched = realtime._find_sell_order("600863", pending, [old_order])

    assert_true(
        "没有委托号时不把本次下单前的旧同股卖单误认为pending成交",
        matched is None,
        matched,
    )


def test_realtime_position_cost_price_uses_decimal_places():
    from intraday_monitor_realtime import _position_cost_price

    assert_true(
        "模拟账户成本价按costPriceDec还原",
        abs(_position_cost_price({"costPrice": 6911, "costPriceDec": 3}) - 6.911) < 1e-9,
        _position_cost_price({"costPrice": 6911, "costPriceDec": 3}),
    )


def test_realtime_limit_down_uses_board_limit_pct():
    import intraday_monitor_realtime as realtime

    old_parse = realtime._parse_http_kline
    try:
        realtime._parse_http_kline = lambda *args, **kwargs: [{"close": "10.0"}, {"close": "11.0"}]
        regular = realtime._get_limit_down("600000")
        chinext = realtime._get_limit_down("300001")
        star = realtime._get_limit_down("688001")
        bse = realtime._get_limit_down("920001")
    finally:
        realtime._parse_http_kline = old_parse

    assert_true(
        "跌停价按板块比例计算：主板10%，创业/科创20%，北交所30%",
        regular == 9.0 and chinext == 8.0 and star == 8.0 and bse == 7.0,
        {"regular": regular, "chinext": chinext, "star": star, "bse": bse},
    )


def test_sell_stock_uses_passed_latest_price_when_quote_is_bad():
    import intraday_executor as intraday

    captured = {}
    old_quote = intraday.get_realtime_quote
    old_api = intraday.mx_api_post
    old_push = intraday.feishu_push
    try:
        intraday.get_realtime_quote = lambda stock: {"price": 5.98, "limit_down": 6.57, "change_pct": -10.0}
        intraday.mx_api_post = lambda endpoint, payload: captured.update(payload) or {"code": "200", "data": {"result": {"status": 0}}}
        intraday.feishu_push = lambda msg: None

        intraday.sell_stock("600863", "华能蒙电", 7.63, 1900, "test")
    finally:
        intraday.get_realtime_quote = old_quote
        intraday.mx_api_post = old_api
        intraday.feishu_push = old_push

    assert_true(
        "公共卖出函数以调用方传入最新价为基准，不被异常低行情压到跌停价",
        captured.get("price") == 7.52 and captured.get("useMarketPrice") is False,
        captured,
    )


def test_executor_lot_state_restores_executed_tiers_by_fifo():
    import intraday_executor as intraday

    record = {
        "stock": "000008",
        "buy_price": 10.0,
        "quantity": 1500,
        "remaining_quantity": 1200,
        "buy_records": [
            {"date": "2026-05-20", "price": 10.0, "quantity": 1000, "remaining": 700},
            {"date": "2026-05-22", "price": 12.0, "quantity": 500, "remaining": 500},
        ],
        "sells": [{"date": "2026-05-23", "price": 11.0, "quantity": 300, "reason": "止盈第1档"}],
    }
    lots = intraday._build_lot_states(record, 10.0, 1200)
    assert_true(
        "历史卖出按FIFO映射到对应买入批次",
        1 in lots[0]["executed_tp_tiers"] and 1 not in lots[1]["executed_tp_tiers"],
        lots,
    )


def test_executor_reconcile_adds_extra_lot_without_overwriting_old_prices():
    import intraday_executor as intraday

    record = {
        "stock": "000009",
        "buy_price": 10.0,
        "quantity": 1000,
        "remaining_quantity": 1000,
        "buy_records": [{"date": "2026-05-20", "price": 10.0, "quantity": 1000, "remaining": 1000}],
        "sells": [],
    }
    changed = intraday._reconcile_trade_record_to_position(record, 1300, 12.0)
    assert_true(
        "卖出前同步发现多300股时追加新批次且不覆盖旧批次成本",
        changed
        and record["buy_records"][0]["price"] == 10.0
        and record["buy_records"][1]["price"] == 12.0
        and intraday._tracked_remaining_quantity(record) == 1300,
        record,
    )


def test_sync_removes_spurious_position_reconcile_lot_before_real_lots():
    from trade_position_sync import aggregate_trades_positions, reconcile_trades_with_positions

    trades = {
        "records": [
            {
                "stock": "600584",
                "name": "长电科技",
                "buy_date": "2026-06-23",
                "buy_price": 91.04,
                "quantity": 300,
                "remaining_quantity": 300,
                "buy_records": [
                    {"date": "2026-06-23", "price": 91.04, "quantity": 300, "remaining": 0, "source": "intraday_buy_timing"},
                    {"date": "2026-06-26", "price": 99.386, "quantity": 300, "remaining": 300, "source": "position_reconcile"},
                ],
                "sells": [],
            },
            {
                "stock": "600584",
                "name": "长电科技",
                "buy_date": "2026-06-26",
                "buy_price": 105.47,
                "quantity": 400,
                "remaining_quantity": 400,
                "buy_records": [
                    {"date": "2026-06-23", "price": 91.04, "quantity": 300, "remaining": 300, "source": "intraday_buy_timing"},
                    {"date": "2026-06-26", "price": 105.47, "quantity": 400, "remaining": 400, "source": "intraday_buy_timing"},
                ],
                "sells": [],
            },
        ]
    }
    positions = [{
        "stock": "600584",
        "name": "长电科技",
        "quantity": 700,
        "avail_quantity": 300,
        "cost_price": 99.386,
        "current_price": 105.47,
    }]
    synced, report = reconcile_trades_with_positions(trades, positions, source="test")
    agg = aggregate_trades_positions(synced)
    active = [rec for rec in synced["records"] if rec.get("buy_date") == "2026-06-26"][0]
    lots_by_price = {float(lot["price"]): int(lot.get("remaining", 0) or 0) for lot in active["buy_records"]}
    reconcile_remaining = sum(
        int(lot.get("remaining", 0) or 0)
        for lot in active["buy_records"]
        if lot.get("source") == "position_reconcile"
    )
    assert_true(
        "同步数量过高时优先扣掉position_reconcile虚拟批次",
        report["is_consistent"]
        and agg["600584"]["qty"] == 700
        and lots_by_price.get(91.04) == 300
        and lots_by_price.get(105.47) == 400
        and reconcile_remaining == 0,
        {"active": active, "report": report, "agg": agg},
    )


def test_executor_snapshot_sync_uses_total_position_not_sellable_quantity():
    import intraday_executor as intraday
    from trade_position_sync import aggregate_trades_positions

    trades = {
        "records": [{
            "stock": "600584",
            "name": "长电科技",
            "buy_date": "2026-06-26",
            "buy_price": 105.47,
            "quantity": 700,
            "remaining_quantity": 700,
            "buy_records": [
                {"date": "2026-06-23", "price": 91.04, "quantity": 300, "remaining": 300, "source": "intraday_buy_timing"},
                {"date": "2026-06-26", "price": 105.47, "quantity": 400, "remaining": 400, "source": "intraday_buy_timing"},
            ],
            "sells": [],
        }]
    }
    positions = [{
        "stockCode": "600584",
        "stockName": "长电科技",
        "totalQuantity": 700,
        "availQuantity": 300,
        "costPrice": 99386,
        "costPriceDec": 3,
        "price": 10547,
        "priceDec": 2,
    }]
    synced, report = intraday._reconcile_trades_to_positions_snapshot(trades, positions, source="test")
    selected = intraday._select_trade_record_for_stock(synced, "600584")
    agg = aggregate_trades_positions(synced)
    assert_true(
        "盘中快照同步按总持仓校准而不是按可卖数量校准",
        report["is_consistent"]
        and agg["600584"]["qty"] == 700
        and intraday._tracked_remaining_quantity(selected) == 700,
        {"selected": selected, "report": report, "agg": agg},
    )


def main() -> None:
    tests = [
        test_sync_fills_missing_buy_price_from_account_cost,
        test_sync_preserves_original_buy_price_after_partial_sell,
        test_sync_merges_multiple_buy_dates_as_separate_lots,
        test_realtime_multi_lot_take_profit_only_hits_profitable_lot,
        test_realtime_multi_lot_atr_stop_only_hits_retreated_lot,
        test_realtime_partial_take_profit_rounding_rules,
        test_realtime_take_profit_marks_lot_state_after_sell,
        test_realtime_take_profit_marks_sold_lot_when_confirm_quote_missing,
        test_realtime_restart_inherits_top_level_take_profit_tiers_to_lots,
        test_realtime_sell_order_price_uses_latest_price_above_limit_down,
        test_realtime_do_sell_passes_latest_price_not_prediscounted_price,
        test_realtime_sell_uses_pending_order_confirmation_not_account_sync,
        test_realtime_quote_batch_defaults_to_serial,
        test_realtime_poll_refreshes_account_positions_once_for_multiple_triggers,
        test_realtime_append_sell_record_reduces_trade_lots,
        test_realtime_find_sell_order_without_id_ignores_old_orders,
        test_realtime_position_cost_price_uses_decimal_places,
        test_realtime_limit_down_uses_board_limit_pct,
        test_sell_stock_uses_passed_latest_price_when_quote_is_bad,
        test_executor_lot_state_restores_executed_tiers_by_fifo,
        test_executor_reconcile_adds_extra_lot_without_overwriting_old_prices,
        test_sync_removes_spurious_position_reconcile_lot_before_real_lots,
        test_executor_snapshot_sync_uses_total_position_not_sellable_quantity,
    ]
    for test in tests:
        test()
    print(f"all {len(tests)} trade lot cost-basis tests passed")


if __name__ == "__main__":
    main()
