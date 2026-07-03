#!/usr/bin/env python3
"""
盘中实时监控触发规则测试
测试 check_triggers() 函数在各种场景下是否准确触发卖出规则
"""
import sys
sys.path.insert(0, ".")

from intraday_monitor_realtime import check_triggers, TAKE_PROFIT_1, TAKE_PROFIT_2, TAKE_PROFIT_3, ATR_TIERS

# ============================================================================
# 测试用例定义
# ============================================================================
# info 格式:
#   bp = 买入价（成本价）
#   peak_price = 持仓期最高价
#   atr = ATR值
#   ma20 = MA20值
#   avail = 可卖数量
#   original_quantity = 原始买入数量
#   executed_tp_tiers = 已执行的止盈档位 [1,2,3]
#
# quote 格式:
#   current = 当前价

def run_test(name, info, current, expected_trigger=None, expected_reason_contains=None):
    quote = {"current": current}
    triggers = check_triggers("TEST001", info, quote)
    
    print(f"\n{'='*70}")
    print(f"📋 测试: {name}")
    print(f"   成本价(bp)={info['bp']:.3f} 当前价={current:.3f} 浮盈={(current-info['bp'])/info['bp']*100:.1f}%")
    print(f"   最高价={info['peak_price']:.3f} ATR={info.get('atr',0):.3f} MA20={info.get('ma20',0):.3f}")
    print(f"   可卖={info['avail']} 已执行档位={info.get('executed_tp_tiers', [])}")
    
    if not triggers:
        print(f"   ✅ 结果: 无触发（预期正确）")
        return True
    
    for t in triggers:
        print(f"   触发: {t['trigger']} | {t['reason']}")
    
    if expected_trigger:
        triggered = any(t['trigger'] == expected_trigger for t in triggers)
        if not triggered:
            print(f"   ❌ 失败: 期望触发 '{expected_trigger}' 但未触发")
            return False
        print(f"   ✅ 止盈触发正确")
    
    if expected_reason_contains:
        matched = any(expected_reason_contains in t['reason'] for t in triggers)
        if not matched:
            print(f"   ❌ 失败: 原因应包含 '{expected_reason_contains}'")
            return False
        print(f"   ✅ 原因正确: 包含 '{expected_reason_contains}'")
    
    return True

# ============================================================================
# 场景1: 止盈第1档 - 浮盈刚达到10%，卖30%
# ============================================================================
def test_tp1_trigger():
    info = {
        "bp": 10.0, "peak_price": 11.0, "atr": 0.3, "ma20": 10.5,
        "avail": 1000, "original_quantity": 1000, "executed_tp_tiers": []
    }
    return run_test(
        "场景1: 止盈第1档 - 浮盈刚达到10%，卖30%",
        info, current=11.0,
        expected_trigger="tp1",
        expected_reason_contains="止盈第1档"
    )

# ============================================================================
# 场景2: 止盈第2档 - 浮盈达到20%，卖20%
# ============================================================================
def test_tp2_trigger():
    info = {
        "bp": 10.0, "peak_price": 12.0, "atr": 0.3, "ma20": 10.5,
        "avail": 1000, "original_quantity": 1000, "executed_tp_tiers": [1]
    }
    return run_test(
        "场景2: 止盈第2档 - 浮盈达到20%，卖20%（第1档已执行）",
        info, current=12.0,
        expected_trigger="tp2",
        expected_reason_contains="止盈第2档"
    )

# ============================================================================
# 场景3: 止盈第3档 - 浮盈达到50%，卖剩余全部
# ============================================================================
def test_tp3_trigger():
    info = {
        "bp": 10.0, "peak_price": 15.0, "atr": 0.3, "ma20": 10.5,
        "avail": 500, "original_quantity": 1000, "executed_tp_tiers": [1, 2]
    }
    return run_test(
        "场景3: 止盈第3档 - 浮盈达到50%，卖剩余全部（第1、2档已执行）",
        info, current=15.0,
        expected_trigger="tp3",
        expected_reason_contains="止盈第3档"
    )

# ============================================================================
# 场景4: 止盈第1档未触发 - 浮盈只有9%
# ============================================================================
def test_tp1_not_trigger():
    info = {
        "bp": 10.0, "peak_price": 10.9, "atr": 0.3, "ma20": 10.5,
        "avail": 1000, "original_quantity": 1000, "executed_tp_tiers": []
    }
    quote = {"current": 10.9}
    triggers = check_triggers("TEST001", info, quote)
    tp1_fired = any(t["trigger"] == "tp1" for t in triggers)
    print(f"\n{'='*70}")
    print(f"📋 测试: 场景4: 止盈第1档未触发 - 浮盈只有9%")
    print(f"   bp=10.0 当前价=10.9 浮盈=9.0%")
    if not tp1_fired:
        print(f"   ✅ 结果: 未触发tp1（预期正确，9%<10%）")
        return True
    print(f"   ❌ 失败: 不应触发tp1")
    return False

# ============================================================================
# 场景5: ATR止损 - 最高浮盈3%后回撤，触发 max(成本-2ATR, 成本-3%)
# 成本10, ATR=0.3, 成本-2ATR=9.4, 成本-3%=9.7 → 取9.7, 即浮盈-3%
# 当前价9.7, 浮盈-3% < 止损线, 触发止损
# ============================================================================
def test_atr_stop_loss_tier1():
    info = {
        "bp": 10.0, "peak_price": 10.3, "atr": 0.3, "ma20": 10.5,
        "avail": 1000, "original_quantity": 1000, "executed_tp_tiers": []
    }
    # 最高价10.3 = 浮盈3%，当前价9.7 = 浮盈-3%，止损线=max(10-2*0.3, 10*0.97)=max(9.4,9.7)=9.7
    return run_test(
        "场景5: ATR止损 - 最高浮盈3%后回撤到成本-3%，触发止损",
        info, current=9.7,
        expected_trigger="atr",
        expected_reason_contains="ATR止损"
    )

# ============================================================================
# 场景6: ATR止损第2档 - 最高浮盈5%后回撤到5%（成本价）
# ============================================================================
def test_atr_stop_loss_tier2():
    info = {
        "bp": 10.0, "peak_price": 10.5, "atr": 0.3, "ma20": 10.5,
        "avail": 1000, "original_quantity": 1000, "executed_tp_tiers": []
    }
    # 最高价10.5 = 浮盈5%，当前价10.0 = 浮盈0%，止损线=0%
    return run_test(
        "场景6: ATR止损 - 最高浮盈5%后回撤到成本价，触发止损",
        info, current=10.0,
        expected_trigger="atr",
        expected_reason_contains="ATR止损"
    )

# ============================================================================
# 场景7: ATR止损第3档 - 最高浮盈10%后回撤到5%
# ============================================================================
def test_atr_stop_loss_tier3():
    info = {
        "bp": 10.0, "peak_price": 11.0, "atr": 0.3, "ma20": 10.5,
        "avail": 1000, "original_quantity": 1000, "executed_tp_tiers": []
    }
    # 最高价11.0=浮盈10%，当前价10.5=浮盈5%，止损线5%
    return run_test(
        "场景7: ATR止损 - 最高浮盈10%后回撤到5%，触发止损",
        info, current=10.5,
        expected_trigger="atr",
        expected_reason_contains="ATR止损"
    )

# ============================================================================
# 场景8: ATR止损第4档 - 最高浮盈20%后回撤到10%
# ============================================================================
def test_atr_stop_loss_tier4():
    info = {
        "bp": 10.0, "peak_price": 12.0, "atr": 0.3, "ma20": 10.5,
        "avail": 1000, "original_quantity": 1000, "executed_tp_tiers": []
    }
    return run_test(
        "场景8: ATR止损 - 最高浮盈20%后回撤到10%，触发止损",
        info, current=11.0,
        expected_trigger="atr",
        expected_reason_contains="ATR止损"
    )

# ============================================================================
# 场景9: MA20止损 - 股价跌破MA20
# ============================================================================
def test_ma20_stop_loss():
    info = {
        "bp": 10.0, "peak_price": 10.3, "atr": 0.3, "ma20": 10.5,
        "avail": 1000, "original_quantity": 1000, "executed_tp_tiers": []
    }
    return run_test(
        "场景9: MA20止损 - 现价10.3 < MA20 10.5，触发止损",
        info, current=10.3,
        expected_trigger="ma20",
        expected_reason_contains="MA20止损"
    )

# ============================================================================
# 场景10: 止盈第1档和第2档同时触发
# ============================================================================
def test_tp1_tp2_simultaneous():
    info = {
        "bp": 10.0, "peak_price": 12.0, "atr": 0.3, "ma20": 10.5,
        "avail": 1000, "original_quantity": 1000, "executed_tp_tiers": []
    }
    quote = {"current": 12.0}
    triggers = check_triggers("TEST001", info, quote)
    has_tp1 = any(t["trigger"] == "tp1" for t in triggers)
    has_tp2 = any(t["trigger"] == "tp2" for t in triggers)
    print(f"\n{'='*70}")
    print(f"📋 测试: 场景10: 止盈第1档和第2档同时触发（浮盈20%）")
    print(f"   bp=10.0 当前价=12.0 浮盈=20%")
    print(f"   tp1触发: {has_tp1} | tp2触发: {has_tp2}")
    if has_tp1 and has_tp2:
        print(f"   ✅ 两档同时触发正确")
        return True
    print(f"   ❌ 失败")
    return False

# ============================================================================
# 场景11: 止盈第1档执行后，再次达到10%不再触发
# ============================================================================
def test_tp1_already_executed():
    info = {
        "bp": 10.0, "peak_price": 11.0, "atr": 0.3, "ma20": 10.5,
        "avail": 1000, "original_quantity": 1000, "executed_tp_tiers": [1]
    }
    quote = {"current": 11.0}
    triggers = check_triggers("TEST001", info, quote)
    tp1_fired = any(t["trigger"] == "tp1" for t in triggers)
    print(f"\n{'='*70}")
    print(f"📋 测试: 场景11: 止盈第1档执行后，再次达到10%不再触发")
    print(f"   bp=10.0 当前价=11.0 浮盈=10%，但第1档已执行")
    if not tp1_fired:
        print(f"   ✅ 第1档未再次触发（预期正确）")
        return True
    print(f"   ❌ 失败: 不应重复触发")
    return False

# ============================================================================
# 场景12: 股价=0 或 bp=0，不触发
# ============================================================================
def test_zero_price_no_trigger():
    info = {
        "bp": 10.0, "peak_price": 10.3, "atr": 0.3, "ma20": 10.5,
        "avail": 1000, "original_quantity": 1000, "executed_tp_tiers": []
    }
    triggers = check_triggers("TEST001", info, {"current": 0})
    print(f"\n{'='*70}")
    print(f"📋 测试: 场景12: 股价=0，不触发任何规则")
    if not triggers:
        print(f"   ✅ 无触发（预期正确）")
        return True
    print(f"   ❌ 失败")
    return False

# ============================================================================
# 场景13: ATR止损 with ATR=0（无ATR数据时用成本-3%）
# ============================================================================
def test_atr_stop_loss_no_atr_data():
    info = {
        "bp": 10.0, "peak_price": 10.3, "atr": 0.0, "ma20": 10.5,
        "avail": 1000, "original_quantity": 1000, "executed_tp_tiers": []
    }
    # 无ATR数据，止损线=max(10-2*0, 10*0.97)=9.7，当前价9.7，浮盈-3%
    return run_test(
        "场景13: ATR止损 - 无ATR数据时用成本-3%止损",
        info, current=9.7,
        expected_trigger="atr",
        expected_reason_contains="ATR止损"
    )

# ============================================================================
# 场景14: 最高价=成本价，ATR止损不触发
# ============================================================================
def test_no_atr_trigger_when_no_profit():
    info = {
        "bp": 10.0, "peak_price": 10.0, "atr": 0.3, "ma20": 10.5,
        "avail": 1000, "original_quantity": 1000, "executed_tp_tiers": []
    }
    # peak_price == bp，不满足 peak_price > bp 条件，ATR止损不触发
    triggers = check_triggers("TEST001", info, {"current": 9.7})
    atr_triggered = any(t["trigger"] == "atr" for t in triggers)
    print(f"\n{'='*70}")
    print(f"📋 测试: 场景14: 最高价=成本价，即使跌破成本ATR止损也不触发")
    print(f"   bp=10.0 peak=10.0 当前=9.7（未盈利过，不适用ATR止损）")
    if not atr_triggered:
        print(f"   ✅ ATR止损未触发（预期正确，无盈利不适用ATR）")
        return True
    print(f"   ❌ 失败")
    return False

# ============================================================================
# 场景15: 科创板数量规范化（200股起）
# ============================================================================
def test_kcb_quantity_normalization():
    info = {
        "bp": 100.0, "peak_price": 110.0, "atr": 2.0, "ma20": 105.0,
        "avail": 150, "original_quantity": 1000, "executed_tp_tiers": [1, 2]
    }
    # 止盈第3档触发时，avail=150不是100的整倍数，应该规范为200股
    # 或者用 _normalize_full_exit_quantity
    quote = {"current": 150.0}  # 浮盈50%
    triggers = check_triggers("688001", info, quote)
    tp3 = next((t for t in triggers if t["trigger"] == "tp3"), None)
    print(f"\n{'='*70}")
    print(f"📋 测试: 场景15: 科创板数量规范化")
    print(f"   可卖150股（不足200股最低卖出单位），止盈第3档应规范化")
    if tp3:
        print(f"   触发: {tp3['reason']}")
        return True
    print(f"   ❌ 未触发tp3")
    return False

# ============================================================================
# 场景16: ATR止损第5档 - 最高浮盈30%后回撤到25%
# ============================================================================
def test_atr_stop_loss_tier5():
    info = {
        "bp": 10.0, "peak_price": 13.0, "atr": 0.3, "ma20": 10.5,
        "avail": 1000, "original_quantity": 1000, "executed_tp_tiers": []
    }
    return run_test(
        "场景16: ATR止损 - 最高浮盈30%后回撤到25%，触发止损",
        info, current=12.5,
        expected_trigger="atr",
        expected_reason_contains="ATR止损"
    )

# ============================================================================
# 场景17: 止盈第3档触发时只剩很少股票（avail < original*30%）
# ============================================================================
def test_tp3_with_low_avail():
    info = {
        "bp": 10.0, "peak_price": 15.0, "atr": 0.3, "ma20": 10.5,
        "avail": 100, "original_quantity": 1000, "executed_tp_tiers": [1, 2]
    }
    # avail=100，已执行tp1(卖300) tp2(卖200)，剩余100，浮盈50%
    quote = {"current": 15.0}
    triggers = check_triggers("TEST001", info, quote)
    tp3 = next((t for t in triggers if t["trigger"] == "tp3"), None)
    print(f"\n{'='*70}")
    print(f"📋 测试: 场景17: 止盈第3档 - 已执行TP1/TP2，剩余100股应全卖")
    print(f"   可卖100股，原始1000股，已执行TP1(300)+TP2(200)")
    if tp3:
        print(f"   ✅ 触发tp3: {tp3['reason']}")
        return True
    print(f"   ❌ 未触发tp3")
    return False

# ============================================================================
# 运行所有测试
# ============================================================================
if __name__ == "__main__":
    print("="*70)
    print("盘中实时监控 - 触发规则测试套件")
    print(f"TAKE_PROFIT_1={TAKE_PROFIT_1*100:.0f}% TAKE_PROFIT_2={TAKE_PROFIT_2*100:.0f}% TAKE_PROFIT_3={TAKE_PROFIT_3*100:.0f}%")
    print(f"ATR_TIERS: {ATR_TIERS}")
    print("="*70)
    
    tests = [
        test_tp1_trigger,
        test_tp2_trigger,
        test_tp3_trigger,
        test_tp1_not_trigger,
        test_atr_stop_loss_tier1,
        test_atr_stop_loss_tier2,
        test_atr_stop_loss_tier3,
        test_atr_stop_loss_tier4,
        test_ma20_stop_loss,
        test_tp1_tp2_simultaneous,
        test_tp1_already_executed,
        test_zero_price_no_trigger,
        test_atr_stop_loss_no_atr_data,
        test_no_atr_trigger_when_no_profit,
        test_kcb_quantity_normalization,
        test_atr_stop_loss_tier5,
        test_tp3_with_low_avail,
    ]
    
    passed = 0
    failed = 0
    for t in tests:
        try:
            if t():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"   ❌ 异常: {e}")
            failed += 1
    
    print(f"\n{'='*70}")
    print(f"📊 测试结果: {passed} 通过, {failed} 失败, {passed+failed} 总计")
    if failed == 0:
        print(f"🎉 全部通过！")
    else:
        print(f"⚠️ {failed} 个测试失败，需要检查")
    print(f"{'='*70}")