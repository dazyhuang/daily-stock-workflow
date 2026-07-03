#!/usr/bin/env python3
"""
Zombie record bp fallback 回归测试
====================================

防 2026-06-08 14:50 那种 bug 回归：
- record.buy_price 是僵尸字段（5月29日 _zero_record 后保留为 5.23）
- 14:50 run_monitor_mode 算 bp 时，如果 buy_records 全部 rem=0（zombie 状态），
  fallback 必须用 broker 实时 cost，**不能**用 stock_record.buy_price

修复位置: intraday_executor.py run_monitor_mode line 3925-3931
触发场景: 000725 (京东方A) 14:50 14:50:00,797 log 显示
  现价=6.3, 成本=6.316 → 修复后 bp=6.316 浮盈 -0.25% (不触发止盈)
  修复前 bp=5.23 浮盈 +20.46% (误触发止盈第2档，错杀 1700 股)

⚠️ 同步约束: 改 intraday_executor.py line 3925-3931 逻辑时，本测试必须同步更新。
   推荐: 重构成 _compute_position_bp(stock_record, cost) 函数后 import 测试。
"""
import sys
sys.path.insert(0, ".")
from intraday_executor import _compute_position_bp as _compute_bp_from_records


# ============================================================================
# 测试用例
# ============================================================================
def test_zombie_000725_real_scenario():
    """场景1: 真实 000725 14:50 zombie 状态 → bp 必须 = broker cost 6.316"""
    stock_record = {
        "buy_price": 5.23,  # ← 僵尸 buy_price（5月29日 _zero 后保留）
        "buy_records": [
            {"date": "2026-05-25", "price": 5.23, "quantity": 9000, "remaining": 0,
             "source": "intraday_buy_timing"},
            {"date": "2026-06-08", "price": 6.316, "quantity": 8900, "remaining": 0,
             "source": "position_reconcile"},
        ],
    }
    cost = 6.316  # broker 实时返回
    bp = _compute_bp_from_records(stock_record, cost)

    print(f"   bp={bp:.3f} (修复后应该=6.316, 修复前会错误=5.23)")
    assert abs(bp - 6.316) < 1e-6, (
        f"❌ ZOMBIE 回归! bp={bp}，预期用 broker cost 6.316 兜底，"
        f"不能用 stock_record.buy_price=5.23"
    )
    return True


def test_normal_record_uses_lot_weighted():
    """场景2: 正常 record（buy_records 有 rem>0 lot）→ 用 lot 加权，不被 cost 兜底"""
    stock_record = {
        "buy_price": 5.23,  # 僵尸字段但不重要
        "buy_records": [
            {"date": "2026-05-25", "price": 5.23, "quantity": 9000, "remaining": 9000},
            {"date": "2026-06-05", "price": 6.31, "quantity": 8900, "remaining": 0},  # 卖光
        ],
    }
    cost = 6.316
    bp = _compute_bp_from_records(stock_record, cost)

    print(f"   bp={bp:.3f} (应该=5.23, 5月25日 lot rem=9000 占 100%)")
    assert abs(bp - 5.23) < 1e-6, (
        f"❌ 正常 record 应该用 lot 加权 bp={bp:.3f}，预期 5.23"
    )
    return True


def test_mixed_record_uses_only_rem_lots():
    """场景3: 混合 record（部分 rem=0 部分 rem>0）→ 只算 rem>0 lot"""
    # 6月8日 14:50 实际 record 2 状态: 5月25日 lot rem=0 + 6月5日 lot rem=8900
    stock_record = {
        "buy_price": 6.31,
        "buy_records": [
            {"date": "2026-05-25", "price": 5.23, "quantity": 9000, "remaining": 0},
            {"date": "2026-06-05", "price": 6.31, "quantity": 8900, "remaining": 8900},
        ],
    }
    cost = 6.316
    bp = _compute_bp_from_records(stock_record, cost)

    print(f"   bp={bp:.3f} (应该=6.31, 只算 6月5日 lot)")
    assert abs(bp - 6.31) < 1e-6, (
        f"❌ 混合 record 应该只算 rem>0 lot，bp={bp:.3f}，预期 6.31"
    )
    return True


def test_empty_buy_records_uses_cost():
    """场景4: buy_records 空 list → bp = cost（不是 buy_price=0）"""
    stock_record = {"buy_price": 0, "buy_records": []}
    cost = 6.316
    bp = _compute_bp_from_records(stock_record, cost)

    print(f"   bp={bp:.3f} (应该=6.316, 不是 buy_price=0)")
    assert abs(bp - 6.316) < 1e-6, f"❌ 空 record 应该用 cost, bp={bp}"
    return True


def test_zombie_does_not_trigger_tp2():
    """场景5: 真实触发场景——zombie 状态 6.30 当前价 → pct 必须 < 20%（不触发 TP2）"""
    stock_record = {
        "buy_price": 5.23,  # 僵尸
        "buy_records": [
            {"date": "2026-05-25", "price": 5.23, "quantity": 9000, "remaining": 0},
            {"date": "2026-06-08", "price": 6.316, "quantity": 8900, "remaining": 0},
        ],
    }
    cost = 6.316
    current = 6.30
    bp = _compute_bp_from_records(stock_record, cost)
    pct = (current - bp) / bp

    print(f"   bp={bp:.3f} pct={pct*100:+.2f}% (应该 < 20%, 不触发 TP2)")
    assert pct < 0.20, f"❌ 修复后 zombie bp={bp:.3f} 浮盈 {pct*100:.2f}%, 不应触发 TP2"
    assert pct > -0.05, f"❌ 浮盈 {pct*100:.2f}% 异常"
    return True


def test_pre_fix_logic_triggers_tp2_buggy():
    """场景6 (反向): 复刻修复前 buggy 逻辑，确认会触发 TP2（防止"反向修复"）
    
    这个测试故意用 buggy 逻辑，断言它会触发 TP2——用来证明：
    1. 我们捕获的 bug 真实存在
    2. 防止有人"修复"成 bp=0 / bp=stock_record.buy_price 之类（这些也会通过 zombie 测试）
    """
    def _buggy_bp(stock_record, cost):
        buy_records = stock_record.get("buy_records", [])
        if buy_records:
            total_cost = sum(
                br["price"] * br["remaining"]
                for br in buy_records
                if br.get("remaining", 0) > 0
            )
            total_qty = sum(
                br.get("remaining", 0)
                for br in buy_records
                if br.get("remaining", 0) > 0
            )
            # 修复前: fallback 用 stock_record.buy_price (僵尸字段 5.23)
            bp = total_cost / total_qty if total_qty > 0 else stock_record.get("buy_price", 0)
        else:
            bp = stock_record.get("buy_price", 0)
        return bp

    stock_record = {"buy_price": 5.23, "buy_records": []}
    cost = 6.316
    bp = _buggy_bp(stock_record, cost)
    pct = (6.30 - bp) / bp

    print(f"   修复前 bp={bp:.3f} pct={pct*100:+.2f}% (应该触发 TP2)")
    assert abs(bp - 5.23) < 1e-6, f"修复前应该 fallback 到 buy_price=5.23"
    assert pct >= 0.20, f"修复前应该触发 TP2 (pct={pct*100:.2f}%)"
    return True


def test_zombie_does_not_fall_back_to_zero():
    """场景7: 防止反向修复——bp 不能在 zombie 状态下变成 0"""
    stock_record = {
        "buy_price": 0,  # 即使 buy_price 是 0（被清空）也不能用
        "buy_records": [
            {"date": "2026-05-25", "price": 5.23, "quantity": 9000, "remaining": 0},
        ],
    }
    cost = 6.316
    bp = _compute_bp_from_records(stock_record, cost)

    print(f"   bp={bp:.3f} (不能=0, 应该=cost=6.316)")
    assert bp > 0, f"❌ bp 不应为 0（会导致除零）"
    assert abs(bp - 6.316) < 1e-6, f"❌ zombie 状态 bp 应该=cost, got {bp}"
    return True


# ============================================================================
# 主入口
# ============================================================================
if __name__ == "__main__":
    print("=" * 70)
    print("Zombie record bp fallback 回归测试")
    print("=" * 70)

    tests = [
        test_zombie_000725_real_scenario,
        test_normal_record_uses_lot_weighted,
        test_mixed_record_uses_only_rem_lots,
        test_empty_buy_records_uses_cost,
        test_zombie_does_not_trigger_tp2,
        test_pre_fix_logic_triggers_tp2_buggy,
        test_zombie_does_not_fall_back_to_zero,
    ]

    passed = failed = 0
    for t in tests:
        print(f"\n📋 {t.__name__}")
        try:
            if t():
                passed += 1
                print(f"   ✅ 通过")
            else:
                failed += 1
                print(f"   ❌ 失败")
        except AssertionError as e:
            failed += 1
            print(f"   ❌ AssertionError: {e}")
        except Exception as e:
            failed += 1
            print(f"   💥 异常: {type(e).__name__}: {e}")

    print(f"\n{'=' * 70}")
    print(f"📊 测试结果: {passed} 通过, {failed} 失败, {passed+failed} 总计")
    if failed == 0:
        print("🎉 全部通过！zombie fallback 不会回归")
    else:
        print(f"⚠️  {failed} 个测试失败，需要检查 intraday_executor.py line 3925-3931 逻辑")
    print("=" * 70)
    sys.exit(0 if failed == 0 else 1)
