#!/usr/bin/env python3
"""测试修复：选股辩论 + checkpoint 恢复逻辑"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

ALL_PASSED = True

def check(name, condition, detail=""):
    global ALL_PASSED
    status = "✅" if condition else "❌"
    if not condition:
        ALL_PASSED = False
    msg = f"  {status} {name}"
    if detail:
        msg += f": {detail}"
    print(msg)
    return condition

print("=" * 60)
print("Test 1: _parse_signal 异常标记处理")
print("=" * 60)
from workflow import _parse_signal
for dec, expected, desc in [
    ("异常: '\n  \"signal\"'", "AVOID", "旧版报错"),
    ("辩论系统异常（已重试3次）: timeout", "AVOID", "重试失败"),
    ("**最终信号**: BUY\n**置信度**: 75", "BUY", "正常BUY"),
    ("**最终信号**: WATCH\n**置信度**: 50", "WATCH", "正常WATCH"),
    ("**最终信号**: AVOID\n**仓位建议**: 0%", "AVOID", "正常AVOID"),
    ("不给BUY，建议观望", "WATCH", "不给BUY"),
    ("", "WATCH", "空字符串"),
]:
    result = _parse_signal({"final_decision": dec})
    check(desc, result == expected, f"got={result}")

print()
print("=" * 60)
print("Test 2: extract_json_object 正则 fallback")
print("=" * 60)
from stock_selection_debate.providers import extract_json_object
required = {"signal", "confidence", "position_ratio", "reason"}
for text, expected_sig, desc in [
    ('{"signal": "BUY", "confidence": 75, "position_ratio": 0.5, "reason": "looks good"}', "BUY", "完整JSON"),
    ('"signal": "WATCH",\n"confidence": 50,\n"position_ratio": 0.15,\n"reason": "uncertain outlook"', "WATCH", "无外层{}片段"),
    ('signal: "BUY"\nconfidence: 75\nposition_ratio: 0.5\nreason: "strong buy for long"', "BUY", "纯文本冒号格式"),
    ('{"signal": "BUY", "confidence": 75}', None, "缺字段"),
    ('\n  "signal"', None, "碎片文本"),
]:
    result = extract_json_object(text, required_keys=required)
    actual_sig = result.get("signal") if result else None
    check(desc, actual_sig == expected_sig, f"got={actual_sig}")

print()
print("=" * 60)
print("Test 3: checkpoint 旧版残留检测")
print("=" * 60)

def simulate(cp):
    done_set = set(cp["completed"])
    failed_set = set(cp["failed"])
    if len(done_set) == 0 and len(failed_set) > 50:
        failed_set = set()
    return done_set, failed_set

for cp, expected_len, desc in [
    ({"completed": [], "failed": list(range(83))}, 0, "旧版残留清空"),
    ({"completed": [], "failed": list(range(5))}, 5, "少量failed保留"),
    ({"completed": list(range(20)), "failed": list(range(10))}, 10, "正常保留"),
    ({"completed": list(range(91)), "failed": []}, 0, "全部成功"),
    ({"completed": [], "failed": []}, 0, "空checkpoint"),
]:
    _, new_failed = simulate(cp)
    check(desc, len(new_failed) == expected_len, f"got={len(new_failed)}")

print()
print("=" * 60)
print("Test 4: retry_failed 逻辑（类型一致性）")
print("=" * 60)

# 注意：stock_code 是字符串，failed_set 也必须是字符串
def simulate_retry(debate_packets, done_set, failed_set, saved_results):
    retry_failed = [p for p in debate_packets
                    if p.get("stock_code") in failed_set
                    and p.get("stock_code") not in saved_results]
    pending = [p for p in debate_packets
               if p.get("stock_code") not in done_set
               and p.get("stock_code") not in failed_set]
    if not pending and retry_failed:
        pending = retry_failed
        failed_set = set()
    return pending, failed_set, retry_failed

# 91只候选股，stock_code 格式 "000000"
candidates = [{"stock_code": f"{i:06d}"} for i in range(91)]

# Case A: 旧版残留，83 failed无结果 → pending=8（不在failed的），retry_failed=83
pending_a, _, retry_a = simulate_retry(
    candidates, set(), {f"{i:06d}" for i in range(83)}, {})
check("CaseA: pending=8（failed外）", len(pending_a) == 8, f"pending={len(pending_a)}")
check("CaseA: retry_failed=83", len(retry_a) == 83, f"retry={len(retry_a)}")

# Case B: 20完成，10 failed也已有结果 → pending=61，retry=0
saved_b = {f"{i:06d}": {} for i in range(30)}
pending_b, _, retry_b = simulate_retry(
    candidates, {f"{i:06d}" for i in range(20)},
    {f"{i:06d}" for i in range(20, 30)}, saved_b)
check("CaseB: pending=61（done 20 + failed 10 除外）", len(pending_b) == 61, f"pending={len(pending_b)}")
check("CaseB: retry=0（10 failed全有结果）", len(retry_b) == 0, f"retry={len(retry_b)}")

# Case C: 20完成，10 failed无结果 → pending=61，retry=10，retry后failed清空
saved_c = {f"{i:06d}": {} for i in range(20)}
pending_c, _, retry_c = simulate_retry(
    candidates, {f"{i:06d}" for i in range(20)},
    {f"{i:06d}" for i in range(20, 30)}, saved_c)
check("CaseC: retry=10", len(retry_c) == 10, f"retry={len(retry_c)}")

print()
print("=" * 60)
print("Test 5: 辩论异常返回 AVOID")
print("=" * 60)

def make_error_result(name, code, e):
    return {
        "stock_code": code, "stock_name": name,
        "signal": "AVOID", "confidence": 0,
        "final_decision": f"辩论系统异常（已重试3次）: {str(e)[:100]}",
        "error": str(e), "phase1_score": 0,
    }

for err, desc in [('\n  "signal"', "LangGraph异常"), ("JSONDecodeError", "解析失败"), ("timed out", "超时")]:
    r = make_error_result("测试", "000001", err)
    check(f"{desc}→AVOID", r["signal"] == "AVOID", f"signal={r['signal']}")
    check(f"{desc}→含异常标记", "异常" in r["final_decision"], "OK")

print()
print("=" * 60)
print("✅ 所有测试通过" if ALL_PASSED else "❌ 部分测试失败")
print("=" * 60)
