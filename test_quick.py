#!/usr/bin/env python3
"""快速测试：验证 _update_risk 前导 \n 修复 + extract_json_object 正则"""
import sys
sys.path.insert(0, '.')

print("=" * 60)
print("Test 1: _update_risk 去除前导 \\n")
print("=" * 60)

from stock_selection_debate.debate_engine import _update_risk

# Test case: history 拼接后以 \n 开头
test_history = (
    "\n【多方分析师】### 核心看多论据\n1. 基本面边际改善"
)
state = {"stock_code": "000001", "history": ""}

result = _update_risk(state, history=test_history)
hist = result["history"]
assert not hist.startswith('\n'), f"history 仍以 \\n 开头: {hist[:30]!r}"
assert hist.startswith("【多方分析师】"), f"history 清理后内容异常: {hist[:30]!r}"
print("  ✅ _update_risk 前导 \\n 修复验证通过")

print()
print("=" * 60)
print("Test 2: extract_json_object 正则 fallback")
print("=" * 60)

from stock_selection_debate.providers import extract_json_object

required = {"signal", "confidence", "position_ratio", "reason"}
cases = [
    ('{"signal": "BUY", "confidence": 75, "position_ratio": 0.5, "reason": "looks good"}', "BUY", "完整JSON"),
    ('"signal": "WATCH",\n"confidence": 50,\n"position_ratio": 0.15,\n"reason": "uncertain outlook"', "WATCH", "无外层{}片段"),
    ('signal: "BUY"\nconfidence: 75\nposition_ratio: 0.5\nreason: "strong buy for long"', "BUY", "纯文本冒号格式"),
    ('{"signal": "BUY", "confidence": 75}', None, "缺字段"),
    ('\n  "signal"', None, "碎片文本"),
]
passed = 0
for text, expected_sig, desc in cases:
    result = extract_json_object(text, required_keys=required)
    actual_sig = result.get("signal") if result else None
    ok = actual_sig == expected_sig
    if ok: passed += 1
    print(f"  {'✅' if ok else '❌'} {desc}: expected={expected_sig}, got={actual_sig}")

print()
print("=" * 60)
print(f"结果: {passed}/{len(cases)} 通过")
print("=" * 60)
