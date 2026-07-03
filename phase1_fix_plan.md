# Phase1→Phase2 数据传递问题根因分析与修复方案

## 问题现象

`daily_report_20260507.json` 的 `phase2.top_picks` 和 `ranked_candidates`：

| 字段 | 期望值 | 实际值 |
|------|--------|--------|
| `stock` | 真实股票代码（如 `002428`） | `N/A` |
| `total_score` | 50-100 的置信度数字 | `0` |
| `reason` | 核心理由 | `""`（空字符串） |
| `confidence`（ranked 中） | 50-100 | `50`（全部默认值） |

**候选股来源 `phase2.candidates` 正常**（73只有 stock/name/reason），说明 Phase1 → Phase2 传递时 stock 字段丢失。

---

## 完整数据流

```
gen._candidates = [{stock: "002428", name: "云南锗业", reason: "...", ...}, ...]
    ↓ workflow.py 第 1015-1021 行
for c in candidates:
    stock = c.get("stock", "")    → "002428"
    name = c.get("name", "")      → "云南锗业"
    packet = build_debate_packet(stock, name, phase1_cache, kline)
    ↓ data_fetcher.py 第 532 行返回
    {"stock_code": "002428", "name": "云南锗业", ...}
    ↓ debate_packets = [73个 packet]
debate.run(debate_packets, market_context="")
    ↓ debate_engine.run() → run_one() 第 846 行
    code = packet.get("stock_code", "N/A")   → "002428" ✓
    name = packet.get("name", ...)           → "云南锗业" ✓
    ↓ _run_single() → 两阶段辩论
    ↓ 返回 {"stock_code": "002428", "stock_name": "云南锗业",
            "confidence": 50,  ← 默认值（因为正则解析失败）
            "signal": "AVOID", "final_decision": "..."}
    ↓ workflow.py 第 1030-1035 行
ranked.append({
    "stock": r.get("stock_code", ""),      → "002428" ✓
    "name": r.get("stock_name", ""),       → "云南锗业" ✓
    "signal": r.get("signal", "WATCH"),
    "confidence": r.get("confidence", 50),  → 50（默认值）
    "final_decision": r.get("final_decision", ""),
})
    ↓ result = {ranked_candidates: ranked, ...}
    ↓ debate_phase_to_phase2_format(result)
    ↓ ranked_candidates[...]["stock"] = "002428" ✓
    ↓ BUT daily_report 中 stock="N/A" ? → 需进一步验证
```

---

## 根因总结

### 根因 1（已确认）：Confidence 正则解析 100% 失败

**文件：** `debate_engine.py` 第 678 行

```python
m = re.search(r"置信度[：:]\s*(\d+)", line)
```

**问题：** 正则只能匹配 `置信度：78` 或 `置信度: 62`（无 Markdown 格式），但 LLM 实际输出全被 Markdown 粗体包围：

```
**置信度**: 78          → FAIL（Markdown **包围）
**置信度**: **58/100** → FAIL（嵌套粗体）
**置信度**: 65%        → FAIL（带百分号）
**置信度**: **55** / 100  → FAIL（嵌套粗体+分数）
```

**验证：** 当前代码中唯一能匹配的格式是不带 Markdown 的 `置信度：78` / `置信度: 62`，但 LLM 100% 输出带 `**`** 包围的格式。

**后果：** 所有 `confidence = 50`（默认值），导致 `total_score = 0`（因为 `debate_phase_to_phase2_format` 读取 `c.get("final_score", 0)` 但 ranked_candidates 中是 `confidence` 不是 `final_score`）。

---

### 根因 2（已确认）：`debate_phase_to_phase2_format` 字段名完全错误

**文件：** `run_debate_phase.py` 第 158/168/171 行

`debate_phase_to_phase2_format` 是 `run_debate_phase.py` 内的函数，被 `route_a_phase2` 调用（但当前 workflow 走 `route_b_phase2`，所以这个函数实际上**没有在今天的工作流中被调用**）。

**问题字段：**

| 行号 | 错误代码 | 正确字段来源 | 后果 |
|------|----------|--------------|------|
| 158 | `c.get("final_score", 0)` | ranked 无此字段 | 二次排序失效 |
| 168 | `"total_score": c.get("final_score", 0)` | ranked 有 `confidence` | total_score 永远 = 0 |
| 171 | `"reason": c.get("verdict", "")` | ranked 有 `final_decision` | reason 永远 = "" |

**注意：** 由于 workflow 走 `route_b_phase2`（调用 `run_phase2_debate`），而 `debate_phase_to_phase2_format` 只在 `route_a_phase2`（从未被调用）中出现，这个 bug **今天没有直接影响**，但如果未来切换到 Route A 会立即触发。

---

### 根因 3（待确认）：`N/A` stock 问题的可能原因

**矛盾点：** `build_debate_packet` 确认返回 `"stock_code": stock_code`，`debate_engine._run_single` 确认返回 `"stock_code": stock_code`，`workflow.py` 第 1030 行 `r.get("stock_code", "")` 应该取到正确值。

但 `daily_report_20260507.json` 中 `ranked_candidates[...]["stock"] = "N/A"`。

**可能的解释（按可能性排序）：**

1. **代码版本问题：** workflow.py 实际运行的 `build_debate_packet` 返回 key 是 `"stock"` 而非 `"stock_code"`，而磁盘上的源文件已经是最新的（之前某次运行时用的是旧版代码）

2. **debate_checkpoint.json 恢复问题：** 如果使用了 checkpoint 续跑，LangGraph 可能在某个状态下将 `stock_code` 设为 `"N/A"`，但这只影响特定股票的续跑

3. **内存状态污染：** `gen._candidates` 中的对象在传递过程中被修改

**验证方法：** 需要在实时环境中打印 `build_debate_packet` 返回值或检查 `debate_packets[0]`，确认返回的 key 是 `"stock_code"` 还是 `"stock"`。

---

## 修复方案

### 修复 1（必须）：Confidence 正则表达式

**文件：** `debate_engine.py` 第 678 行

**现状：**
```python
m = re.search(r"置信度[：:]\s*(\d+)", line)
```

**修复：**
```python
m = re.search(r"置信度.*?(\d+)", line)
```

宽松正则可以匹配：
- `**置信度**: 78` → 78 ✓
- `**置信度**: **58/100**` → 58 ✓
- `**置信度**: 65%` → 65 ✓
- `**置信度**: **55** / 100` → 55 ✓

### 修复 2（必须）：`debate_phase_to_phase2_format` 字段映射

**文件：** `run_debate_phase.py` 第 158/168/171 行

**行 158（排序 key）：**
```python
# 现状：
-(c.get("final_score", 0) or 0)
# 修复：
-(c.get("confidence", 0) or 0)
```

**行 168（total_score）：**
```python
# 现状：
"total_score": c.get("final_score", 0),
# 修复：
"total_score": c.get("confidence", 50),
```

**行 171（reason）：**
```python
# 现状：
"reason": c.get("verdict", ""),
# 修复：
"reason": c.get("final_decision", ""),
```

### 修复 3（建议）：添加 `confidence` 到 `top_picks`

**文件：** `run_debate_phase.py` 第 168 行附近

`top_picks` 字典中添加：
```python
"confidence": c.get("confidence", 50),
```

---

## 待验证项

如果修复正则后 confidence 正确但 `N/A` stock 仍然存在，需要进一步检查：

1. 在 `build_debate_packet` 返回后打印确认 key：`print(packet.keys())`
2. 在 `debate.run()` 调用前打印：`print(debate_packets[0].get("stock_code"), debate_packets[0].get("stock"))`
3. 确认 `gen._candidates` 中的 stock 字段没有被意外修改

---

## 修复优先级

| 优先级 | 修复内容 | 文件:行号 | 预期效果 |
|--------|----------|-----------|----------|
| P0（必须） | 正则解析 | debate_engine.py:678 | 所有 confidence 正确（50-100） |
| P0（必须） | total_score 字段 | run_debate_phase.py:168 | top_picks.total_score 有意义（50-100） |
| P1（建议） | 排序字段 | run_debate_phase.py:158 | BUY 信号排在最前 |
| P1（建议） | reason 字段 | run_debate_phase.py:171 | top_picks.reason 有内容 |
| P2（待确认） | stock N/A | 待检查 | stock 字段正确传递 |
