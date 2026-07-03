# Phase3 价格数据为 N/A 的修复方案

## 问题现象
- `daily_report_20260507.json` 的 `phase3_selection` 和 `phase3_strategy` 全部 stock 为 "N/A"
- 原因：Phase2 辩论结果的 `stock_code` 字段是 "N/A"，导致回测无法获取价格

## 根本原因分析

### Bug 1: `build_debate_packet` 返回值 key 不一致（关键根因）

**文件**: `stock_selection_debate/data_fetcher.py` 第 532 行

```python
return {
    "stock": stock_code,   # ← 问题在这里！用了 "stock" 作为 key
    "name": stock_name,
    ...
}
```

但 `StockDebateEngine._run_single()` （`debate_engine.py` 第 910 行）构造 `debate_packet` 时，用的是 `packet.get("stock_code")`：

```python
result = {
    "stock_code": packet.get("stock_code", "N/A"),  # ← 永远取到 "N/A"！
    ...
}
```

数据包中实际存的是 `packet["stock"]`，但 engine 读的是 `packet["stock_code"]`，所以 stock_code 永远是 "N/A"。

### Bug 2: `run_phase2_debate` 结果转换时字段名不一致

**文件**: `workflow.py` 第 ~1030 行

```python
for r in results:
    c = {
        "stock": r.get("stock_code", ""),   # ← engine 返回的是 "stock_code"，这里取的也是 stock_code
        "name": r.get("stock_name", ""),    # ← engine 返回的是 "stock_name"，但这里取的是 "stock_name"
        ...
    }
    ranked.append(c)
```

实际 `results` 里的字段是 `stock_code` 和 `stock_name`，而上面取的是 `stock_code`（正确）和 `stock_name`（正确）。但因为 Bug 1 导致 `stock_code` 永远是 "N/A"，所以这里虽然字段名对，但数据是错的。

### Bug 3: `debate_engine.py` 中 `packet.get("stock_code")` 永远拿到 "N/A"

**文件**: `debate_engine.py` 第 846 行和第 882 行

```python
code = packet.get("stock_code", "N/A")  # ← 永远拿到 "N/A"（数据包用的是 "stock" key）
name = packet.get("name", packet.get("stock_name", code))
```

这里如果改成 `packet.get("stock")`，就能正确获取股票代码。

## 修复方案

### 修复 1: `data_fetcher.py` - `build_debate_packet` 返回值 key 统一改为 "stock_code"

将返回字典的 `"stock"` key 改为 `"stock_code"`，与 `debate_engine.py` 的访问方式一致。

```python
# 修改前
return {
    "stock": stock_code,
    "name": stock_name,
    ...
}

# 修改后
return {
    "stock_code": stock_code,   # 统一用 stock_code
    "name": stock_name,
    ...
}
```

### 修复 2: `debate_engine.py` - `packet.get("stock_code")` → 直接使用（已一致，删除 fallback）

第 846 行和第 882 行：

```python
# 修改前
code = packet.get("stock_code", "N/A")
name = packet.get("name", packet.get("stock_name", code))

# 修改后（保持一致即可，不需要改）
# 但 name 获取有个问题：packet["name"] 是股票名称，应该用于显示
# 而 packet.get("stock_name") 是不存在的 key，这里有逻辑问题
# 实际上 packet 中有 "name" 字段表示股票名称，应直接用 packet.get("name")
```

**实际上**：数据包中股票名称存在 `"name"` 字段，不是 `"stock_name"`。`name = packet.get("name", packet.get("stock_name", code))` 能正确取到名称（fallback 到 code）。这个逻辑没问题。

所以只需要确认 `packet.get("stock_code")` 能取到值（即修复1），这里就能工作。

### 修复 3: `workflow.py` - `run_phase2_debate` 中 `r.get("stock_code")` 改为 `r.get("stock")`

**文件**: `workflow.py` 第 ~1030 行

Engine 返回的结果字段是 `stock_code` 和 `stock_name`，`workflow.py` 的 `ranked` 构造用的是 `r.get("stock_code")` 和 `r.get("stock_name")`，字段名本身是对的。

但由于 Bug 1 导致 engine 收到的 `packet.get("stock_code")` 永远为 "N/A"，所以 engine 返回的 `stock_code` 字段本身就是错的。

**修复后**：数据包 key 统一为 `"stock_code"`，`packet.get("stock_code")` 能正确取值，engine 返回正确的 `stock_code`，`workflow.py` 的 `r.get("stock_code")` 也能正确取到。

### 修复 4: `workflow.py` - `_get_stock_hist_prices_mx` 处理 "N/A"

即使修复了 Bug 1-3，如果某些边缘情况（空候选股、异常数据）仍然传入了 "N/A"，`_get_stock_hist_prices_mx` 应该直接返回 None 而不是尝试查询。

当前 `_get_stock_hist_prices_mx("N/A", ...)` 返回 `None`，回测逻辑会处理这个情况（返回 "价格数据不足"），这是 OK 的。

## 修复完成状态

### ✅ 已修复

**修复 1** (`data_fetcher.py`): `build_debate_packet` 返回值 key 统一为 `"stock_code"`
- 修改：`return {"stock": stock_code, ...}` → `return {"stock_code": stock_code, ...}`
- 验证：`build_debate_packet('002463', '沪电股份', {}, [])` 返回 `{"stock_code": "002463"}`, 无 `"stock"` key

**修复 2** (`workflow.py`): `_get_stock_hist_prices_mx` 添加 N/A 防御
- 修改：函数开头添加 `if not stock_code or stock_code in ("N/A", "", "None"): return None`
- 验证：`_get_stock_hist_prices_mx('N/A', 5)` → `None`

### ✅ 验证数据

```
build_debate_packet key check:
  stock_code in packet: True
  stock in packet: False
  stock_code value: 002463

N/A guard test:
  N/A result: None
  002463 result (last 5): [104.95, 101.88, 101.98, 102.57, 104.79]
```

修复后数据流：
1. `build_debate_packet("002463", "沪电股份", ...)` → packet 包含 `"stock_code": "002463"` ✅
2. `debate_engine._run_single()` 用 `packet.get("stock_code")` → 正确取到 `"002463"` ✅
3. `workflow.py run_phase2_debate` 中 `r.get("stock_code")` → 正确取到 `"002463"` ✅
4. Phase3 回测 `run_backtest_selection([{"stock": "002463", ...}])` → `_get_stock_hist_prices_mx("002463")` → 正确返回价格列表 ✅

### ⏳ 待验证

完整 workflow 需要重新运行才能验证 Phase3 实际获取到价格数据。

快速验证命令：
```bash
cd .
python3 workflow.py --dry-run
```

预期 Phase3 输出：
```python
# phase3_selection.stocks[0] 应该是类似:
{"stock": "002463", "entry": 101.98, "exit": 104.79, "return_pct": 2.76, "signal": "BUY"}
# 而不是:
{"stock": "N/A", "error": "价格数据不足"}
```