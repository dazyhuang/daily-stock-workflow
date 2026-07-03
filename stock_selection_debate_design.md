# 选股辩论流程设计方案（完全版）

## 一、整体流程

```
┌──────────────────────────────────────────────────────────┐
│  Phase 1: mx_xuangu 选股（六个股票池，不变）              │
│  → 候选股池（约20-30只，去重后）                          │
└────────────────────────┬─────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────────┐
│  Phase 2: 数据准备（新增）                                │
│  ① xqshare 客户端获取每只候选股的：                        │
│     - 日K线（最近6个月）                                  │
│     - 财务数据（ROE、营收增速、净利润、现金流等）          │
│  ② 读取证券技术分析类知识库（注入上下文）：                  │
│     - candlestick-charting（蜡烛图形态）                   │
│     - stock-trend-technical-analysis（趋势/形态/均线）    │
│     - technical-analysis-murphy（技术分析理论）           │
│  → 生成"股票辩论数据包"JSON                               │
└────────────────────────┬─────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────────┐
│  Phase 3: 完全版辩论（核心新增）                          │
│  → 输出：Top5 + BUY/WATCH/AVOID + 买入比例                │
└──────────────────────────────────────────────────────────┘
```

---

## 二、辩论架构（5角色 × 4步）

### 参与角色

| 角色 | 视角 | 任务 |
|------|------|------|
| **行业研究员** | 成长/催化剂 | 基本面深度研究 + 目标价 |
| **技术分析师** | 趋势/形态 | K线形态识别 + 买卖点 |
| **量化风控师** | 数据/风险 | 财务数据异常检测 + 估值风险 |
| **市场情绪官** | 资金/情绪 | 北向资金、板块情绪、筹码分布 |
| **裁判（基金经理）** | 综合决策 | 汇总四方意见，输出最终判决 |

---

### 辩论流程（4步）

```
Step 1 ──────────────────────────────────────────────────
│ 各角色独立给出评估（互相不可见）
│
├─ 行业研究员：成长性评估（0-100分）
│   • 业绩增速、ROE趋势、市场空间
│   • 目标价 + 上涨逻辑
│
├─ 技术分析师：技术面评估（0-100分）
│   • K线形态（来自知识库：头肩顶/底、三角形、支撑阻力等）
│   • 均线系统：MA5/10/20/60 多头排列？趋势方向？
│   • 成交量：放量突破？还是缩量整理？
│   • 关键技术位：压力/支撑
│
├─ 量化风控师：风险评估（0-100分，及风险点列举）
│   • 财务数据异常：商誉减值？应收账款过高？现金流恶化？
│   • 估值风险：PE/PB 历史分位数？是否泡沫？
│   • 流动性风险：市值太小？机构持仓过低？
│
└─ 市场情绪官：情绪评估（0-100分）
    • 北向资金：近期净流入/流出？
    • 板块情绪：热门板块？还是边缘行业？
    • 主力动向：游资？庄股？机构持仓？

───────────────────────────────────────────────────────────
Step 2 ──────────────────────────────────────────────────
│ 矛盾点识别（裁判主导，发现各方矛盾）
│
│ 裁判识别：
│ • 行业研究员看多 vs 技术分析师看空 → 矛盾A
│ • 行业研究员看好 vs 量化风控师提示风险 → 矛盾B
│ • 技术分析师提示顶部 vs 市场情绪官提示资金流入 → 矛盾C
│
│ 输出：矛盾点清单（每只股票 1-3 个核心矛盾）

───────────────────────────────────────────────────────────
Step 3 ──────────────────────────────────────────────────
│ 角色辩论（针对矛盾点，正反方交叉辩论）
│
│ 每只股票：
│ • 矛盾A辩论：研究员（正） vs 风控师（反）
│ • 矛盾B辩论：（视矛盾类型匹配角色）
│
│ 辩论格式：
│ • 正方：最强论据 + 反驳对方质疑
│ • 反方：最强论据 + 反驳对方论据

───────────────────────────────────────────────────────────
Step 4 ──────────────────────────────────────────────────
│ 裁判判决
│
│ 综合评分 = 行业(25%) + 技术(25%) + 风控(25%) + 情绪(25%)
│
│ 调整规则：
│ • 辩论后，若某方论据被有效反驳，该方分数降低
│ • 矛盾未解决 → 置信度下降
│
│ 最终输出：
│ • BUY（≥65分）/ WATCH（40-64分）/ AVOID（<40分）
│ • 买入比例：BUY 15-25%，WATCH 5-10%，AVOID 0%
│ • 置信度：高/中/低
│ • 裁判一句话结论
```

---

## 三、知识库注入（辩论前的技术分析上下文）

### 读取策略

在辩论前，从以下知识库加载相关内容作为技术分析师的判断依据：

```
知识库读取顺序：
1. candlestick-charting（蜡烛图形态识别）
   - 单根K线：锤子线、射击星、吞没形态
   - 组合形态：早晨之星、黄昏之星、红三兵、三乌鸦
   - 关键形态：头肩顶/底、双顶/底、三角形

2. stock-trend-technical-analysis（趋势技术分析）
   - 道氏理论：趋势定义、支撑阻力
   - 趋势线：绘制方法、通道理论
   - 均线系统：多头排列、空头排列、金叉死叉

3. technical-analysis-murphy（技术分析理论）
   - 移动平均线：SMA/EMA 的应用
   - 趋势确认：成交量与价格趋势的关系
   - 震荡指标：RSI、MACD 的用法
```

### 注入格式

```python
# 伪代码：知识库注入
def load_technical_context() -> str:
    kb_dir = Path("~/.openclaw/workspace/knowledge-base")

    candlestick = (kb_dir / "candlestick-charting" / "核心概念.md").read_text()
    stock_trend = (kb_dir / "stock-trend-technical-analysis" / "核心概念.md").read_text()
    murphy = (kb_dir / "technical-analysis-murphy" / "核心概念.md").read_text()

    # 提取形态识别的关键规则
    pattern_rules = extract_pattern_rules(candlestick)
    trend_rules = extract_trend_rules(stock_trend)
    indicator_rules = extract_indicator_rules(murphy)

    return f"""
技术分析判断标准：
- 蜡烛图形态：{pattern_rules}
- 趋势判断标准：{trend_rules}
- 技术指标规则：{indicator_rules}
"""
```

---

## 四、数据准备：xqshare 获取 K 线 + 财务数据

### 接口调用

```python
# 伪代码
from xqshare import XQShareClient

client = XQShareClient()

for stock in candidates:
    # ① K线数据
    kline = client.get_market_data(
        stock_code=stock,
        period="daily",
        start_date=(today - timedelta(days=180)).strftime("%Y%m%d"),
        end_date=today.strftime("%Y%m%d"),
        fields=["open", "high", "low", "close", "volume"],
    )

    # ② 财务数据
    financial = client.get_financial_data(
        stock_code=stock,
        fields=[
            "roe_latest",          # 最新ROE
            "revenue_growth",      # 营收增速
            "net_profit_growth",   # 净利润增速
            "gross_margin",        # 毛利率
            "debt_ratio",         # 资产负债率
            "cash_flow",          # 经营现金流
            "book_value_per_share",# 每股净资产
            "pe_ttm",             # 市盈率TTM
            "pb",                 # 市净率
            "market_cap",         # 总市值
        ]
    )

    # ③ 筹码分布（若有）
    chips = client.get_chip_distribution(stock)
```

### 生成辩论数据包

```python
@dataclass
class StockDebatePacket:
    stock: str           # 代码
    name: str            # 名称
    sector: str          # 所属行业

    # K线数据（供技术分析师）
    kline_6m: List[KLineBar]   # 最近6个月日K
    kline_summary: dict         # K线摘要

    # 财务数据（供行业研究员 + 风控师）
    financial: FinancialData

    # 情绪数据（供市场情绪官）
    sentiment: SentimentData

    # 技术形态摘要（知识库规则识别）
    candlestick_patterns: List[str]    # 识别到的蜡烛图形态
    trend_signals: List[str]           # 趋势信号
    indicator_values: dict             # RSI/MACD 等指标值


def build_debate_packet(stock: str) -> StockDebatePacket:
    # 1. xqshare 获取原始数据
    kline = get_kline_xqshare(stock)
    financial = get_financial_xqshare(stock)

    # 2. 技术形态识别（应用知识库规则）
    patterns = identify_candlestick_patterns(kline, KB_PATTERNS)
    trends = identify_trend_signals(kline, KB_TRENDS)
    indicators = calculate_indicators(kline)  # RSI, MACD, MA

    # 3. 北向/情绪数据（mx-data 或 akshare）
    sentiment = get_market_sentiment(stock)

    return StockDebatePacket(
        stock=stock,
        kline_6m=kline,
        kline_summary=summarize_kline(kline),
        financial=financial,
        sentiment=sentiment,
        candlestick_patterns=patterns,
        trend_signals=trends,
        indicator_values=indicators,
    )
```

---

## 五、辩论 Prompt（完全版）

### Step 1：各角色独立评估

```
【行业研究员 Prompt】

你是一位资深A股行业研究员，负责对候选股票进行基本面深度研究。
知识库参考：{technical_context_from_kb}

候选股数据包（JSON）：
{stock_debate_packet_json}

请对每只股票输出：

{
  "stock": "代码",
  "name": "名称",
  "growth_score": 0-100,         # 成长性评分
  "target_price": "元",           # 目标价（基于什么逻辑）
  "upside_percent": "X%",         # 上涨空间
  "bull_case": [                  # 最强上涨逻辑（2-3条）
    "...",
    "..."
  ],
  "key_concerns": [               # 你自己承认的风险（为风控师留攻击点）
    "...",
    "..."
  ],
  "roe_trend": "上升/平稳/下降",
  "revenue_growth_quality": "优质/一般/恶化",
}
```

```
【技术分析师 Prompt】

你是一位资深A股技术分析师，负责识别K线形态和趋势信号。
知识库（必须严格应用以下规则）：
{technical_context_from_kb}

候选股数据包（包含K线数据）：
{stock_debate_packet_json}

请严格按知识库规则识别以下内容：

1. 蜡烛图形态（必须对照candlestick-charting知识库）：
   - 识别到的形态：早晨之星？黄昏之星？锤子线？射击星？吞没？
   - 形态有效性判断：是否在关键支撑/阻力位？

2. 趋势信号（必须对照stock-trend-technical-analysis知识库）：
   - 均线系统：MA5/10/20/60 的多头/空头排列？
   - 趋势方向：上升趋势/下降趋势/横盘震荡？
   - 趋势线：是否突破重要趋势线？

3. 成交量信号：
   - 放量突破？还是缩量整理？
   - 量价配合是否健康？

4. 技术指标：
   - RSI(14)：是否超买（>70）/超卖（<30）？
   - MACD：是否金叉/死叉？
   - 是否出现背离？

输出格式（JSON）：
{
  "stock": "代码",
  "name": "名称",
  "tech_score": 0-100,
  "trend_direction": "上升/下降/横盘",
  "ma_system": "多头排列/空头排列/混乱",
  "patterns_recognized": ["形态1", "形态2"],
  "pattern_meaning": "这些形态的技术含义",
  "key_support": "元（关键支撑位）",
  "key_resistance": "元（关键压力位）",
  "volume_signal": "放量/缩量/正常",
  "rsi_14": 数值,
  "macd_signal": "金叉/死叉/中性",
  "divergence": "顶背离/底背离/无",
  "bull_evidence": ["技术面利好证据1", "..."],
  "bear_evidence": ["技术面风险证据1", "..."],
}
```

```
【量化风控师 Prompt】

你是一位资深量化风控师，负责挖掘财务数据和估值风险。
候选股数据包（包含财务数据）：
{stock_debate_packet_json}

请对每只股票输出：

{
  "stock": "代码",
  "name": "名称",
  "risk_score": 0-100,            # 风险评分（越低越危险）
  "financial_red_flags": [         # 财务预警信号
    "商誉占总资产 X%（过高）",
    "应收账款增速 > 营收增速",
    "经营现金流连续为负",
    ...
  ],
  "valuation_risk": {
    "pe_ttm": 数值,
    "pe_historical_percentile": "历史分位数",
    "pb": 数值,
    "assessment": "高估/合理/低估",
  },
  "liquidity_risk": "低/中/高",
  "debt_concern": "资产负债率X%，是否安全",
  "accounting_quality": "优质/一般/存疑",
  "risk_summary": "一句话总结最大风险",
}
```

```
【市场情绪官 Prompt】

你是一位资深市场情绪分析师，负责判断资金流向和板块情绪。
候选股数据包：
{stock_debate_packet_json}

请对每只股票输出：

{
  "stock": "代码",
  "name": "名称",
  "sector": "所属行业",
  "sector_sentiment": "热门/一般/冷门",
  "sentiment_score": 0-100,
  "northbound_flow": "近5日净流入X万/净流出X万",
  "main_player_style": "游资主导/机构主导/混合",
  "chip_distribution": "集中/分散/不明",
  "sector_rotation_signal": "是否可能切换到该板块",
  "money_flow_summary": "一句话资金面总结",
  "bull_money_signal": ["资金面利好1", "..."],
  "bear_money_signal": ["资金面风险1", "..."],
}
```

### Step 2：裁判识别矛盾

```
【裁判 Prompt - 矛盾识别】

你是一位客观的基金经理，负责识别各方评估的核心矛盾。

以下是各角色对候选股的评估结果（JSON）：
{all_roles_assessment_json}

请识别每只股票的核心矛盾：

{
  "stock": "代码",
  "contradictions": [
    {
      "type": "行业vs技术",
      "positive_side": "研究员：目标价X元，上涨空间Y%",
      "negative_side": "技术分析师：出现顶背离，RSI超买",
      "key_question": "上涨逻辑能否战胜技术面压力？",
    },
    ...
  ],
  "consensus_points": [
    "各方均认可的风险：...",
    ...
  ],
}
```

### Step 3：角色辩论

```
【辩论 Prompt - 针对矛盾点】

矛盾清单：
{contradictions_json}

各方原始评估：
{all_assessments_json}

辩论规则：
- 正方：用最强论据回应对方质疑
- 反方：用最强论据反驳对方看好理由
- 每次发言不超过3条，每条不超过50字
- 客观中立，基于数据，不人身攻击

输出格式（每只股票的辩论）：
{
  "stock": "代码",
  "debate_rounds": [
    {
      "contradiction": "矛盾描述",
      "bull_side": "正方论点",
      "bear_side": "反方论点",
      "bull_wins": true/false/null,
      "winning_argument": "胜出的论据（1-2条）",
    },
    ...
  ],
}
```

### Step 4：裁判判决

```
【裁判 Prompt - 最终判决】

完整辩论记录：
{debate_record_json}

各方评分汇总：
{scores_json}

辩论结果：
{debate_outcome_json}

判决规则：
1. 基础分 = 行业(25%) + 技术(25%) + 风控(25%) + 情绪(25%)
2. 辩论后，按胜负调整：
   - 正方胜出：+5分
   - 反方胜出：-5分
   - 平局：无调整
3. 置信度判定：
   - 各方一致 → 高置信
   - 有分歧但能解决 → 中置信
   - 分歧无法解决 → 低置信

信号规则：
- 最终分 ≥ 65 → BUY（建议建仓）
- 最终分 40-64 → WATCH（观察，谨慎参与）
- 最终分 < 40 → AVOID（不参与）

仓位规则（BUY信号下）：
- 置信度高 + 各方一致 → 20-25%
- 置信度中 → 10-15%
- 置信度低 → 5-10%

最终输出格式（JSON）：
{
  "ranked_candidates": [
    {
      "rank": 1,
      "stock": "代码",
      "name": "名称",
      "final_score": 0-100,
      "signal": "BUY/WATCH/AVOID",
      "position_ratio": "X%",
      "conviction": "高/中/低",
      "bull_argument": "最强看好理由",
      "bear_argument": "最强风险理由",
      "verdict": "裁判一句话结论",
    },
    ...
  ],
  "summary": "整体裁判总结",
  "portfolio_suggestion": "总仓位建议 + 板块配置建议",
}
```

---

## 六、飞书卡片输出

```
📊 选股辩论报告 2026-05-06

━━━━━━━━ 辩论阵容 ━━━━━━━━
🔬 行业研究员：成长性/催化剂
📈 技术分析师：趋势/形态/指标
🛡️ 量化风控师：财务/估值风险
💹 市场情绪官：资金/筹码/情绪
⚖️ 基金经理：综合裁判

━━━━━━━━ 数据来源 ━━━━━━━━
• 选股：mx_xuangu 六个股票池
• K线+财务：xqshare（xtquant）
• 技术规则：蜡烛图+趋势技术+墨菲分析

━━━━━━━━ TOP 5 BUY ━━━━━━━━

🥇 1. 沪电股份 002463
   综合分：82 | 置信度：🟢高 | 仓位：25%
   信号：BUY
   裁判结论：PCB景气延续，量价配合健康，RSI未超买
   行业：目标价48元，上涨空间35%（AI算力需求）
   技术：均线多头排列，成交量温和放大，突破前高
   风控：PE合理，ROE持续上升，无重大财务风险
   情绪：北向资金连续净流入，主力持仓增加

🥈 2. 协创数据 300857
   综合分：78 | 置信度：🟢高 | 仓位：20%
   信号：BUY
   ...

━━━━━━━━ WATCH 观察（2只）━━━━━━━━

3. 中国巨石 600176
   综合分：62 | 置信度：🟡中 | 仓位：10%
   信号：WATCH
   裁判结论：技术面强势但估值偏高，谨慎参与
   ...

━━━━━━━━ AVOID 回避（1只）━━━━━━━━

🔴 4. 宏和科技 603256
   综合分：28 | 置信度：🟢高 | 仓位：0%
   信号：AVOID
   裁判结论：均线空头排列，RSI超卖未企稳，基本面不可得
   ...

━━━━━━━━ 组合建议 ━━━━━━━━
总仓位：55%（BUY 3只合计45% + WATCH 10%）
板块分布：PCB（40%）+ 数据要素（30%）+ 观望（30%）
```

---

## 七、文件结构

```
daily-stock-workflow/
├── workflow.py                      # 主工作流（不变）
├── debate_flow.py                   # 周持仓辩论（不变）
├── weekly_review_debate.py          # 周复盘辩论（不变）
│
├── stock_selection_debate/          # 新增：选股辩论模块
│   ├── __init__.py
│   ├── data_fetcher.py              # xqshare 数据获取
│   ├── kb_loader.py                 # 知识库加载
│   ├── pattern_recognizer.py        # K线形态识别（应用知识库规则）
│   ├── debate_engine.py              # 辩论流程引擎
│   ├── judge.py                     # 裁判判决逻辑
│   └── feishu_card.py               # 飞书卡片生成
│
└── stock_selection_debate_design.md  # 本文档
```

---

## 八、关键实现要点

### 1. 知识库加载

在辩论开始前，加载技术分析知识库作为规则依据：

```python
KB_DIR = Path("~/.openclaw/workspace/knowledge-base")

def load_technical_kb() -> dict:
    """加载并结构化技术分析知识库"""
    return {
        "candlestick": parse_kb(KB_DIR / "candlestick-charting" / "核心概念.md"),
        "trend": parse_kb(KB_DIR / "stock-trend-technical-analysis" / "核心概念.md"),
        "murphy": parse_kb(KB_DIR / "technical-analysis-murphy" / "核心概念.md"),
    }
```

### 2. 形态识别器

```python
def recognize_patterns(kline: List[KLineBar], kb: dict) -> List[dict]:
    """严格按知识库规则识别K线形态"""
    patterns = []
    for rule in kb["candlestick"]["rules"]:
        if rule["type"] == "single_bar":
            if detect_single_bar_pattern(kline[-1], rule):
                patterns.append(rule["name"])
        elif rule["type"] == "multi_bar":
            if detect_multi_bar_pattern(kline[-3:], rule):
                patterns.append(rule["name"])
    return patterns
```

### 3. xqshare 连接

```python
# 数据获取使用现有的 xqshare 客户端
# 路径：knowledge-base/xqshare/client.py
import sys
sys.path.insert(0, str(Path("~/.openclaw/workspace/knowledge-base/xqshare")))
from client import XQShareClient

client = XQShareClient(host="127.0.0.1", port=18812)
kline = client.get_market_data(stock_code, period="daily", ...)
financial = client.get_financial_data(stock_code, ...)
```

### 4. 辩论 Token 预算

完全版辩论约消耗：
- Step 1（四方并行）：约 8,000-15,000 tokens/股票 × N只
- Step 2（矛盾识别）：约 3,000 tokens/股票 × N只
- Step 3（辩论）：约 5,000 tokens/股票 × N只 × 2轮
- Step 4（判决）：约 2,000 tokens/股票 × N只

**建议**：每只股票约 25,000-35,000 tokens，Top 10 约 250,000-350,000 tokens

---

## 九、与现有工作流的衔接

```python
# workflow.py 中新增
from stock_selection_debate import StockSelectionDebate

def run_daily_workflow(...):
    # Phase 1: mx_xuangu 选股（不变）
    candidates = run_phase1_xuangu()

    # [新增] Phase 2: 获取完整数据
    debate_packets = []
    for stock in candidates:
        packet = build_debate_packet(stock)  # xqshare K线+财务
        debate_packets.append(packet)

    # [新增] Phase 3: 完全版辩论
    debate = StockSelectionDebate(model=model)
    result = debate.run(debate_packets)

    # Phase 4: 回测 + 推送（不变）
    final_picks = result["ranked_candidates"][:5]
    backtest_results = run_backtest(final_picks)
    push_feishu(result, backtest_results)
```

---

## 十、优先级

| 阶段 | 内容 | 复杂度 |
|------|------|--------|
| **Phase 1** | 数据获取（xqshare）+ 知识库加载 | 中 |
| **Phase 2** | 四角色独立评估（4次LLM并行） | 低 |
| **Phase 3** | 矛盾识别 + 交叉辩论 | 高 |
| **Phase 4** | 裁判判决 + 飞书卡片 | 低 |

建议先实现 Phase 1+2（MVP），再逐步加入 Phase 3 的交叉辩论。
