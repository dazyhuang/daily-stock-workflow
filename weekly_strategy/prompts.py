"""
周复盘辩论角色 Prompts
======================
对齐 TradingAgents 结构化 Prompt 设计模式:
- 每个角色有明确的 Rating Scale 说明
- 输出格式由 Pydantic schema 描述(field descriptions 作为 instructions)
- 最后调用 get_language_instruction() 统一语言处理
"""

from typing import Dict, Any


# ── 统一工具 ──────────────────────────────────────────────

def get_language_instruction() -> str:
    """返回语言指令(中文)"""
    return "\n\n**输出语言**:始终使用中文。"


def build_analyst_prompt(
    week_data: Dict[str, Any],
    analyst_output: str = "",
) -> str:
    """
    数据分析师(Analyst)

    角色:对齐 TradingAgents Bull Researcher 的数据驱动风格
    输入:本周量化数据 + 参数回测对比
    输出:参数调整建议(structured → AnalystOutput)
    """
    trades = week_data.get("trades", [])
    stats = week_data.get("stats", {})
    benchmark = week_data.get("benchmark", {})
    market = week_data.get("market_regime", "震荡")
    params = week_data.get("current_params", {})
    backtest = week_data.get("backtest", {})

    win_rate = stats.get("win_rate", 0) * 100
    avg_return = stats.get("avg_return", 0) * 100
    hit_rate = stats.get("hit_rate", 0) * 100
    max_drawdown = stats.get("max_drawdown", 0) * 100
    total_pnl = stats.get("total_pnl_pct", 0) * 100
    vs_bench = benchmark.get("vs_benchmark", "?")
    current_pos = params.get("position_size_pct", 0.2) * 100
    score_thresh = params.get("scoring_threshold", 50)
    stop_loss = params.get("stop_loss_pct", -0.03) * 100
    tp1 = params.get("take_profit_1", 0.05) * 100

    # ── 自适应指标(Analyst 参考用)──────────────────────────
    raw_hit = stats.get("raw_hit_rate", 0) * 100
    bayes_hit = stats.get("bayesian_hit_rate", 0)
    momentum = stats.get("momentum_weeks", 0)
    recent_dirs = stats.get("recent_directions", [])
    pool_bayes = stats.get("pool_bayesian", {})
    pool_lines = "\n".join([f"  {k}: {v.get('bayes', v):.1f}%" if isinstance(v, dict) else f"  {k}: {v}" for k, v in pool_bayes.items()]) or "  无"
    dirs_str = "→".join([str(d) for d in recent_dirs[-6:]]) or "无"

    # ── 板块强弱(Strategist 参考用)───────────────────────────
    sector_rot = week_data.get("sector_rotation", {})
    hot_s = sector_rot.get("hot_sectors", []) if isinstance(sector_rot, dict) else []
    cold_s = sector_rot.get("cold_sectors", []) if isinstance(sector_rot, dict) else []
    hot_str = ", ".join(hot_s[:5]) if hot_s else "无"
    cold_str = ", ".join(cold_s[:5]) if cold_s else "无"

    # ── 交易列表────────────────────────────────────────────
    trade_list = "\n".join([
        f"  - {t['code']} {t['name']} [{t.get('signal_quality','')}] "
        f"买:{t['buy_price']} → 现:{t.get('current_price','?')} "
        f"({t['pnl_pct']*100:+.1f}%)"
        for t in trades[:10]
    ]) or "  无交易记录"

    # ── 回测 section(不变)─────────────────────────────────────
    backtest_section = ""
    if backtest and not backtest.get("error"):
        bt = backtest.get("comparison_table", "")
        best = backtest.get("best", {})
        curr_res = backtest.get("summary", {}).get("current_params_result", {})
        backtest_section = f"""

【参数回测结果(近4周)】
对比表:
{bt}

当前参数总收益:{curr_res.get('total', '?')}%
最优参数:{best.get('label', '?')}(总收益 {best.get('total', '?')}%,夏普 {best.get('sharpe', '?')},最大回撤 {best.get('max_dd', '?')}%)
"""

    return f"""你是A股量化团队的数据分析师,负责客观汇报本周实战数据。

【本周数据 ({week_data.get("week", "?")})】
大盘环境:{market}

交易记录:
{trade_list}

统计摘要:
  交易次数:{len(trades)}
  胜率:{win_rate:.0f}%
  平均收益率:{avg_return:+.2f}%
  原始命中率:{raw_hit:.1f}%
  贝叶斯命中率:{bayes_hit:.1f}%
  最大回撤:{max_drawdown:+.2f}%
  累计盈亏:{total_pnl:+.2f}%
  跑赢基准:{vs_bench}
  动量周数:{momentum}周
  近期方向序列:{dirs_str}
  各池子贝叶斯命中率:
{pool_lines}

当前策略参数:
  仓位:{current_pos:.0f}%
  选股打分阈值:{score_thresh}
  止损:{stop_loss:.0f}%
  止盈1:{tp1:.0f}%

自适应规则:
  命中率 > 70% → 仓位+10%,阈值-3
  命中率 < 40% → 仓位-20%,阈值+5
  连续2周亏损 → 仓位再降50%

{backtest_section}

【输出格式】
data_summary: 本周数据摘要(3句内)
suggested_position_change: 建议仓位调整幅度(%),正=加仓,负=降仓
suggested_threshold_change: 建议选股阈值调整,正=提高阈值(更严格),负=降低阈值(更宽松)
adjustment_reason: 调整理由(1-2句)
win_rate: 本周胜率(0-100)
avg_return: 本周平均收益率(%)
max_drawdown: 最大回撤(%)
bayesian_hit_rate: 贝叶斯命中率(%)

请输出JSON格式,包含上述所有字段,字段名必须与输出格式要求完全一致,不要新增字段。{get_language_instruction()}"""


def build_strategist_prompt(
    week_data: Dict[str, Any],
    analyst_output: str,
) -> str:
    """
    策略师(Strategist)

    对齐 TradingAgents Bear Researcher 的挑战风格
    角色:质疑分析师的市场环境判断
    输入:分析师输出 + 大盘背景
    输出:市场评估 + 支持/质疑(structured → StrategistOutput)
    """
    market = week_data.get("market_regime", "震荡")
    market_detail = week_data.get("market_detail", "")
    benchmark = week_data.get("benchmark", {})
    params = week_data.get("current_params", {})
    current_pos = params.get("position_size_pct", 0.2) * 100
    hit_rate = week_data.get("stats", {}).get("hit_rate", 0) * 100

    # ── 板块强弱（Strategist 核心参考）────────────────────────
    sector_rot = week_data.get("sector_rotation", {})
    hot_s = sector_rot.get("hot_sectors", []) if isinstance(sector_rot, dict) else []
    cold_s = sector_rot.get("cold_sectors", []) if isinstance(sector_rot, dict) else []
    hot_str = ", ".join(hot_s[:5]) if hot_s else "无"
    cold_str = ", ".join(cold_s[:5]) if cold_s else "无"
    momentum = week_data.get("stats", {}).get("momentum_weeks", 0)
    recent_dirs = week_data.get("stats", {}).get("recent_directions", [])
    dirs_str = "→".join([str(d) for d in recent_dirs[-6:]]) or "无"
    pool_bayes = week_data.get("stats", {}).get("pool_bayesian", {})
    pool_lines = "\n".join([f"  {k}: {v.get('bayes', v):.1f}%" if isinstance(v, dict) else f"  {k}: {v}" for k, v in pool_bayes.items()]) or "  无"

    return f"""你是A股量化团队的策略师，负责从市场环境角度评估参数调整建议。

【分析师结论】
{analyst_output}

【大盘背景 ({week_data.get("week", "?")})】
市场环境判断：{market}
市场详情：{market_detail}

【大盘表现】
沪深300：{benchmark.get('hs300_change', 0)*100:+.2f}%
上证：{benchmark.get('sh_change', 0)*100:+.2f}%
深证：{benchmark.get('sz_change', 0)*100:+.2f}%

【板块强弱】
强势板块：{hot_str}
弱势板块：{cold_str}

【动量与方向】
近期方向序列：{dirs_str}（动量{momentum}周）
各池子命中率：
{pool_lines}

【当前参数】
仓位：{current_pos:.0f}%
命中率：{hit_rate:.0f}%

【输出格式】
market_assessment: 市场环境评估（2-3句）
hit_rate_analysis: 命中率低是系统问题还是市场整体弱势（2-3句）
position_adjustment_appropriate: 当前市场环境下调整仓位是否合适（2-3句）
recommendation: 建议：支持/降级/否决分析师方案
recommendation_reason: 理由（2-3句）

请输出JSON格式，包含上述所有字段，字段名必须与输出格式要求完全一致，不要新增字段。{get_language_instruction()}"""


def build_risk_prompt(
    week_data: Dict[str, Any],
    analyst_output: str,
    strategist_output: str,
) -> str:
    """
    风控官(Risk Officer)

    对齐 TradingAgents 三风控分析师(Aggressive/Neutral/Conservative)的风险评估风格
    角色:风险视角质疑
    输入:分析师 + 策略师输出 + 风险数据 + 参数回测
    输出:风险评估(structured → RiskOutput)
    """
    stats = week_data.get("stats", {})
    max_drawdown = stats.get("max_drawdown", 0) * 100
    total_pnl = stats.get("total_pnl_pct", 0) * 100
    params = week_data.get("current_params", {})
    current_pos = params.get("position_size_pct", 0.2) * 100
    consecutive_loss = stats.get("consecutive_loss_weeks", 0)
    backtest = week_data.get("backtest", {})
    stop_loss = params.get("stop_loss_pct", -0.03) * 100
    tp1 = params.get("take_profit_1", 0.05) * 100

    backtest_section = ""
    if backtest and not backtest.get("error"):
        bt = backtest.get("comparison_table", "")
        curr_res = backtest.get("summary", {}).get("current_params_result", {})
        best = backtest.get("best", {})
        backtest_section = f"""

【参数回测(近4周)】
对比表:
{bt}

当前参数总收益:{curr_res.get('total', '?')}%  夏普:{curr_res.get('sharpe', '?')}  最大回撤:{curr_res.get('max_dd', '?')}%
最优参数:{best.get('label', '?')}(总收益 {best.get('total', 0):.1f}%)
"""

    return f"""你是A股量化团队的风控官,负责从风险角度评估决策。

【分析师结论】
{analyst_output}

【策略师结论】
{strategist_output}

【风险数据 ({week_data.get("week", "?")})】
最大回撤:{max_drawdown:+.2f}%
累计盈亏:{total_pnl:+.2f}%
当前仓位:{current_pos:.0f}%
止损:{stop_loss:.0f}%
止盈1:{tp1:.0f}%
连续亏损周数:{consecutive_loss}周

{backtest_section}

【输出格式】
risk_exposure_assessment: 当前风险暴露是否过高(当前仓位{current_pos:.0f}%下的最大亏损可能)(2-3句)
strategy_vs_parameters: 连续亏损是否意味着策略需要系统性修正,而非仅调整参数(2-3句)
risk_recommendation: 是否需要更保守的调整(2-3句)
final_risk_stance: 最终风险立场:激进/中性/保守

请输出JSON格式,包含上述所有字段,字段名必须与输出格式要求完全一致,不要新增字段。{get_language_instruction()}"""


def build_fund_manager_prompt(
    week_data: Dict[str, Any],
    analyst_output: str,
    strategist_output: str,
    risk_output: str,
) -> str:
    """
    基金经理(Portfolio Manager)

    对齐 TradingAgents Portfolio Manager 的最终决策模式
    角色:综合三方意见,给出最终参数决策
    输入:分析师 + 策略师 + 风控输出 + 当前参数
    输出:最终决策(structured → WeeklyReviewDecision)
    """
    params = week_data.get("current_params", {})
    current_pos = params.get("position_size_pct", 0.2) * 100
    score_thresh = params.get("scoring_threshold", 50)
    stop_loss = params.get("stop_loss_pct", -0.03) * 100
    tp1 = params.get("take_profit_1", 0.05) * 100
    tp2 = params.get("take_profit_2", 0.10) * 100
    tp3 = params.get("take_profit_3", 0.30) * 100

    return f"""你是基金经理,综合分析师、策略师、风控官三方意见后,做出最终参数决策。

【分析师】
{analyst_output}

【策略师】
{strategist_output}

【风控官】
{risk_output}

【当前参数】
仓位:{current_pos:.0f}% | 选股阈值:{score_thresh}
止损:{stop_loss:.0f}% | 止盈1/2/3:{tp1:.0f}%/{tp2:.0f}%/{tp3:.0f}%

【Rating Scale】(必须选一个):
- Buy(加仓): 强烈看多,建议增加仓位
- Overweight(维持偏多): 看好,建议维持或小幅增加
- Hold(维持): 中性,建议维持当前仓位
- Underweight(降仓): 谨慎,建议降低仓位
- Sell(清仓): 强烈看空,建议清仓

【决策约束】
- 仓位调整幅度:±5%~±20%(不要一次性拉满)
- 阈值调整幅度:±3~±5
- 止损调整范围:-2%~-5%(更负=更宽松)
- 止盈1/2/3:均可单独调整,三档协同
- 连续2周亏损 → 仓位额外降50%

【输出格式】
rating: 最终评级(Buy/Overweight/Hold/Underweight/Sell)
executive_summary: 执行摘要(2-3句覆盖仓位调整、风险水平、操作建议)
investment_thesis: 投资论点(引用分析师/策略师/风控官的具体数据)
position_size_pct: 调整后目标仓位(%)
scoring_threshold: 调整后选股阈值
stop_loss_pct: 止损设置(如 -3.0)
take_profit_1: 止盈1设置(如 5.0)
take_profit_2: 止盈2设置(如 10.0)
take_profit_3: 止盈3设置(如 30.0)
confidence: 决策置信度:高/中/低
analyst_view: 分析师视角摘要(1-2句)
strategist_view: 策略师视角摘要(1-2句)
risk_view: 风控视角摘要(1-2句)
disagreements: 三方分歧点,无分歧填'无'

请输出JSON格式,包含上述所有字段,字段名必须与输出格式要求完全一致,不要新增字段。{get_language_instruction()}"""


# ── 持仓辩论 Prompts(每持仓股票单独决策)────────────

def build_stock_trader_prompt(
    stock_data: Dict[str, Any],
    research_plan: str,
    market_report: str = "",
    sentiment_report: str = "",
    news_report: str = "",
    fundamentals_report: str = "",
) -> str:
    """
    持仓交易决策(Trader 节点)

    对齐 TradingAgents Trader agent
    角色:把研究计划翻译成具体交易操作
    输入:持仓数据 + 研究计划
    输出:BUY/HOLD/SELL 操作建议
    """
    code = stock_data.get("code", "")
    name = stock_data.get("name", "")
    buy_price = stock_data.get("buy_price", 0)
    current_price = stock_data.get("current_price", 0)
    pnl_pct = stock_data.get("pnl_pct", 0) * 100
    action = stock_data.get("action", "HOLD")

    return f"""你是交易员,基于研究计划对持仓股票做出具体操作决策。

【持仓信息】
股票代码:{code}
股票名称:{name}
买入价:{buy_price}
当前价:{current_price if current_price else '?'}
浮盈浮亏:{pnl_pct:+.1f}%
当前操作建议:{action}

【研究报告】
{research_plan}

【市场报告】
{market_report or '无'}

【舆情报告】
{sentiment_report or '无'}

【新闻报告】
{news_report or '无'}

【基本面报告】
{fundamentals_report or '无'}

【输出格式】
action: BUY/HOLD/SELL(必须选一个)
reasoning: 操作理由(2-4句,引用数据和报告)
entry_price: 可选入场价目标(如需要加仓)
stop_loss: 可选止损价(如需要减仓)
position_sizing: 可选仓位指导,如 '5% of portfolio'

请输出JSON格式,不要有其他文字。{get_language_instruction()}"""


# ── 风险辩论 Prompts(持仓风险评估)────────────

def build_stock_risk_prompt(
    stock_data: Dict[str, Any],
    trader_proposal: str,
    risk_data: Dict[str, Any],
) -> str:
    """
    持仓风险评估(Risk 节点)

    对齐 TradingAgents 三风控分析师辩论
    角色:激进/中性/保守三角度评估
    输入:持仓数据 + 交易员建议 + 风险数据
    输出:风险评估结论
    """
    code = stock_data.get("code", "")
    name = stock_data.get("name", "")
    pnl_pct = stock_data.get("pnl_pct", 0) * 100
    stop_loss_pct = risk_data.get("stop_loss_pct", -0.03) * 100
    max_loss_if_wrong = risk_data.get("max_loss_if_wrong", 0) * 100

    return f"""你是风险分析师,从风险角度评估持仓操作建议。

【持仓信息】
{code} {name},浮盈浮亏:{pnl_pct:+.1f}%

【交易员建议】
{trader_proposal}

【风险数据】
止损设置:{stop_loss_pct:.0f}%
方向错误最大亏损:{max_loss_if_wrong:.0f}%

【输出格式】
risk_assessment: 风险评估(2-3句)
should_adjust: 是否需要调整止损/仓位:是/否
adjusted_stop_loss: 调整后止损(如需要)
adjusted_position: 调整后仓位(如需要)
risk_stance: 风险立场:激进/中性/保守

请输出JSON格式,不要有其他文字。{get_language_instruction()}"""