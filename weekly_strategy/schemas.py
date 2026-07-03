"""
周复盘辩论 Pydantic Schemas
===========================
来自 TradingAgents v0.2.5 架构设计：
- 使用 structured output（MiniMax native + fallback to free-text）
- 5个角色对应 5 个 schema
- Portfolio Manager 输出最终决策

新增 WeeklyReviewDecision 对齐周复盘场景：
- position_size_pct（仓位百分比）
- scoring_threshold（选股阈值）
- stop_loss_pct（止损）
- take_profit_1（止盈1）
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ── 共享枚举 ──────────────────────────────────────────────

class PortfolioRating(str, Enum):
    """评级：维持/加仓/降仓/清仓"""
    BUY = "Buy"          # 加仓
    OVERWEIGHT = "Overweight"   # 维持（偏多）
    HOLD = "Hold"       # 维持
    UNDERWEIGHT = "Underweight" # 降仓
    SELL = "Sell"        # 清仓


class Action(str, Enum):
    """操作方向：Buy(买)/Hold(持有)/Sell(卖)"""
    BUY = "Buy"
    HOLD = "Hold"
    SELL = "Sell"


# ── 分析师节点（Analyst）──────────────

class AnalystOutput(BaseModel):
    """数据分析师输出：本周数据摘要 + 参数调整建议"""
    data_summary: str = Field(
        description="本周数据摘要，3句内",
    )
    suggested_position_change: int = Field(
        description="建议仓位调整幅度（%），正=加仓，负=降仓",
    )
    suggested_threshold_change: int = Field(
        description="建议选股阈值调整，正=提高阈值（更严格），负=降低阈值（更宽松）",
    )
    adjustment_reason: str = Field(
        description="调整理由，1-2句",
    )
    win_rate: float = Field(description="本周胜率（0-100）")
    avg_return: float = Field(description="本周平均收益率（%）")
    max_drawdown: float = Field(description="最大回撤（%）")
    bayesian_hit_rate: float = Field(description="贝叶斯命中率（%）")


class TraderProposal(BaseModel):
    """交易员提案：对齐 TradingAgents TraderProposal"""
    action: Action = Field(description="操作方向：Buy/Hold/Sell")
    reasoning: str = Field(description="操作理由，2-4句")
    entry_price: Optional[float] = Field(default=None, description="可选入场价")
    stop_loss: Optional[float] = Field(default=None, description="可选止损价")
    position_sizing: Optional[str] = Field(default=None, description="可选仓位指导")


def render_trader_proposal(o: TraderProposal) -> str:
    parts = [
        f"**Action**: {o.action.value}",
        f"**Reasoning**: {o.reasoning}",
    ]
    if o.entry_price is not None:
        parts.append(f"**Entry Price**: {o.entry_price}")
    if o.stop_loss is not None:
        parts.append(f"**Stop Loss**: {o.stop_loss}")
    if o.position_sizing:
        parts.append(f"**Position Sizing**: {o.position_sizing}")
    parts.append(f"\nFINAL TRANSACTION PROPOSAL: **{o.action.value.upper()}**")
    return "\n".join(parts)


def render_analyst_output(o: AnalystOutput) -> str:
    return "\n".join([
        f"**数据摘要**: {o.data_summary}",
        f"**建议仓位调整**: {'+' if o.suggested_position_change >= 0 else ''}{o.suggested_position_change}%",
        f"**建议阈值调整**: {'+' if o.suggested_threshold_change >= 0 else ''}{o.suggested_threshold_change}",
        f"**调整理由**: {o.adjustment_reason}",
        f"**本周胜率**: {o.win_rate:.0f}%",
        f"**平均收益率**: {o.avg_return:+.2f}%",
        f"**最大回撤**: {o.max_drawdown:+.2f}%",
        f"**贝叶斯命中率**: {o.bayesian_hit_rate:.1f}%",
    ])


# ── 策略师节点（Strategist）────────────

class StrategistOutput(BaseModel):
    """策略师输出：市场环境评估 + 支持/质疑分析师"""
    market_assessment: str = Field(
        description="市场环境评估，2-3句",
    )
    hit_rate_analysis: str = Field(
        description="命中率低是系统问题还是市场整体弱势，2-3句",
    )
    position_adjustment_appropriate: str = Field(
        description="当前市场环境下调整仓位是否合适，2-3句",
    )
    recommendation: str = Field(
        description="建议：支持/降级/否决分析师方案",
    )
    recommendation_reason: str = Field(
        description="理由，2-3句",
    )


def render_strategist_output(o: StrategistOutput) -> str:
    return "\n".join([
        f"**市场评估**: {o.market_assessment}",
        f"**命中率分析**: {o.hit_rate_analysis}",
        f"**仓位调整建议**: {o.position_adjustment_appropriate}",
        f"**策略师结论**: {o.recommendation}",
        f"**理由**: {o.recommendation_reason}",
    ])


# ── 风控官节点（Risk Officer）────────────

class RiskOutput(BaseModel):
    """风控官输出：风险视角评估"""
    risk_exposure_assessment: str = Field(
        description="当前风险暴露是否过高，2-3句",
    )
    strategy_vs_parameters: str = Field(
        description="连续亏损是否意味着策略需要系统性修正，2-3句",
    )
    risk_recommendation: str = Field(
        description="是否需要更保守的调整，2-3句",
    )
    final_risk_stance: str = Field(
        description="最终风险立场：激进/中性/保守",
    )


def render_risk_output(o: RiskOutput) -> str:
    return "\n".join([
        f"**风险暴露评估**: {o.risk_exposure_assessment}",
        f"**策略 vs 参数**: {o.strategy_vs_parameters}",
        f"**风控建议**: {o.risk_recommendation}",
        f"**风险立场**: {o.final_risk_stance}",
    ])


# ── 基金经理节点（Portfolio Manager）────────────

class WeeklyReviewDecision(BaseModel):
    """
    基金经理最终决策（Pydantic 模型，structured output）
    
    对齐 TradingAgents PortfolioDecision schema 设计模式：
    - rating: 最终评级（Buy/Overweight/Hold/Underweight/Sell）
    - executive_summary: 执行摘要
    - investment_thesis: 投资论点
    - price_target: 目标价格（可选）
    - time_horizon: 持仓周期（可选）
    
    扩展字段（周复盘专用）：
    - position_size_pct: 目标仓位（20 = 20%）
    - scoring_threshold: 选股阈值
    - stop_loss_pct: 止损设置
    - take_profit_1: 止盈1设置
    """
    rating: PortfolioRating = Field(
        description="最终评级：Buy(加仓)/Overweight(维持偏多)/Hold(维持)/Underweight(降仓)/Sell(清仓)",
    )
    executive_summary: str = Field(
        description="执行摘要：2-3句覆盖仓位调整、风险水平、操作建议",
    )
    investment_thesis: str = Field(
        description="投资论点：详细理由，引用分析师/策略师/风控官的具体数据",
    )
    position_size_pct: int = Field(
        description="调整后目标仓位（%）",
    )
    scoring_threshold: int = Field(
        description="调整后选股阈值",
    )
    stop_loss_pct: float = Field(
        description="止损设置（如 -3.0）",
    )
    take_profit_1: float = Field(
        description="止盈1设置（如 5.0）",
    )
    take_profit_2: float = Field(
        description="止盈2设置（如 10.0）",
    )
    take_profit_3: float = Field(
        description="止盈3设置（如 30.0）",
    )
    confidence: str = Field(
        description="决策置信度：高/中/低",
    )
    analyst_view: str = Field(
        description="分析师视角摘要",
    )
    strategist_view: str = Field(
        description="策略师视角摘要",
    )
    risk_view: str = Field(
        description="风控视角摘要",
    )
    disagreements: str = Field(
        description="三方分歧点，无分歧填'无'",
    )
    price_target: Optional[float] = Field(
        default=None,
        description="可选目标价格",
    )
    time_horizon: Optional[str] = Field(
        default=None,
        description="可选持仓周期，如 '1-2周' / '1个月'",
    )


def render_weekly_decision(d: WeeklyReviewDecision) -> str:
    """渲染 WeeklyReviewDecision 为 markdown（用于存储和下游消费）"""
    parts = [
        f"**Rating**: {d.rating.value}",
        "",
        f"**Executive Summary**: {d.executive_summary}",
        "",
        f"**Investment Thesis**: {d.investment_thesis}",
        "",
        f"**Position Size**: {d.position_size_pct}%",
        f"**Scoring Threshold**: {d.scoring_threshold}",
        f"**Stop Loss**: {d.stop_loss_pct}%",
        f"**Take Profit 1/2/3**: {d.take_profit_1}%/{d.take_profit_2}%/{d.take_profit_3}%",
        f"**Confidence**: {d.confidence}",
        "",
        f"**Analyst View**: {d.analyst_view}",
        f"**Strategist View**: {d.strategist_view}",
        f"**Risk View**: {d.risk_view}",
        f"**Disagreements**: {d.disagreements}",
    ]
    if d.price_target is not None:
        parts.extend(["", f"**Price Target**: {d.price_target}"])
    if d.time_horizon:
        parts.extend(["", f"**Time Horizon**: {d.time_horizon}"])
    return "\n".join(parts)