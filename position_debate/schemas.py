"""
持仓辩论 Pydantic 模型
对齐 TradingAgents schemas 结构：
- StockDecision — 最终交易决策
- InvestmentPlan — Research Manager 裁决结果
- TraderProposal — 交易员提案
"""

from pydantic import BaseModel, Field
from typing import Optional


class StockDecision(BaseModel):
    """最终交易决策（Portfolio Manager 输出）"""
    rating: str = Field(description="Buy(加仓)/Hold(维持)/Sell(清仓)")
    executive_summary: str = Field(description="执行摘要（2-3句）")
    investment_thesis: str = Field(description="投资论点（3-5句）")
    confidence: str = Field(description="置信度：高/中/低")
    
    # 额外字段（兼容旧格式）
    reasoning: Optional[str] = Field(default=None, description="决策理由")
    action: Optional[str] = Field(default=None, description="操作类型 BUY/HOLD/SELL")
    stop_loss: Optional[float] = Field(default=None, description="止损价")
    position_size: Optional[str] = Field(default=None, description="建议仓位")


class InvestmentPlan(BaseModel):
    """Research Manager 裁决结果"""
    rating: str = Field(description="Buy/Overweight/Hold/Underweight/Sell")
    rationale: str = Field(description="裁决理由（2-3句）")
    strategic_actions: str = Field(description="战略操作建议")


class TraderProposal(BaseModel):
    """交易员提案"""
    action: str = Field(description="BUY/HOLD/SELL")
    reasoning: str = Field(description="理由（2-4句）")
    stop_loss: Optional[float] = Field(default=None, description="止损价")
    position_sizing: Optional[str] = Field(default=None, description="仓位建议")


# ── 渲染函数（将 Pydantic 对象渲染为可读文本）────────────────

def render_stock_decision(obj: StockDecision) -> str:
    """渲染最终决策为 markdown 文本"""
    lines = [
        f"**Rating**: {obj.rating}",
        "",
        f"**Executive Summary**: {obj.executive_summary}",
        "",
        f"**Investment Thesis**: {obj.investment_thesis}",
        "",
        f"**Confidence**: {obj.confidence}",
    ]
    if obj.reasoning:
        lines.append("")
        lines.append(f"**Reasoning**: {obj.reasoning}")
    if obj.stop_loss:
        lines.append("")
        lines.append(f"**Stop Loss**: {obj.stop_loss}")
    return "\n".join(lines)


def render_investment_plan(obj: InvestmentPlan) -> str:
    """渲染投资计划为 markdown 文本"""
    return (
        f"**Rating**: {obj.rating}\n\n"
        f"**Rationale**: {obj.rationale}\n\n"
        f"**Strategic Actions**: {obj.strategic_actions}"
    )


def render_trader_proposal(obj: TraderProposal) -> str:
    """渲染交易员提案为 markdown 文本"""
    lines = [
        f"**Action**: {obj.action}",
        "",
        f"**Reasoning**: {obj.reasoning}",
    ]
    if obj.stop_loss:
        lines.append("")
        lines.append(f"**Stop Loss**: {obj.stop_loss}")
    if obj.position_sizing:
        lines.append("")
        lines.append(f"**Position Sizing**: {obj.position_sizing}")
    return "\n".join(lines)