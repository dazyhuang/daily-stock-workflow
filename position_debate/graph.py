"""
持仓辩论 LangGraph 状态机
=========================
对齐 TradingAgents graph/setup.py 架构：

完整流程：
START → Bull Researcher
         ↘
          → (条件边: 轮次 < 2*max_debate_rounds → 继续Bull/Bear轮换)
         ↗                                 ↓
      Bear Researcher → (条件边: 轮次 >= 2*max_debate_rounds → Research Manager)
                                                        ↓
                                                  Research Manager
                                                        ↓
                                                        Trader
                                                        ↓
                                        ↗           ↗           ↗
                                        ↓           ↓           ↓
                                   Aggressive  Conservative  Neutral
                                        ↘
                                         → (条件边: 轮次 < 3*max_risk_rounds → 继续轮换)
                                        ↗                                    ↓
                                        ...                                   ↓
                                                       (条件边: 轮次 >= 3*max_risk_rounds → Portfolio Manager)
                                                                                      ↓
                                                                            Portfolio Manager → END
"""

from langgraph.graph import END, START, StateGraph

from .state import StockDebateState, InvestDebateState, RiskDebateState
from .nodes import (
    create_bull_researcher_node,
    create_bear_researcher_node,
    create_research_manager_node,
    create_trader_node,
    create_aggressive_risk_node,
    create_conservative_risk_node,
    create_neutral_risk_node,
    create_portfolio_manager_stock_node,
)


def build_stock_debate_graph(max_debate_rounds: int = 1, max_risk_rounds: int = 1):
    """
    持仓辩论图：对齐 TradingAgents 单股票完整流程
    
    流程：
    Bull/Bear 多轮辩论 → Research Manager 裁决
    → Trader 提案
    → 三风控多轮辩论 → Portfolio Manager 最终决策
    """
    workflow = StateGraph(StockDebateState)

    # ── 节点 ─────────────────────────────────────────────
    workflow.add_node("Bull Researcher", create_bull_researcher_node())
    workflow.add_node("Bear Researcher", create_bear_researcher_node())
    workflow.add_node("Research Manager", create_research_manager_node())
    workflow.add_node("Trader", create_trader_node())
    workflow.add_node("Aggressive Analyst", create_aggressive_risk_node())
    workflow.add_node("Conservative Analyst", create_conservative_risk_node())
    workflow.add_node("Neutral Analyst", create_neutral_risk_node())
    workflow.add_node("Portfolio Manager", create_portfolio_manager_stock_node())

    # ── 条件路由函数 ─────────────────────────────────────

    def should_continue_debate(state: StockDebateState) -> str:
        """投资辩论：Bull/Bear 轮转"""
        inv_state = state.get("investment_debate_state", {})
        count = inv_state.get("count", 0)
        current_response = inv_state.get("current_response", "")

        if count >= 2 * max_debate_rounds:
            return "Research Manager"
        if current_response.startswith("Bull"):
            return "Bear Researcher"
        return "Bull Researcher"

    def should_continue_risk(state: StockDebateState) -> str:
        """风险辩论：三风控分析师轮转"""
        risk_state = state.get("risk_debate_state", {})
        count = risk_state.get("count", 0)
        latest = risk_state.get("latest_speaker", "")

        if count >= 3 * max_risk_rounds:
            return "Portfolio Manager"
        if latest == "Aggressive Analyst":
            return "Conservative Analyst"
        if latest == "Conservative Analyst":
            return "Neutral Analyst"
        return "Aggressive Analyst"

    # ── 边定义 ─────────────────────────────────────────────

    # 入口点
    workflow.add_edge(START, "Bull Researcher")

    # 投资辩论条件边（Bull ↔ Bear 轮换）
    workflow.add_conditional_edges(
        "Bull Researcher",
        should_continue_debate,
        {
            "Bear Researcher": "Bear Researcher",
            "Research Manager": "Research Manager",
        },
    )
    workflow.add_conditional_edges(
        "Bear Researcher",
        should_continue_debate,
        {
            "Bull Researcher": "Bull Researcher",
            "Research Manager": "Research Manager",
        },
    )

    # Research Manager → Trader（固定边）
    workflow.add_edge("Research Manager", "Trader")

    # 风险辩论条件边（三风控轮换）
    workflow.add_conditional_edges(
        "Aggressive Analyst",
        should_continue_risk,
        {
            "Conservative Analyst": "Conservative Analyst",
            "Portfolio Manager": "Portfolio Manager",
        },
    )
    workflow.add_conditional_edges(
        "Conservative Analyst",
        should_continue_risk,
        {
            "Neutral Analyst": "Neutral Analyst",
            "Portfolio Manager": "Portfolio Manager",
        },
    )
    workflow.add_conditional_edges(
        "Neutral Analyst",
        should_continue_risk,
        {
            "Aggressive Analyst": "Aggressive Analyst",
            "Portfolio Manager": "Portfolio Manager",
        },
    )

    # 最终边
    workflow.add_edge("Portfolio Manager", END)

    return workflow.compile()


# ── 预编译图实例 ─────────────────────────────────────

_stock_debate_graph = None


def get_stock_debate_graph(max_debate_rounds: int = 1, max_risk_rounds: int = 1):
    """获取预编译的持仓辩论图"""
    global _stock_debate_graph
    if _stock_debate_graph is None:
        _stock_debate_graph = build_stock_debate_graph(max_debate_rounds, max_risk_rounds)
    return _stock_debate_graph