"""
持仓辩论节点定义
对齐 TradingAgents agents/ 目录的节点工厂模式：

8个节点：
- Bull Researcher — 多头研究员
- Bear Researcher — 空头研究员
- Research Manager — 研究总监（裁决）
- Trader — 交易员
- Aggressive Analyst — 激进风控
- Conservative Analyst — 保守风控
- Neutral Analyst — 中性风控
- Portfolio Manager — 组合经理（最终决策）
"""

from __future__ import annotations

import functools
import logging
from typing import Any, Dict, Optional, Type, TypeVar

from .schemas import (
    StockDecision,
    InvestmentPlan,
    TraderProposal,
    render_stock_decision,
    render_investment_plan,
    render_trader_proposal,
)

logger = logging.getLogger("stock_debate.nodes")

T = TypeVar("T", bound=object)

DEBATE_THINKING_BUDGET = 10000
JSON_THINKING_BUDGET = 0

# ── 统一 LLM 调用 ─────────────────────────────────────────

def _get_llm_clients():
    """延迟加载 LLM 客户端（复用 stock_selection_debate providers）"""
    import sys
    from pathlib import Path

    workflowProviders = Path(__file__).parent.parent / "stock_selection_debate" / "providers.py"
    if str(workflowProviders) not in sys.path:
        sys.path.insert(0, str(Path(__file__).parent.parent))

    from stock_selection_debate.providers import call_llm, call_llm_structured
    return call_llm, call_llm_structured


def _structured_output(
    prompt: str,
    schema: Type[T],
    agent_name: str,
    model: str = "volcengine-plan/ark-code-latest",
    thinking_budget: int = JSON_THINKING_BUDGET,
) -> Optional[T]:
    """调用结构化输出，失败时 fallback"""
    call_llm, _ = _get_llm_clients()

    try:
        result = call_llm(
            prompt=prompt,
            model=model,
            system="你是一个严谨的金融分析助手，输出格式必须是合法的JSON。",
            temperature=0,
            thinking_budget=thinking_budget,
            max_tokens=1500,
        )

        import json
        raw = result.strip()
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:] if len(lines) > 1 else lines)
            if raw.endswith("```"):
                raw = raw[:-3].strip()

        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > start:
            return schema(**json.loads(raw[start:end]))

        logger.warning(f"[{agent_name}] 无法从响应中提取 JSON")
        return None

    except Exception as e:
        logger.warning(f"[{agent_name}] 结构化输出失败: {e}，fallback 到自由文本")
        return None


def _call_with_fallback(
    prompt: str,
    agent_name: str,
    render_fn: callable,
    schema: Optional[Type[T]] = None,
    model: str = "volcengine-plan/ark-code-latest",
) -> str:
    """调用 LLM，优先结构化输出，失败时自由文本"""
    call_llm, _ = _get_llm_clients()

    if schema is not None:
        result = _structured_output(prompt, schema, agent_name, model=model)
        if result is not None:
            return render_fn(result)

    # Fallback：JSON 节点仍然使用低温、关 thinking，避免格式漂移。
    temp = 0 if schema is not None else 0.3
    budget = JSON_THINKING_BUDGET if schema is not None else DEBATE_THINKING_BUDGET
    max_tokens = 1500 if schema is not None else 12000
    response = call_llm(
        prompt=prompt,
        model=model,
        system="你是一个严谨的金融分析助手。请按要求输出JSON格式。",
        temperature=temp,
        thinking_budget=budget,
        max_tokens=max_tokens,
    )

    if schema is not None:
        try:
            import json
            raw = response.strip()
            if raw.startswith("```"):
                lines = raw.split("\n")
                raw = "\n".join(lines[1:] if len(lines) > 1 else lines)
                if raw.endswith("```"):
                    raw = raw[:-3].strip()

            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start >= 0 and end > start:
                return render_fn(schema(**json.loads(raw[start:end])))
        except Exception as e:
            logger.warning(f"[{agent_name}] 自由文本 JSON 解析失败: {e}")

    return response


# ── 节点工厂 ─────────────────────────────────────────────

def create_bull_researcher_node():
    """
    多头研究员（Bull Researcher）
    
    角色：建立强有力看多论点，引用数据和催化剂
    对齐 TradingAgents Bull Researcher
    """
    def bull_node(state: dict) -> dict:
        inv_state = state.get("investment_debate_state", {})
        history = inv_state.get("history", "")
        current_response = inv_state.get("current_response", "")

        stock_code = state.get("stock_code", "")
        stock_name = state.get("stock_name", "")
        market_report = state.get("market_report", "")
        sentiment_report = state.get("sentiment_report", "")
        news_report = state.get("news_report", "")
        fundamentals_report = state.get("fundamentals_report", "")

        prompt = f"""你是多头分析师，为持仓股票建立强有力的看多论点。

持仓信息：{stock_code} {stock_name}

可用研究资源：
市场报告：{market_report if market_report else '（暂无）'}
舆情报告：{sentiment_report if sentiment_report else '（暂无）'}
新闻报告：{news_report if news_report else '（暂无）'}
基本面报告：{fundamentals_report if fundamentals_report else '（暂无）'}
辩论历史：{history}
空方最新论点：{current_response if current_response else '（首轮发言）'}

重点：
- 增长催化剂：强调市场机会、收入预测、可扩展性
- 竞争优势：独特产品、强品牌、市场主导地位
- 积极指标：财务健康、行业趋势、最新正面新闻
- 反驳空头：用具体数据和合理推理

用中文输出你的多头论点（3-5句），不要有其他格式或标记。"""

        call_llm, _ = _get_llm_clients()
        response = call_llm(
            prompt=prompt,
            model="volcengine-plan/ark-code-latest",
            system="你是多头分析师，用中文输出。",
            temperature=0.3,
            thinking_budget=DEBATE_THINKING_BUDGET,
        )

        argument = f"Bull: {response}"

        new_inv_state = {
            "history": history + "\n" + argument,
            "bull_history": inv_state.get("bull_history", "") + "\n" + argument,
            "bear_history": inv_state.get("bear_history", ""),
            "current_response": argument,
            "count": inv_state.get("count", 0) + 1,
            "judge_decision": inv_state.get("judge_decision", ""),
        }

        logger.info(f"[{stock_code} Bull] 输出: {response[:100]}...")

        return {"investment_debate_state": new_inv_state}

    return bull_node


def create_bear_researcher_node():
    """
    空头研究员（Bear Researcher）
    
    角色：建立强有力看空论点，挑战多头假设
    对齐 TradingAgents Bear Researcher
    """
    def bear_node(state: dict) -> dict:
        inv_state = state.get("investment_debate_state", {})
        history = inv_state.get("history", "")
        current_response = inv_state.get("current_response", "")

        stock_code = state.get("stock_code", "")
        stock_name = state.get("stock_name", "")
        market_report = state.get("market_report", "")
        sentiment_report = state.get("sentiment_report", "")
        news_report = state.get("news_report", "")
        fundamentals_report = state.get("fundamentals_report", "")

        prompt = f"""你是空头分析师，为持仓股票建立强有力的看空论点。

持仓信息：{stock_code} {stock_name}

可用研究资源：
市场报告：{market_report if market_report else '（暂无）'}
舆情报告：{sentiment_report if sentiment_report else '（暂无）'}
新闻报告：{news_report if news_report else '（暂无）'}
基本面报告：{fundamentals_report if fundamentals_report else '（暂无）'}
辩论历史：{history}
多方最新论点：{current_response if current_response else '（首轮发言）'}

重点：
- 风险因素：市场逆风、竞争威胁、估值过高
- 负面指标：财务问题、行业下行、技术破位
- 反驳多头：用具体数据挑战多头的乐观预期

用中文输出你的空头论点（3-5句），不要有其他格式或标记。"""

        call_llm, _ = _get_llm_clients()
        response = call_llm(
            prompt=prompt,
            model="volcengine-plan/ark-code-latest",
            system="你是空头分析师，用中文输出。",
            temperature=0.3,
            thinking_budget=DEBATE_THINKING_BUDGET,
        )

        argument = f"Bear: {response}"

        new_inv_state = {
            "history": history + "\n" + argument,
            "bull_history": inv_state.get("bull_history", ""),
            "bear_history": inv_state.get("bear_history", "") + "\n" + argument,
            "current_response": argument,
            "count": inv_state.get("count", 0) + 1,
            "judge_decision": inv_state.get("judge_decision", ""),
        }

        logger.info(f"[{stock_code} Bear] 输出: {response[:100]}...")

        return {"investment_debate_state": new_inv_state}

    return bear_node


def create_research_manager_node():
    """
    研究总监（Research Manager）
    
    角色：评估多空辩论，给出裁决（structured output）
    对齐 TradingAgents Research Manager
    """
    def research_manager_node(state: dict) -> dict:
        inv_state = state.get("investment_debate_state", {})
        history = inv_state.get("history", "")

        stock_code = state.get("stock_code", "")
        stock_name = state.get("stock_name", "")
        buy_price = state.get("buy_price", 0)
        current_price = state.get("current_price", 0)
        pnl_pct = state.get("pnl_pct", 0) * 100

        prompt = f"""你是研究总监，评估多空辩论后给出投资建议。

持仓信息：{stock_code} {stock_name}
买入价：{buy_price}
当前价：{current_price if current_price else '?'}
浮盈亏：{pnl_pct:+.1f}%

多空辩论历史：
{history}

Rating Scale：
- Buy(加仓): 强烈看多，上涨空间 > 20%
- Overweight(维持偏多): 看好，上涨空间 10-20%
- Hold(维持): 中性，上下空间有限
- Underweight(降仓): 谨慎，可能下跌 10-20%
- Sell(清仓): 强烈看空，下跌空间 > 20%

输出JSON格式（必须包含所有字段）：
{{"rating": "Buy/Overweight/Hold/Underweight/Sell", "rationale": "理由2-3句", "strategic_actions": "具体操作建议"}}

用中文输出合法JSON，不要有任何其他文字。"""

        output = _call_with_fallback(
            prompt=prompt,
            agent_name="Research Manager",
            render_fn=render_investment_plan,
            schema=InvestmentPlan,
        )

        new_inv_state = {
            "history": history,
            "bull_history": inv_state.get("bull_history", ""),
            "bear_history": inv_state.get("bear_history", ""),
            "current_response": output,
            "judge_decision": output,
            "count": inv_state.get("count", 0),
        }

        logger.info(f"[{stock_code} Research Manager] 裁决: {output[:150]}...")

        return {
            "investment_debate_state": new_inv_state,
            "investment_plan": output,
        }

    return research_manager_node


def create_trader_node():
    """
    交易员（Trader）
    
    角色：基于研究计划提出具体交易提案（structured output）
    对齐 TradingAgents Trader
    """
    def trader_node(state: dict) -> dict:
        stock_code = state.get("stock_code", "")
        stock_name = state.get("stock_name", "")
        investment_plan = state.get("investment_plan", "")

        buy_price = state.get("buy_price", 0)
        current_price = state.get("current_price", 0)
        pnl_pct = state.get("pnl_pct", 0) * 100

        prompt = f"""你是交易员，基于研究计划对持仓做出交易决策。

持仓信息：
股票：{stock_code} {stock_name}
买入价：{buy_price}
当前价：{current_price if current_price else '?'}
浮盈亏：{pnl_pct:+.1f}%

研究计划：
{investment_plan}

Rating Scale：
- Buy(加仓): 买入更多
- Hold(维持): 保持现有持仓
- Sell(清仓): 卖出全部

输出JSON格式（必须包含所有字段）：
{{"action": "BUY/HOLD/SELL", "reasoning": "理由2-4句", "stop_loss": 可选止损价, "position_sizing": 可选仓位建议"}}

用中文输出合法JSON，不要有任何其他文字。"""

        output = _call_with_fallback(
            prompt=prompt,
            agent_name="Trader",
            render_fn=render_trader_proposal,
            schema=TraderProposal,
        )

        logger.info(f"[{stock_code} Trader] 提案: {output[:150]}...")

        return {"trader_investment_plan": output}

    return trader_node


def create_aggressive_risk_node():
    """激进风控分析师"""
    def node(state: dict) -> dict:
        risk_state = state.get("risk_debate_state", {})
        history = risk_state.get("history", "")

        stock_code = state.get("stock_code", "")
        trader_plan = state.get("trader_investment_plan", "")
        pnl_pct = state.get("pnl_pct", 0) * 100

        prompt = f"""你是激进风控分析师，从激进角度评估交易建议。

你支持大胆交易，追求高收益。

持仓信息：{stock_code}
浮盈亏：{pnl_pct:+.1f}%
交易员建议：{trader_plan}
风险辩论历史：{history}

用中文输出你的激进风险评估（3-4句），包含对潜在收益的肯定和对可接受风险的认识。不要有其他格式。"""

        call_llm, _ = _get_llm_clients()
        response = call_llm(
            prompt=prompt,
            model="volcengine-plan/ark-code-latest",
            system="你是激进风控分析师，用中文输出。",
            temperature=0.3,
            thinking_budget=DEBATE_THINKING_BUDGET,
        )

        argument = f"Aggressive Analyst: {response}"

        new_risk_state = {
            "aggressive_history": risk_state.get("aggressive_history", "") + "\n" + argument,
            "conservative_history": risk_state.get("conservative_history", ""),
            "neutral_history": risk_state.get("neutral_history", ""),
            "history": history + "\n" + argument,
            "latest_speaker": "Aggressive Analyst",
            "current_aggressive_response": argument,
            "count": risk_state.get("count", 0) + 1,
            "judge_decision": risk_state.get("judge_decision", ""),
        }

        logger.info(f"[{stock_code} Aggressive] 输出: {response[:80]}...")

        return {"risk_debate_state": new_risk_state}

    return node


def create_conservative_risk_node():
    """保守风控分析师"""
    def node(state: dict) -> dict:
        risk_state = state.get("risk_debate_state", {})
        history = risk_state.get("history", "")

        stock_code = state.get("stock_code", "")
        trader_plan = state.get("trader_investment_plan", "")
        pnl_pct = state.get("pnl_pct", 0) * 100

        prompt = f"""你是保守风控分析师，从保守角度评估交易建议。

你强调风险控制，主张确定性操作。

持仓信息：{stock_code}
浮盈亏：{pnl_pct:+.1f}%
交易员建议：{trader_plan}
风险辩论历史：{history}

用中文输出你的保守风险评估（3-4句），包含对潜在亏损的警惕和对风险管理重要性的强调。不要有其他格式。"""

        call_llm, _ = _get_llm_clients()
        response = call_llm(
            prompt=prompt,
            model="volcengine-plan/ark-code-latest",
            system="你是保守风控分析师，用中文输出。",
            temperature=0.3,
            thinking_budget=DEBATE_THINKING_BUDGET,
        )

        argument = f"Conservative Analyst: {response}"

        new_risk_state = {
            "aggressive_history": risk_state.get("aggressive_history", ""),
            "conservative_history": risk_state.get("conservative_history", "") + "\n" + argument,
            "neutral_history": risk_state.get("neutral_history", ""),
            "history": history + "\n" + argument,
            "latest_speaker": "Conservative Analyst",
            "current_conservative_response": argument,
            "count": risk_state.get("count", 0) + 1,
            "judge_decision": risk_state.get("judge_decision", ""),
        }

        logger.info(f"[{stock_code} Conservative] 输出: {response[:80]}...")

        return {"risk_debate_state": new_risk_state}

    return node


def create_neutral_risk_node():
    """中性风控分析师"""
    def node(state: dict) -> dict:
        risk_state = state.get("risk_debate_state", {})
        history = risk_state.get("history", "")

        stock_code = state.get("stock_code", "")
        trader_plan = state.get("trader_investment_plan", "")
        pnl_pct = state.get("pnl_pct", 0) * 100

        prompt = f"""你是中性风控分析师，从中立角度评估交易建议。

你平衡利弊，客观分析风险收益比。

持仓信息：{stock_code}
浮盈亏：{pnl_pct:+.1f}%
交易员建议：{trader_plan}
风险辩论历史：{history}

用中文输出你的中性风险评估（3-4句），包含对多空因素的平衡分析。不要有其他格式。"""

        call_llm, _ = _get_llm_clients()
        response = call_llm(
            prompt=prompt,
            model="volcengine-plan/ark-code-latest",
            system="你是中性风控分析师，用中文输出。",
            temperature=0.3,
            thinking_budget=DEBATE_THINKING_BUDGET,
        )

        argument = f"Neutral Analyst: {response}"

        new_risk_state = {
            "aggressive_history": risk_state.get("aggressive_history", ""),
            "conservative_history": risk_state.get("conservative_history", ""),
            "neutral_history": risk_state.get("neutral_history", "") + "\n" + argument,
            "history": history + "\n" + argument,
            "latest_speaker": "Neutral Analyst",
            "current_neutral_response": argument,
            "count": risk_state.get("count", 0) + 1,
            "judge_decision": risk_state.get("judge_decision", ""),
        }

        logger.info(f"[{stock_code} Neutral] 输出: {response[:80]}...")

        return {"risk_debate_state": new_risk_state}

    return node


def create_portfolio_manager_stock_node():
    """
    组合经理（最终决策）
    
    对齐 TradingAgents Portfolio Manager（structured output）
    """
    def node(state: dict) -> dict:
        risk_state = state.get("risk_debate_state", {})
        history = risk_state.get("history", "")

        stock_code = state.get("stock_code", "")
        stock_name = state.get("stock_name", "")
        investment_plan = state.get("investment_plan", "")
        trader_plan = state.get("trader_investment_plan", "")
        pnl_pct = state.get("pnl_pct", 0) * 100

        prompt = f"""你是组合经理，综合风险辩论后给出最终持仓决策。

持仓信息：{stock_code} {stock_name}
浮盈亏：{pnl_pct:+.1f}%

研究计划：{investment_plan}
交易员建议：{trader_plan}
风险辩论历史：{history}

Rating Scale：
- Buy(加仓): 强烈看多
- Hold(维持): 中性观望
- Sell(清仓): 强烈看空

输出JSON格式（必须包含所有字段）：
{{"rating": "Buy/Hold/Sell", "executive_summary": "执行摘要2-3句", "investment_thesis": "投资论点3-5句", "confidence": "高/中/低"}}

用中文输出合法JSON，不要有任何其他文字。"""

        output = _call_with_fallback(
            prompt=prompt,
            agent_name="Portfolio Manager",
            render_fn=render_stock_decision,
            schema=StockDecision,
        )

        new_risk_state = {
            "aggressive_history": risk_state.get("aggressive_history", ""),
            "conservative_history": risk_state.get("conservative_history", ""),
            "neutral_history": risk_state.get("neutral_history", ""),
            "history": history,
            "latest_speaker": "Portfolio Manager",
            "judge_decision": output,
            "count": risk_state.get("count", 0),
        }

        logger.info(f"[{stock_code} Portfolio Manager] 最终决策: {output[:150]}...")

        return {
            "risk_debate_state": new_risk_state,
            "final_trade_decision": output,
            "past_context": f"最终决策：{output}",
        }

    return node
