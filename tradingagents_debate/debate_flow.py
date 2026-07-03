"""
TradingAgents 辩论流程编排器 v3
================================
论文架构:
  AnalystTeam(并行4维) → Bull↔Bear研究员(辩论2轮) → ResearchManager(裁决)
  → Aggressive↔Conservative↔Neutral风控(各1轮，并行)
  → FundManager(5档最终决策)

优化点(v3):
  - 风控压缩为1轮（3方并行）
  - Bull/Bear 每轮内并行（threading）
  - K线形态检测 + 知识库动态注入
  - 预获取所有候选股OHLCV

用法:
  flow = DebateFlow(model="minimax-m3")
  result = flow.run(portfolio, market_data)
"""

import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from .agents import (
    AnalystTeam,
    BullResearcher, BearResearcher, ResearchManager,
    AggressiveRisk, ConservativeRisk, NeutralRisk,
    FundManager,
    apply_portfolio_constraint,
)

logger = logging.getLogger("debate_flow")

# ── 辩论轮数配置 ─────────────────────────────────────────
MAX_DEBATE_ROUNDS = 2       # Bull/Bear 研究员辩论轮数（不变）
MAX_RISK_ROUNDS = 1         # 风控轮数：2→1，节省 50% 风控调用
MAX_HOLD_STOCKS = 10        # 最大持仓数


class DebateFlow:
    """辩论流程编排器 v3"""

    def __init__(self, model: str = "minimax-m3", checkpoint_file: str = None):
        from pathlib import Path
        self.model = model
        self.analyst = AnalystTeam(model=model)
        self.bull = BullResearcher(model=model)
        self.bear = BearResearcher(model=model)
        self.research_mgr = ResearchManager(model=model)
        self.risk_agg = AggressiveRisk(model=model)
        self.risk_con = ConservativeRisk(model=model)
        self.risk_neu = NeutralRisk(model=model)
        self.fund_mgr = FundManager(model=model)
        self._kline_ctx: Dict[str, str] = {}  # code → K线上下文
        if checkpoint_file:
            self._ckpt_file = Path(checkpoint_file)
        else:
            base = Path(__file__).parent.parent
            self._ckpt_file = base / "output" / "debate_checkpoint.json"
        self._ckpt_file.parent.mkdir(parents=True, exist_ok=True)

    def set_kline_context(self, stock_code: str, kline_context: str):
        """预先注入K线上下文（含形态检测+知识库）"""
        self._kline_ctx[str(stock_code)] = kline_context

    def _get_kline_ctx(self, stock_code: str) -> str:
        return self._kline_ctx.get(str(stock_code), "")

    # ── 断点续跑 ─────────────────────────────────────────
    def _load_checkpoint(self) -> Dict[str, Any]:
        if not self._ckpt_file.exists():
            return {}
        try:
            mtime = datetime.fromtimestamp(self._ckpt_file.stat().st_mtime).date()
            if mtime != datetime.now().date():
                logger.info(f"[断点] 文件过期（{mtime}），删除旧断点")
                self._ckpt_file.unlink()
                return {}
            import json
            data = json.loads(self._ckpt_file.read_text(encoding="utf-8"))
            logger.info(f"[断点] 加载 {len(data)} 只股票断点数据")
            return data
        except Exception as e:
            logger.warning(f"[断点] 读取失败: {e}")
            return {}

    def _save_checkpoint(self, code: str, phase: str, round_done: int, data: Dict):
        import json
        ckpt = self._load_checkpoint()
        ckpt[str(code)] = {"phase": phase, "round_done": round_done, "data": data}
        tmp = self._ckpt_file.parent / ".debate_checkpoint.tmp"
        tmp.write_text(json.dumps(ckpt, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self._ckpt_file)

    def _get_checkpoint(self, code: str) -> Optional[Dict]:
        ckpt = self._load_checkpoint()
        return ckpt.get(str(code))

    def _run_bull_bear_round(self, stock: Dict, analyst_report: str,
                              prev_bull: str, prev_bear: str,
                              round_num: int) -> tuple:
        """
        并行执行一轮 Bull + Bear（线程级并行）
        返回: (bull_arg, bear_arg, elapsed)
        """
        t0 = time.time()
        bull_arg = None
        bear_arg = None
        bull_err = None
        bear_err = None

        def do_bull():
            try:
                return self.bull.argue(stock, analyst_report, prev_bear, round_num)
            except Exception as e:
                return f"[Bull异常] {e}"

        def do_bear():
            try:
                return self.bear.argue(stock, analyst_report, prev_bull, round_num)
            except Exception as e:
                return f"[Bear异常] {e}"

        with ThreadPoolExecutor(max_workers=2) as ex:
            f_bull = ex.submit(do_bull)
            f_bear = ex.submit(do_bear)

            try:
                bull_arg = f_bull.result(timeout=80)
            except Exception as e:
                bull_err = str(e)
                bull_arg = ""

            try:
                bear_arg = f_bear.result(timeout=80)
            except Exception as e:
                bear_err = str(e)
                bear_arg = ""

        elapsed = time.time() - t0
        logger.info(f"  [辩论第{round_num}轮] {elapsed:.1f}s (Bull/Bear并行)"
                    + (f" Bull异常={bull_err}" if bull_err else "")
                    + (f" Bear异常={bear_err}" if bear_err else ""))

        # 轮次间隔（给LLM冷却时间）
        time.sleep(1)
        return bull_arg, bear_arg, elapsed

    def _run_risk_parallel(self, stock: Dict, analyst_report: str,
                           bull_arg: str, bear_arg: str) -> tuple:
        """
        三方风控并行执行（激进/保守/中性同时调用）
        返回: (risk_agg, risk_con, risk_neu, elapsed)
        """
        t0 = time.time()
        r_agg = None
        r_con = None
        r_neu = None

        def do_agg():
            try:
                return self.risk_agg.assess(stock, analyst_report, bull_arg, bear_arg, "", "", "", 1)
            except Exception as e:
                return f"[激进风控异常] {e}"

        def do_con():
            try:
                return self.risk_con.assess(stock, analyst_report, bull_arg, bear_arg, "", "", "", 1)
            except Exception as e:
                return f"[保守风控异常] {e}"

        def do_neu():
            try:
                return self.risk_neu.assess(stock, analyst_report, bull_arg, bear_arg, "", "", "", 1)
            except Exception as e:
                return f"[中性风控异常] {e}"

        with ThreadPoolExecutor(max_workers=3) as ex:
            f_agg = ex.submit(do_agg)
            f_con = ex.submit(do_con)
            f_neu = ex.submit(do_neu)

            for f in [f_agg, f_con, f_neu]:
                try:
                    result = f.result(timeout=80)
                except Exception:
                    result = f"[风控超时/异常]"
                if f is f_agg:
                    r_agg = result
                elif f is f_con:
                    r_con = result
                elif f is f_neu:
                    r_neu = result

        elapsed = time.time() - t0
        logger.info(f"  [风控] {elapsed:.1f}s (三方并行)")
        time.sleep(1)
        return r_agg, r_con, r_neu, elapsed

    def debate_single(self, stock: Dict, market_data: Dict,
                      all_stocks: List[Dict]) -> Dict:
        """对单只股票执行完整辩论流程，支持断点续跑"""
        code = stock.get("code", "?")
        name = stock.get("name", "?")
        t_total = time.time()
        logger.info(f"[辩论] {code} {name}")

        # ── 断点检查 ─────────────────────────────────────
        ckpt = self._get_checkpoint(code)
        if ckpt and ckpt.get("phase") == "done":
            logger.info(f"  [断点] {code} 已完成辩论，跳过")
            return ckpt["data"]

        # ── K线上下文注入 ─────────────────────────────────
        kline_ctx = self._get_kline_ctx(code)
        if kline_ctx:
            stock["_kline_ctx"] = kline_ctx

        # ── Phase 1: 分析师 ───────────────────────────────
        analyst_report = ""
        if ckpt and ckpt.get("phase") == "analyst_done":
            analyst_report = ckpt["data"].get("analyst", "")
            logger.info(f"  [断点] 跳过分析师（已缓存）")
        else:
            t0 = time.time()
            analyst_report = self.analyst.analyze(stock, market_data)
            logger.info(f"  [分析师] {time.time()-t0:.1f}s")
            self._save_checkpoint(code, "analyst_done", 0,
                {"analyst": analyst_report})
            time.sleep(1)

        # ── Phase 2: Bull/Bear 2轮辩论 ─────────────────────
        bull_args = []
        bear_args = []
        start_round = 1
        if ckpt and ckpt.get("phase") == "bull_bear_done":
            bull_args = ckpt["data"].get("bull_args", [])
            bear_args = ckpt["data"].get("bear_args", [])
            start_round = len(bull_args) + 1
            logger.info(f"  [断点] Bull/Bear 已完成 {len(bull_args)} 轮，从第 {start_round} 轮继续")

        for rnd in range(start_round, MAX_DEBATE_ROUNDS + 1):
            prev_bull = bull_args[-1] if bull_args else ""
            prev_bear = bear_args[-1] if bear_args else ""
            bull_arg, bear_arg, _ = self._run_bull_bear_round(
                stock, analyst_report, prev_bull, prev_bear, rnd
            )
            bull_args.append(bull_arg)
            bear_args.append(bear_arg)
            self._save_checkpoint(code, "bull_bear_done", rnd,
                {"analyst": analyst_report, "bull_args": bull_args, "bear_args": bear_args})

        bull_arg_final = bull_args[-1] if bull_args else ""
        bear_arg_final = bear_args[-1] if bear_args else ""

        # ── Phase 2b: 研究总监裁决 ─────────────────────────
        judge_result = {}
        if ckpt and ckpt.get("phase") in ("judge_done", "risk_done", "done"):
            judge_result = ckpt["data"].get("judge_result", {})
            logger.info(f"  [断点] 跳过裁决（已缓存）")
        else:
            t0 = time.time()
            judge_result = self.research_mgr.judge(
                stock, analyst_report, bull_arg_final, bear_arg_final, MAX_DEBATE_ROUNDS
            )
            logger.info(f"  [裁决] {time.time()-t0:.1f}s winner={judge_result.get('winner')}")
            self._save_checkpoint(code, "judge_done", MAX_DEBATE_ROUNDS,
                {"analyst": analyst_report, "bull_args": bull_args, "bear_args": bear_args,
                 "judge_result": judge_result})
            time.sleep(1)

        # ── Phase 3: 三方风控 ──────────────────────────────
        risk_agg = ""
        risk_con = ""
        risk_neu = ""
        if ckpt and ckpt.get("phase") in ("risk_done", "done"):
            risk_agg = ckpt["data"].get("risk_agg", "")
            risk_con = ckpt["data"].get("risk_con", "")
            risk_neu = ckpt["data"].get("risk_neu", "")
            logger.info(f"  [断点] 跳过风控（已缓存）")
        else:
            risk_agg, risk_con, risk_neu, _ = self._run_risk_parallel(
                stock, analyst_report, bull_arg_final, bear_arg_final
            )
            self._save_checkpoint(code, "risk_done", MAX_DEBATE_ROUNDS,
                {"analyst": analyst_report, "bull_args": bull_args, "bear_args": bear_args,
                 "judge_result": judge_result, "risk_agg": risk_agg,
                 "risk_con": risk_con, "risk_neu": risk_neu})

        # ── Phase 4: 基金经理决策 ──────────────────────────
        t0 = time.time()
        decision = self.fund_mgr.decide(
            stock, analyst_report, bull_arg_final, bear_arg_final, judge_result,
            risk_agg, risk_con, risk_neu, all_stocks,
        )
        logger.info(f"  [决策] {time.time()-t0:.1f}s → {decision.get('action','?')}")
        logger.info(f"  [总耗时] {time.time()-t_total:.1f}s")

        result = {
            "code": code,
            "name": name,
            "quantity": stock.get("quantity", 0),
            "cost": stock.get("cost", 0),
            "current_price": stock.get("current_price", 0),
            "pnl_pct": stock.get("pnl_pct", 0),
            "analyst": analyst_report[:2000],
            "bull_arg": bull_arg_final[:1200],
            "bear_arg": bear_arg_final[:1200],
            "judge": judge_result,
            "risk_aggressive": risk_agg[:600] if risk_agg else "",
            "risk_conservative": risk_con[:600] if risk_con else "",
            "risk_neutral": risk_neu[:600] if risk_neu else "",
            "decision": decision,
        }
        self._save_checkpoint(code, "done", MAX_DEBATE_ROUNDS, result)
        return result

    def run(self, portfolio: List[Dict], market_data: Dict) -> Dict:
        """对整个持仓组合运行辩论"""
        if not portfolio:
            return {
                "status": "empty",
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "message": "无持仓，辩论跳过",
            }

        logger.info(f"[辩论启动] {len(portfolio)} 只股票")
        results = []

        for i, stock in enumerate(portfolio):
            logger.info(f"─── [{i+1}/{len(portfolio)}] ───")
            result = self.debate_single(stock, market_data, portfolio)
            results.append(result)

            # 每只股票间隔2秒（减少API限流风险）
            if i < len(portfolio) - 1:
                time.sleep(2)

        # ── 持仓约束：最多MAX_HOLD_STOCKS只 ────────────────
        results = apply_portfolio_constraint(results, MAX_HOLD_STOCKS)

        # ── 汇总 ────────────────────────────────────────────
        action_summary = {"STRONG_BUY": 0, "BUY": 0, "HOLD": 0, "REDUCE": 0, "CLEAR": 0}
        for r in results:
            a = r.get("decision", {}).get("action", "HOLD")
            if a in action_summary:
                action_summary[a] += 1

        # 兼容旧 action 格式（ADD→BUY 映射给 execute_debate_result）
        for r in results:
            action = r["decision"].get("action", "HOLD")
            if action in ("STRONG_BUY", "BUY"):
                r["decision"]["action_compat"] = "ADD"
            elif action == "REDUCE":
                r["decision"]["action_compat"] = "REDUCE"
            elif action == "CLEAR":
                r["decision"]["action_compat"] = "CLEAR"
            else:
                r["decision"]["action_compat"] = "HOLD"

        return {
            "status": "done",
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "market_data_date": market_data.get("data_date", ""),
            "total_stocks": len(portfolio),
            "action_summary": action_summary,
            "stocks": results,
        }
