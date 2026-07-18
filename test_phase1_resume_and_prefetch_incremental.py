import copy
import inspect
import json
import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


class Phase1ResumeAndPrefetchIncrementalTests(unittest.TestCase):
    def test_stable_runner_accepts_today_phase1_context(self):
        import run_daily_stock_workflow_stable as stable

        with tempfile.TemporaryDirectory() as tmp:
            phase1_file = Path(tmp) / "phase1_context.json"
            phase1_file.write_text('[{"status":"success","name":"新闻分析师"}]', encoding="utf-8")
            with mock.patch.object(stable, "_phase1_context_path", return_value=phase1_file), \
                    mock.patch.object(stable, "_checkpoint_matches_today", return_value=False):
                self.assertTrue(stable._phase1_context_matches_today())
                self.assertEqual(stable._resume_state_kind(), "phase1")
                self.assertTrue(stable._resume_state_available())

    def test_stable_runner_requires_matching_successful_push_marker(self):
        import run_daily_stock_workflow_stable as stable

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifacts = {}
            for key in ("candidates_jsonl", "trace_json", "summary_json"):
                artifact = root / f"{key}.json"
                artifact.write_text("{}", encoding="utf-8")
                artifacts[key] = str(artifact)
            report = {
                "status": "success",
                "date": stable._today(),
                "phase2": {"top_picks": [{"stock": str(i)} for i in range(5)]},
                "artifacts": artifacts,
            }
            report_file = root / f"daily_report_{stable._today()}.json"
            report_file.write_text(json.dumps(report), encoding="utf-8")
            marker_file = root / f"daily_report_push_{stable._today()}.json"

            with mock.patch.object(stable, "_push_marker_path", return_value=marker_file):
                self.assertFalse(stable._report_is_complete(report_file))
                marker_file.write_text(json.dumps({
                    "date": stable.date.today().isoformat(),
                    "status": "success",
                    "top_picks_count": 5,
                    "report_digest": "wrong",
                }), encoding="utf-8")
                self.assertFalse(stable._report_is_complete(report_file))

                payload = json.dumps(report, ensure_ascii=False, sort_keys=True, default=str)
                marker = json.loads(marker_file.read_text(encoding="utf-8"))
                marker["report_digest"] = stable.hashlib.sha256(payload.encode("utf-8")).hexdigest()
                marker_file.write_text(json.dumps(marker), encoding="utf-8")
                self.assertTrue(stable._report_is_complete(report_file))

    def test_phase1_candidate_snapshot_round_trip(self):
        import workflow

        with tempfile.TemporaryDirectory() as tmp:
            gen = SimpleNamespace(
                screening_signature="screen-v1",
                candidates=[{"stock": "600001", "name": "测试股", "pool_score": 66.5}],
            )
            with mock.patch.object(workflow, "OUTPUT_DIR", Path(tmp)), \
                    mock.patch("llm_scorer._screening_signature", return_value="screen-v1"):
                workflow._save_phase1_candidates(gen)
                restored = workflow._load_phase1_candidates()

            self.assertEqual(restored["screening_signature"], "screen-v1")
            self.assertEqual(restored["candidates"], gen.candidates)

    def test_push_marker_digest_matches_persisted_compact_report(self):
        import workflow

        report = {
            "date": workflow.date.today().isoformat(),
            "status": "success",
            "phase1": [],
            "phase2": {
                "top_picks": [{"stock": str(i), "name": f"测试{i}"} for i in range(5)],
                "ranked_candidates": [],
            },
            "phase3_selection": {},
            "phase3_strategy": {},
            "timestamp": "2026-07-15T00:00:00",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(workflow, "OUTPUT_DIR", Path(tmp)):
            workflow._mark_daily_report_pushed(report)
            marker = workflow._read_daily_push_marker()

        self.assertEqual(
            marker["report_digest"],
            workflow._stable_digest(workflow._compact_daily_report(report)),
        )

    def test_report_card_renderer_does_not_mutate_sector_rotation(self):
        import workflow

        source = inspect.getsource(workflow._send_daily_report_card)
        self.assertNotIn('phase2_result["sector_rotation"] =', source)

    def test_phase1_financial_cache_normalizes_legacy_fields(self):
        import stock_selection_debate.data_fetcher as df

        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "fundamental_cache"
            cache_dir.mkdir(parents=True)
            (cache_dir / "all_stocks_financial.json").write_text(
                json.dumps({
                    "data": {
                        "600001.SH": {
                            "roe": 12.5,
                            "营收增速": 8.1,
                            "净利润增长率": 9.2,
                            "毛利率": 31.0,
                            "负债率": 44.0,
                            "pe": 18.6,
                            "市净率": 2.3,
                            "行业": "电子",
                        },
                    },
                }, ensure_ascii=False),
                encoding="utf-8",
            )
            cache = df.load_phase1_cache(Path(tmp))

        self.assertEqual(set(cache), {"600001"})
        record = cache["600001"]
        self.assertEqual(record["roe_annual_latest"], 12.5)
        self.assertEqual(record["revenue_growth_yoy"], 8.1)
        self.assertEqual(record["net_profit_growth_yoy"], 9.2)
        self.assertEqual(record["gross_margin"], 31.0)
        self.assertEqual(record["debt_asset_ratio"], 44.0)
        self.assertEqual(record["pe_ttm"], 18.6)
        self.assertEqual(record["pb"], 2.3)
        self.assertEqual(record["sector"], "电子")
        self.assertEqual(record["负债率"], 44.0)

    def test_debate_packet_prefers_phase1_financial_cache(self):
        import stock_selection_debate.data_fetcher as df

        today = df.date.today().strftime("%Y%m%d")
        klines = [
            {
                "date": f"2026{(idx // 28) + 1:02d}{(idx % 28) + 1:02d}",
                "open": 10 + idx * 0.01,
                "high": 10.2 + idx * 0.01,
                "low": 9.8 + idx * 0.01,
                "close": 10.1 + idx * 0.01,
                "volume": 100000 + idx,
            }
            for idx in range(120)
        ]
        cached_packet = {
            "600001": {
                "updated": today,
                "klines": klines,
                "news": [{"title": "测试新闻", "time": today, "source": "cache"}],
            },
        }
        phase1_cache = {
            "600001": {
                "roe": 11.5,
                "营收增速": 7.2,
                "净利润增长率": 6.3,
                "负债率": 40.1,
                "行业": "电子",
            },
        }
        money_flow = {
            "main_net_flow": 1.2,
            "super_net_flow": 0.4,
            "ddx_5": 0.1,
            "ddy_10": 0.2,
            "source": "cache_hot",
            "as_of": today,
        }
        with mock.patch.dict(os.environ, {
            "FINANCIAL_LIVE_REFRESH_IN_PACKET": "0",
            "MONEY_FLOW_LIVE_FETCH_IN_PACKET": "0",
        }, clear=False), \
                mock.patch.object(df, "_load_debate_data_cache", return_value=cached_packet), \
                mock.patch.object(df, "_save_debate_data_cache"), \
                mock.patch.object(df, "_set_cached_sector"), \
                mock.patch.object(df, "_get_cached_money_flow", return_value=money_flow), \
                mock.patch.object(df, "_fetch_financial_via_xqshare") as fetch_financial:
            packet = df.build_debate_packet("600001", "测试股", phase1_cache, [])

        fetch_financial.assert_not_called()
        self.assertEqual(packet["financial"]["roe"], 11.5)
        self.assertEqual(packet["financial"]["revenue_growth"], 7.2)
        self.assertEqual(packet["financial"]["net_profit_growth"], 6.3)
        self.assertEqual(packet["financial"]["debt_ratio"], 40.1)
        self.assertEqual(packet["financial"]["_fin_source"], "phase1_cache")
        self.assertEqual(packet["data_contract"]["financial"]["source"], "phase1_cache")

    def test_report_price_cache_reuses_verified_snapshot_and_rejects_stale(self):
        import workflow

        fresh = {
            "stock": "600001.SH",
            "verified_market_snapshot": {
                "status": "ok",
                "source": "debate_data_cache:xqshare",
                "latest_date": "20260715",
                "latest_close": 12.0,
                "previous_close": 10.0,
                "pct_change_1d": 20.0,
            },
            "data_contract": {"kline": {"status": "ok", "is_stale": False}},
        }
        stale = copy.deepcopy(fresh)
        stale["stock"] = "600002"
        stale["data_contract"]["kline"]["is_stale"] = True

        price_cache = workflow._seed_report_price_cache([fresh, stale])

        self.assertEqual(set(price_cache), {"600001"})
        self.assertEqual(price_cache["600001"]["close"], 12.0)
        self.assertEqual(price_cache["600001"]["prev_close"], 10.0)
        self.assertEqual(price_cache["600001"]["pct"], 20.0)
        self.assertIn("verified_market_snapshot", price_cache["600001"]["source"])

    def test_candidate_financial_merge_uses_normalized_packet_fields(self):
        import workflow

        packet = {
            "financial": {
                "roe": 15.0,
                "revenue_growth": None,
                "debt_ratio": None,
                "_fin_raw": {"source_marker": "packet"},
            },
        }
        workflow._merge_candidate_financial(packet, {
            "roe": 99.0,
            "营收增速": 8.8,
            "负债率": 41.2,
        })

        self.assertEqual(packet["financial"]["roe"], 15.0)
        self.assertEqual(packet["financial"]["revenue_growth"], 8.8)
        self.assertEqual(packet["financial"]["debt_ratio"], 41.2)
        self.assertEqual(packet["financial"]["_fin_raw"]["source_marker"], "packet")
        self.assertEqual(packet["financial"]["_fin_raw"]["revenue_growth_yoy"], 8.8)

    def test_prefetch_persists_completed_stock_before_later_failure(self):
        import stock_selection_debate.data_fetcher as df

        cache_holder = {}

        def load_cache():
            return copy.deepcopy(cache_holder)

        def save_cache(data):
            cache_holder.clear()
            cache_holder.update(copy.deepcopy(data))

        calls = []

        def fetch_mx(stock):
            calls.append(stock)
            if stock == "600001":
                return {
                    "main_net_flow": 1.2,
                    "super_net_flow": 0.4,
                    "ddx_5": 0.1,
                    "ddy_10": 0.2,
                    "source": "mx-data",
                    "as_of": "20260715",
                }
            raise RuntimeError("simulated interruption")

        env = {
            "ENABLE_MX_MONEY_FLOW_PREFETCH": "1",
            "MX_MONEY_FLOW_PREFETCH_BATCH": "1",
            "MX_MONEY_FLOW_PREFETCH_BATCH_PAUSE_SEC": "0",
            "MONEY_FLOW_PREFETCH_BUDGET_SEC": "60",
        }
        with ExitStack() as stack:
            stack.enter_context(mock.patch.dict(os.environ, env, clear=False))
            stack.enter_context(mock.patch.object(df, "_load_debate_data_cache", side_effect=load_cache))
            stack.enter_context(mock.patch.object(df, "_save_debate_data_cache", side_effect=save_cache))
            stack.enter_context(mock.patch.object(df, "_fetch_money_flow_via_mx", side_effect=fetch_mx))
            with self.assertRaisesRegex(RuntimeError, "simulated interruption"):
                df._prefetch_debate_data([
                    {"stock": "600001", "name": "测试一"},
                    {"stock": "600002", "name": "测试二"},
                ])

        self.assertEqual(calls, ["600001", "600002"])
        self.assertEqual(cache_holder["600001"]["money_flow"]["main_net_flow"], 1.2)
        self.assertTrue(cache_holder["600001"]["prefetch_partial"])

    def test_prefetch_logger_reaches_workflow_hierarchy(self):
        import stock_selection_debate.data_fetcher as df

        self.assertEqual(df.logger.name, "daily_stock_workflow.data_fetcher")

    def test_role_evidence_guard_ignores_negations_conditions_and_thresholds(self):
        import stock_selection_debate.debate_engine as engine

        packet = {
            "data_contract": {
                key: {"status": "ok"}
                for key in ("kline", "money_flow", "financial", "sector", "news")
            },
            "indicators": {
                "macd_state": "空头",
                "macd_cross_event": "无",
                "rsi_14": 51.2,
            },
            "kline_summary": {"ma_system": "空头排列"},
        }
        response = (
            "当前尚未形成MACD金叉，也未形成MACD多头。"
            "若后续MACD金叉再确认，可作为触发条件。"
            "目前未形成均线多头排列，RSI>75才属于严重超买。"
        )

        self.assertEqual(engine._role_evidence_errors(packet, response), [])

    def test_role_evidence_guard_still_rejects_explicit_false_claims(self):
        import stock_selection_debate.debate_engine as engine

        packet = {
            "data_contract": {"kline": {"status": "ok"}},
            "indicators": {
                "macd_state": "空头",
                "macd_cross_event": "无",
                "rsi_14": 51.2,
            },
            "kline_summary": {"ma_system": "空头排列"},
        }
        errors = engine._role_evidence_errors(
            packet,
            "MACD金叉已经形成，当前为MACD多头，均线多头排列，RSI为75。",
        )

        self.assertTrue(any("MACD金叉" in item for item in errors))
        self.assertTrue(any("MACD多头" in item for item in errors))
        self.assertTrue(any("均线多头排列" in item for item in errors))
        self.assertTrue(any("RSI引用75" in item for item in errors))

    def test_role_evidence_guard_ignores_attributed_or_rebutted_claims(self):
        import stock_selection_debate.debate_engine as engine

        packet = {
            "data_contract": {"kline": {"status": "ok"}},
            "indicators": {
                "macd_state": "空头",
                "macd_cross_event": "无",
                "rsi_14": 51.2,
            },
            "kline_summary": {"ma_system": "混乱"},
        }
        response = (
            "多方声称“均线多头排列”，但数据包实际为混乱。"
            "我反驳多方的MACD金叉假设。"
            "空方观点：MACD多头，这一说法并不成立。"
        )

        self.assertEqual(engine._role_evidence_errors(packet, response), [])
        self.assertTrue(engine._phrase_is_asserted("多方所说的均线多头排列确实成立。", "均线多头排列"))

    def test_role_evidence_guard_catches_formatted_macd_and_ma_claims(self):
        import stock_selection_debate.debate_engine as engine

        packet = {
            "stock_name": "测试股",
            "data_contract": {"kline": {"status": "ok"}},
            "indicators": {
                "macd_state": "多头",
                "macd_cross_event": "无",
                "rsi_14": 55.0,
            },
            "kline_summary": {"ma_system": "混乱"},
        }
        response = (
            "MACD动量确认：金叉有效，柱线扩张。"
            "均线系统得分14/15，完美多头排列。"
        )
        errors = engine._role_evidence_errors(packet, response)
        self.assertTrue(any("MACD金叉" in item for item in errors))
        self.assertTrue(any("均线多头排列" in item for item in errors))

        repaired = engine._deterministic_role_evidence_repair(packet, response, errors)
        self.assertIn("MACD多头状态（本日无新金叉事件）", repaired)
        self.assertIn("均线系统混乱", repaired)
        self.assertNotIn("金叉有效", repaired)
        self.assertNotIn("完美多头排列", repaired)
        self.assertEqual(engine._role_evidence_errors(packet, repaired), [])
        self.assertEqual(
            engine._role_evidence_errors(packet, "MACD多头排列，但均线系统混乱。"),
            [],
        )

    def test_role_evidence_guard_catches_compact_ma_claims(self):
        import stock_selection_debate.debate_engine as engine

        packet = {
            "stock_name": "金安国纪",
            "data_contract": {"kline": {"status": "ok"}},
            "indicators": {
                "macd_state": "空头",
                "macd_cross_event": "无",
                "rsi_14": 48.0,
            },
            "kline_summary": {"ma_system": "混乱"},
        }
        for claim in (
            "技术面均线空头、MACD偏空。",
            "均线空排，短线承压。",
            "均线系统整体呈空头格局。",
        ):
            errors = engine._role_evidence_errors(packet, claim)
            self.assertTrue(any("均线空头排列" in item for item in errors), claim)
            repaired = engine._deterministic_role_evidence_repair(packet, claim, errors)
            self.assertIn("均线系统混乱", repaired)
            self.assertNotIn("均线空头", repaired)
            self.assertNotIn("均线空排", repaired)
            self.assertEqual(engine._role_evidence_errors(packet, repaired), [])

        self.assertEqual(
            engine._role_evidence_errors(packet, "MACD空头，但均线系统混乱。"),
            [],
        )
        self.assertEqual(
            engine._role_evidence_errors(packet, "均线并非空头，仍是混乱状态。"),
            [],
        )
        self.assertEqual(
            engine._role_evidence_errors(packet, "未形成均线空头，仍需观察。"),
            [],
        )

    def test_role_evidence_guard_validates_money_flow_metric_names_and_values(self):
        import stock_selection_debate.debate_engine as engine

        packet = {
            "stock_name": "宁波中百",
            "data_contract": {
                "money_flow": {
                    "status": "ok",
                    "field_status": {
                        "main_net_flow": "ok",
                        "super_net_flow": "ok",
                        "ddx_5": "ok",
                        "ddy_10": "ok",
                        "main_net_flow_5d": "ok",
                        "main_net_flow_10d": "ok",
                    },
                },
            },
            "money_flow": {
                "main_net_flow": 0.02,
                "super_net_flow": 0.0037,
                "ddx_5": 1.046,
                "ddy_10": -1.39,
                "main_net_flow_5d": -0.2106,
                "main_net_flow_10d": 0.7319,
            },
        }
        response = "5日DDX为1.046，但10日DDX=-0.442，且主力净流入0.7亿元。"
        errors = engine._role_evidence_errors(packet, response)
        self.assertTrue(any("资金流字段10日DDX不可用" in item for item in errors))
        self.assertTrue(any("资金流净额引用" in item for item in errors))

        repaired = engine._deterministic_role_evidence_repair(packet, response, errors)
        self.assertIn("10日DDY=-1.39", repaired)
        self.assertIn("主力净流入0.02亿元", repaired)
        self.assertNotIn("10日DDX", repaired)
        self.assertNotIn("0.7亿元", repaired)
        self.assertEqual(engine._role_evidence_errors(packet, repaired), [])

        mismatch = "10日DDY=-0.442。"
        mismatch_errors = engine._role_evidence_errors(packet, mismatch)
        self.assertTrue(any("10日DDY引用" in item for item in mismatch_errors))
        self.assertEqual(
            engine._role_evidence_errors(
                packet,
                "空方声称10日DDX=-0.442，这一说法并不成立。",
            ),
            [],
        )

    def test_money_flow_repair_expands_compound_ddx_ddy_claims(self):
        import stock_selection_debate.debate_engine as engine

        packet = {
            "data_contract": {
                "money_flow": {
                    "status": "ok",
                    "field_status": {
                        "ddx_5": "ok",
                        "ddy_10": "ok",
                    },
                },
            },
            "money_flow": {
                "ddx_5": -3.476,
                "ddy_10": -4.5,
            },
        }
        response = "单日流入与5/10日DDX=-4.5/DDY深度负值构成资金背离。"
        errors = engine._role_evidence_errors(packet, response)
        self.assertTrue(any("资金流复合指标写法不明确" in item for item in errors))
        self.assertTrue(any("资金流字段10日DDX不可用" in item for item in errors))

        repaired = engine._deterministic_role_evidence_repair(packet, response, errors)
        self.assertIn("5日DDX=-3.476、10日DDY=-4.5", repaired)
        self.assertNotIn("5/10日", repaired)
        self.assertEqual(engine._role_evidence_errors(packet, repaired), [])

    def test_role_evidence_guard_repairs_false_tech_threshold_and_veto(self):
        import stock_selection_debate.debate_engine as engine

        state = {
            "debate_packet": {
                "stock_name": "鑫科材料",
                "data_contract": {"kline": {"status": "ok"}},
                "indicators": {"macd_state": "空头", "macd_cross_event": "无"},
                "kline_summary": {"ma_system": "空头排列"},
            },
            "tech_pattern_score": 38,
            "tech_raw_score": 38,
            "tech_max_score": 100,
            "tech_rule_signal": "AVOID",
            "tech_veto_reasons": [],
        }
        packet = engine._role_evidence_packet(state)
        response = "量化技术分38<35，均线空头、量价背离构成硬否决。"

        errors = engine._role_evidence_errors(packet, response)
        self.assertTrue(any("38<35" in item for item in errors))
        self.assertTrue(any("实际无否决原因" in item for item in errors))

        repaired = engine._deterministic_role_evidence_repair(packet, response, errors)
        self.assertIn("量化技术分38/100", repaired)
        self.assertIn("未触发技术硬否决", repaired)
        self.assertNotIn("38<35", repaired)
        self.assertEqual(engine._role_evidence_errors(packet, repaired), [])
        self.assertEqual(
            engine._role_evidence_errors(packet, "无明显技术硬否决，但需警惕空头趋势。"),
            [],
        )
        self.assertEqual(
            engine._role_evidence_errors(packet, "技术面未现硬否决，仍需盘中确认。"),
            [],
        )
        forced_packet = copy.deepcopy(packet)
        forced_packet["_workflow_evidence"]["tech_pattern_score"] = 28
        forced_packet["_workflow_evidence"]["tech_raw_score"] = 28
        forced_response = "量化技术分28/100且<35，技术面强制否决。"
        forced_errors = engine._role_evidence_errors(forced_packet, forced_response)
        self.assertTrue(any("实际无否决原因" in item for item in forced_errors))
        forced_repaired = engine._deterministic_role_evidence_repair(
            forced_packet,
            forced_response,
            forced_errors,
        )
        self.assertIn("未触发技术硬否决", forced_repaired)
        self.assertEqual(engine._role_evidence_errors(forced_packet, forced_repaired), [])

    def test_resume_revalidates_tech_score_and_veto_facts(self):
        import workflow

        packet = {
            "data_contract": {"kline": {"status": "ok"}},
            "indicators": {"macd_state": "空头", "macd_cross_event": "无"},
            "kline_summary": {"ma_system": "空头排列"},
        }
        errors = workflow._node_checkpoint_evidence_errors(
            packet,
            {
                "current_response": "量化技术分38<35，构成硬否决。",
                "tech_pattern_score": 38,
                "tech_raw_score": 38,
                "tech_veto_reasons": [],
            },
        )
        self.assertTrue(any("38<35" in item for item in errors))
        self.assertTrue(any("实际无否决原因" in item for item in errors))

    def test_resume_rejects_failed_pm_evidence_status(self):
        import workflow

        errors = workflow._node_checkpoint_evidence_errors(
            {"data_contract": {"kline": {"status": "ok"}}},
            {
                "current_response": "等待盘中确认。",
                "evidence_validation": {
                    "status": "fail",
                    "errors": ["声称触发技术硬否决但实际无否决原因"],
                },
            },
        )
        self.assertTrue(any("基金经理证据校验状态失败" in item for item in errors))

    def test_pm_evidence_rejects_false_tech_threshold(self):
        import stock_selection_debate.debate_engine as engine

        packet = engine._role_evidence_packet({
            "debate_packet": {
                "data_contract": {"kline": {"status": "ok"}},
                "indicators": {"macd_state": "空头", "macd_cross_event": "无"},
                "kline_summary": {"ma_system": "空头排列"},
                "verified_market_snapshot": {"close": 4.35},
            },
            "tech_pattern_score": 38,
            "tech_raw_score": 38,
            "tech_veto_reasons": [],
        })
        validation = engine._validate_pm_evidence(
            packet,
            "量化技术分38<35，构成硬否决。",
            [{"field": "verified_market_snapshot.close", "value": 4.35, "claim": "收盘价"}],
            [],
            [],
        )

        self.assertEqual(validation["status"], "fail")
        self.assertTrue(any("38<35" in item for item in validation["errors"]))

    def test_tech_analyst_output_uses_the_same_evidence_guard(self):
        import stock_selection_debate.debate_engine as engine

        state = {
            "stock_code": "600255",
            "stock_name": "鑫科材料",
            "debate_packet": {
                "data_contract": {"kline": {"status": "ok"}},
                "indicators": {"macd_state": "空头", "macd_cross_event": "无"},
                "kline_summary": {"ma_system": "空头排列"},
            },
            "history": "",
            "model": "volcengine-plan/ark-code-latest",
            "node_models_log": [],
        }
        score_result = {
            "total_score": 38,
            "raw_total": 38,
            "max_total": 100,
            "signal": "AVOID",
            "confidence": 38,
            "veto_reasons": [],
            "breakdown": {},
        }
        bad_response = (
            "**技术面综合**: AVOID\n"
            "**核心依据**: MACD死叉，量化技术分38<35。\n"
            "**风险提示**: 均线空头构成硬否决。"
        )

        with mock.patch.object(engine, "compute_tech_score", return_value=score_result), \
                mock.patch.object(engine, "_call_role", return_value=bad_response) as call_role:
            result = engine.tech_analyst_node(state)

        self.assertEqual(call_role.call_count, 1)
        verdict = result["tech_analyst_verdict"]
        self.assertNotIn("MACD死叉", verdict)
        self.assertNotIn("38<35", verdict)
        self.assertIn("未触发技术硬否决", verdict)
        self.assertEqual(result["node_models_log"][-1]["node"], "tech")

    def test_unrepaired_role_evidence_failure_is_retryable(self):
        import stock_selection_debate.debate_engine as engine

        packet = {
            "data_contract": {"kline": {"status": "ok"}},
            "indicators": {"macd_state": "空头", "macd_cross_event": "无"},
            "kline_summary": {"ma_system": "空头排列"},
        }
        bad_response = ""
        with mock.patch.object(engine, "_call_role", side_effect=[bad_response, bad_response]):
            with self.assertRaises(engine.RoleEvidenceValidationError):
                engine._call_role_guarded(
                    "system",
                    "prompt",
                    packet=packet,
                    model="volcengine-plan/ark-code-latest",
                    timeout=1,
                    max_tokens=32,
                )

    def test_deterministic_role_repair_uses_packet_facts(self):
        import stock_selection_debate.debate_engine as engine

        packet = {
            "stock_name": "测试股",
            "data_contract": {"kline": {"status": "ok"}},
            "indicators": {
                "macd_state": "空头",
                "macd_cross_event": "无",
                "rsi_14": 51.2,
            },
            "kline_summary": {"ma_system": "混乱"},
        }
        bad_response = "MACD死叉已经形成，均线空头排列，RSI为75。"
        with mock.patch.object(engine, "_call_role", side_effect=[bad_response, bad_response]) as call_role:
            repaired = engine._call_role_guarded(
                "system",
                "prompt",
                packet=packet,
                model="volcengine-plan/ark-code-latest",
                timeout=1,
                max_tokens=32,
            )

        self.assertEqual(call_role.call_count, 1)
        self.assertIn("MACD空头状态（本日无新死叉事件）", repaired)
        self.assertIn("均线系统混乱", repaired)
        self.assertIn("RSI为51.2", repaired)
        self.assertEqual(engine._role_evidence_errors(packet, repaired), [])

    def test_invalid_role_node_checkpoint_is_not_reused(self):
        import workflow

        self.assertFalse(workflow._node_checkpoint_state_reusable({
            "bull_history": "证据校验未通过，本轮论据不计入后续裁决。",
        }))
        self.assertTrue(workflow._node_checkpoint_state_reusable({
            "bull_history": "基于真实K线与资金流的有效分析",
        }))
        self.assertFalse(workflow._node_checkpoint_entry_reusable(
            "invest.bull_researcher.0.tech",
            {"current_response": "有效但阶段标记错误的分析"},
        ))
        self.assertTrue(workflow._node_checkpoint_entry_reusable(
            "invest.research_manager.4.tech",
            {"current_response": "技术复核后的有效裁决"},
        ))

    def test_resume_node_is_revalidated_against_current_evidence_contract(self):
        import workflow

        packet = {
            "data_contract": {"kline": {"status": "ok"}},
            "indicators": {"macd_state": "多头", "macd_cross_event": "无"},
            "kline_summary": {"ma_system": "混乱"},
        }
        errors = workflow._node_checkpoint_evidence_errors(
            packet,
            {"current_response": "MACD动量确认：金叉有效，均线系统完美多头排列。"},
        )
        self.assertTrue(any("MACD金叉" in item for item in errors))
        self.assertTrue(any("均线多头排列" in item for item in errors))
        self.assertEqual(
            workflow._node_checkpoint_evidence_errors(
                packet,
                {"current_response": "MACD多头状态，本日无新金叉事件；均线系统混乱。"},
            ),
            [],
        )

    def test_only_research_manager_uses_tech_checkpoint_marker(self):
        import stock_selection_debate.debate_engine as engine

        state = {"count": 4, "tech_analyst_verdict": "WATCH"}
        self.assertEqual(
            engine._node_checkpoint_key("invest", "bull_researcher", state),
            "invest.bull_researcher.4.debate",
        )
        self.assertEqual(
            engine._node_checkpoint_key("invest", "research_manager", state),
            "invest.research_manager.4.tech",
        )

    def test_langgraph_retries_resume_with_none_input(self):
        import stock_selection_debate.debate_engine as engine

        invest_result = {
            "stock_code": "600001",
            "stock_name": "测试股",
            "debate_packet": {},
            "market_context": "",
            "research_plan": "研究结论",
            "history": "辩论历史",
            "bull_history": "多方历史",
            "bear_history": "空方历史",
            "model": "volcengine-plan/ark-code-latest",
            "data_quality_flags": [],
            "node_models_log": [],
        }

        class RetryOnceGraph:
            def __init__(self, result=None):
                self.calls = []
                self.result = result
                self.initial = None

            def invoke(self, value, config):
                self.calls.append(value)
                if value is not None:
                    self.initial = value
                if len(self.calls) == 1:
                    raise RuntimeError("simulated node failure")
                return self.result if self.result is not None else self.initial

        debate_engine = object.__new__(engine.StockDebateEngine)
        debate_engine.model = "volcengine-plan/ark-code-latest"
        debate_engine.max_debate_rounds = 1
        debate_engine._invest_graph = RetryOnceGraph(invest_result)
        debate_engine._risk_graph = RetryOnceGraph()

        with mock.patch.object(engine.time, "sleep", return_value=None):
            result = debate_engine._run_single(
                "600001",
                "测试股",
                {},
                "",
                resume_nodes={},
            )

        self.assertIsInstance(debate_engine._invest_graph.calls[0], dict)
        self.assertIsNone(debate_engine._invest_graph.calls[1])
        self.assertIsInstance(debate_engine._risk_graph.calls[0], dict)
        self.assertIsNone(debate_engine._risk_graph.calls[1])
        self.assertEqual(result["stock_code"], "600001")

    def test_current_run_node_checkpoint_is_immediately_available_to_retry(self):
        import stock_selection_debate.debate_engine as engine

        debate_engine = object.__new__(engine.StockDebateEngine)
        persisted = []

        def fake_run_single(code, name, packet, market_context, resume_nodes=None):
            snapshot = {"count": 1, "current_response": "有效多方分析"}
            engine._NODE_CHECKPOINT_LOCAL.callback(
                code,
                "invest.bull_researcher.0.debate",
                snapshot,
            )
            self.assertEqual(
                resume_nodes["invest.bull_researcher.0.debate"],
                snapshot,
            )
            return {"signal": "WATCH", "confidence": 60}

        debate_engine._run_single = fake_run_single
        results = debate_engine.run(
            [{"stock_code": "600001", "stock_name": "测试股"}],
            max_parallel=1,
            node_checkpoint_cb=lambda code, key, state: persisted.append((code, key, state)),
        )

        self.assertEqual(results[0]["signal"], "WATCH")
        self.assertEqual(persisted[0][1], "invest.bull_researcher.0.debate")

    def test_provider_config_is_published_atomically_to_parallel_callers(self):
        import stock_selection_debate.providers as providers

        first_provider_loaded = threading.Event()
        allow_finish = threading.Event()

        class SlowProviders:
            def items(self):
                yield "placeholder", {
                    "api": "openai-completions",
                    "baseUrl": "https://placeholder.invalid/v1",
                    "apiKey": "PLACEHOLDER_KEY",
                }
                first_provider_loaded.set()
                allow_finish.wait(timeout=2)
                yield "volcengine-plan", {
                    "api": "openai-completions",
                    "baseUrl": "https://ark.example/v3",
                    "apiKey": "VOLCANO_ENGINE_API_KEY",
                }

        fake_models = {"providers": SlowProviders()}
        old_map = providers._PROVIDER_MAP
        providers._PROVIDER_MAP = {}
        try:
            with mock.patch.object(providers, "_load_project_env", return_value=None), \
                    mock.patch("builtins.open", mock.mock_open(read_data="{}")), \
                    mock.patch.object(providers.json, "load", return_value=fake_models):
                def worker():
                    providers._load_models_config()
                    cfg = providers._PROVIDER_MAP.get("volcengine-plan") or {}
                    return cfg.get("apiKey") == "VOLCANO_ENGINE_API_KEY"

                with ThreadPoolExecutor(max_workers=4) as pool:
                    first = pool.submit(worker)
                    self.assertTrue(first_provider_loaded.wait(timeout=1))
                    others = [pool.submit(worker) for _ in range(3)]
                    allow_finish.set()
                    results = [first.result(timeout=2)] + [f.result(timeout=2) for f in others]
            self.assertEqual(results, [True, True, True, True])
        finally:
            allow_finish.set()
            providers._PROVIDER_MAP = old_map


if __name__ == "__main__":
    unittest.main()
