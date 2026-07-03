#!/usr/bin/env python3
"""
LLM Scorer - Route B 核心
==========================
使用 openclaw agent --local 调用 LLM 给候选股票打分
"""

import os
import sys
import json
import re
import time
import hashlib
import threading
import subprocess
import logging
from datetime import date, timedelta
import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as ConcurrentlyTimeoutError
from typing import Dict, List, Any, Optional

BASE_DIR = Path(__file__).parent
from domestic_network import domestic_subprocess_env, retry_call  # noqa: E402

# 加载 .env 文件注入环境变量（解决 isolated session 不继承 env 的问题）
_ENV_FILE = BASE_DIR / ".env"
if _ENV_FILE.exists():
    with open(_ENV_FILE) as f:
        for line in f:
            line = line.strip()
            if line and "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k, v)
os.environ.setdefault("MINIMAX_ALLOW_MX_DIRECT_KEY", "1")

logger = logging.getLogger("llm_scorer")


class LLMScorer:
    """用 openclaw agent --local 调用 LLM 给候选股票打分"""

    def __init__(self, model: Optional[str] = None, timeout: int = 120, api_key: Optional[str] = None, output_dir: Optional[Path] = None):
        self.model = model or "volcengine-plan/ark-code-latest"
        self.timeout = timeout
        self.api_key = api_key or os.environ.get("MX_DIRECT_KEY", "") or os.environ.get("MX_APIKEY", "")
        self.output_dir = output_dir
        # 火山引擎 coding plan 主 + GPT-5.5 / MiniMax M3 兜底（_call_api_direct 内部处理）
        self._use_direct_api = True
        self._check_openclaw()

    def _check_openclaw(self):
        """检查 openclaw 是否可用"""
        try:
            r = subprocess.run(
                ["openclaw", "--version"],
                capture_output=True, text=True, timeout=10
            )
            if r.returncode != 0:
                raise RuntimeError("openclaw --version failed")
            logger.info(f"openclaw 可用: {r.stdout.strip()}")
        except Exception as e:
            raise RuntimeError(f"openclaw 不可用: {e}")

    def _call_llm(self, prompt: str) -> str:
        """通过 openclaw agent --local 调用 LLM（带进程树强制终止）"""
        import uuid, signal
        session_id = f"workflow-{uuid.uuid4().hex[:8]}"
        cmd = [
            "openclaw", "agent",
            "--local",
            "--session-id", session_id,
            "--message", prompt,
            "--timeout", str(self.timeout),
        ]
        proc = None
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env={**os.environ},
                preexec_fn=os.setsid if hasattr(os, 'setsid') else None,
            )
            try:
                stdout, stderr = proc.communicate(timeout=self.timeout + 10)
            except subprocess.TimeoutExpired:
                # 强制杀死整个进程树
                try:
                    if hasattr(os, 'killpg'):
                        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    else:
                        proc.terminate()
                    proc.wait(timeout=5)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                logger.error("LLM 调用超时，已强制终止")
                return ""

            if proc.returncode != 0:
                logger.error(f"openclaw agent failed (code {proc.returncode}): {stderr[:200]}")
                return ""
            
            # 过滤掉 openclaw 日志行，只保留 JSON 响应
            lines = stdout.splitlines()
            json_lines = [l for l in lines if l.startswith("{") or l.startswith("  ") or l.startswith('"') or l.startswith("[")]
            # 找到 JSON 块的起始和结束
            json_output = stdout.strip()
            # 提取第一个 { 到最后一个 } 之间的内容
            start = json_output.find("{")
            end = json_output.rfind("}") + 1
            if start >= 0 and end > start:
                json_output = json_output[start:end]
            else:
                json_output = stdout.strip()
            
            logger.info(f"LLM 响应长度: {len(json_output)} chars")
            return json_output
        except Exception as e:
            logger.error(f"LLM 调用异常: {e}")
            if proc:
                try:
                    proc.kill()
                except Exception:
                    pass
            return ""

    def _call_api_direct(self, prompt: str) -> str:
        """火山引擎 coding plan（深度思考）→ GPT-5.5 → MiniMax M3 兜底"""
        import requests

        if not isinstance(prompt, str):
            logger.error(f"prompt 类型异常: {type(prompt)}")
            return ""

        try:
            from stock_selection_debate.providers import call_llm_with_fallback
            used_model = [None]
            text = call_llm_with_fallback(
                prompt=prompt,
                model=self.model,
                fallback_model="openai/gpt-5.5",
                secondary_fallback_model="minimax-portal/MiniMax-M3",
                timeout=self.timeout,
                retries=3,
                max_tokens=12000,
                thinking_budget=16000,
                fallback_thinking_budget=8000,
                actual_model_out=used_model,
            )
            if text:
                import json as _json
                decoder = _json.JSONDecoder()
                try:
                    parsed, _ = decoder.raw_decode(text.strip())
                    logger.info(f"{used_model[0] or self.model} 响应(raw_decode): {len(text)} chars")
                    return _json.dumps(parsed, ensure_ascii=False)
                except Exception:
                    if text.strip().startswith("{"):
                        return text.strip()
                    start = text.find("{")
                    end = text.rfind("}") + 1
                    if start >= 0 and end > start:
                        logger.info(f"{used_model[0] or self.model} 响应(find): {end - start} chars")
                        return text[start:end]
        except Exception as e:
            logger.error(f"GPT-5.5/MiniMax 统一调用失败: {e}")

        # 兼容显式指定旧火山模型的手工调试；默认工作流不会进入这里。
        volc_key = os.environ.get("VOLCAN_API_KEY", "")
        if volc_key and ("volcengine" in self.model or "ark-code" in self.model):
            for attempt in range(2):
                try:
                    r = requests.post(
                        "https://ark.cn-beijing.volces.com/api/coding/v3/chat/completions",
                        headers={"Authorization": f"Bearer {volc_key}", "Content-Type": "application/json"},
                        json={
                            "model": "ark-code-latest",
                            "messages": [{"role": "user", "content": prompt}],
                            "max_tokens": 12000,
                            "temperature": 0.3,
                            
                        },
                        timeout=self.timeout,
                    )
                    if r.status_code == 200:
                        data = r.json()
                        msg = data.get("choices", [{}])[0].get("message", {})
                        content = msg.get("content", "") or ""
                        think = msg.get("reasoning_content", "") or ""

                        import json as _json
                        decoder = _json.JSONDecoder()

                        # 优先从 content 用 raw_decode 提取（自动找到完整 JSON）
                        try:
                            parsed, end = decoder.raw_decode(content)
                            logger.info(f"火山引擎 响应(raw_decode): {len(content)} chars parsed")
                            return _json.dumps(parsed, ensure_ascii=False)
                        except Exception:
                            pass

                        # 备用：content 已是纯 JSON 对象
                        if content.strip().startswith("{"):
                            return content.strip()

                        # Fallback: 从 reasoning_content 提取
                        if think:
                            try:
                                parsed, end = decoder.raw_decode(think)
                                logger.info(f"火山引擎 reasoning_content 响应: {len(think)} chars")
                                return _json.dumps(parsed, ensure_ascii=False)
                            except Exception:
                                pass
                    elif r.status_code == 429:
                        import time; time.sleep(5); continue
                    if r.status_code == 400 and attempt == 0:
                        try:
                            r2 = requests.post(
                                "https://ark.cn-beijing.volces.com/api/coding/v3/chat/completions",
                                headers={"Authorization": f"Bearer {volc_key}", "Content-Type": "application/json"},
                                json={
                                    "model": "ark-code-latest",
                                    "messages": [{"role": "user", "content": prompt[:32000]}],
                                    "max_tokens": 12000,
                                    "temperature": 0.3,
                                    
                                },
                                timeout=self.timeout,
                            )
                            if r2.status_code == 200:
                                data = r2.json()
                                content = data.get("choices", [{}])[0].get("message", {}).get("content", "") or ""
                                import json as _json
                                decoder = _json.JSONDecoder()
                                try:
                                    parsed, end = decoder.raw_decode(content)
                                    logger.info(f"火山引擎 截断重试(raw_decode): {len(content)} chars")
                                    return _json.dumps(parsed, ensure_ascii=False)
                                except Exception:
                                    if content.strip().startswith("{"):
                                        return content.strip()
                                    start = content.find("{")
                                    end = content.rfind("}") + 1
                                    if start >= 0 and end > start:
                                        logger.info(f"火山引擎 截断重试(find): {end - start} chars")
                                        return content[start:end]
                        except Exception:
                            pass
                except Exception as e:
                    if attempt == 0:
                        import time; time.sleep(3); continue

        # 备用模型：MiniMax M3
        mx_key = os.environ.get("MX_DIRECT_KEY") or os.environ.get("MINIMAX_API_KEY", "")
        if mx_key:
            try:
                r = requests.post(
                    "https://api.minimaxi.com/anthropic/v1/messages",
                    headers={"Authorization": f"Bearer {mx_key}", "Content-Type": "application/json",
                             "anthropic-version": "2023-06-01"},
                    json={
                        "model": "MiniMax-M3",
                        "max_tokens": 12000,
                        
                        "messages": [{"role": "user", "content": prompt}],
                    },
                    timeout=self.timeout,
                )
                if r.status_code == 200:
                    data = r.json()
                    import json as _json
                    decoder = _json.JSONDecoder()
                    for block in data.get("content", []):
                        if block.get("type") == "text":
                            text = block["text"]
                            try:
                                parsed, end = decoder.raw_decode(text)
                                logger.info(f"MiniMax 响应(raw_decode): {len(text)} chars")
                                return _json.dumps(parsed, ensure_ascii=False)
                            except Exception:
                                if text.strip().startswith("{"):
                                    return text.strip()
                                start = text.find("{")
                                end = text.rfind("}") + 1
                                if start >= 0 and end > start:
                                    logger.info(f"MiniMax 响应(find): {end - start} chars")
                                    return text[start:end]
            except Exception as e:
                logger.error(f"MiniMax 异常: {e}")

        return ""

    def _tavily_supplement(self, candidates: List[Dict]) -> Dict[str, Any]:
        """
        用 Tavily 搜索补充候选股的最新市场信息
        返回格式：{tavily: {...}} 作为额外分析师输出
        """
        try:
            tavily_key = os.environ.get("TAVILY_API_KEY", "")
            if not tavily_key:
                # 尝试从 openclaw.json env 读取
                try:
                    import json as _json
                    with open(Path.home() / ".openclaw/openclaw.json") as f:
                        _cfg = _json.load(f)
                    _env = _cfg.get("env", {})
                    tavily_key = _env.get("TAVILY_API_KEY", "")
                except Exception:
                    tavily_key = ""
            if not tavily_key:
                return {}

            import subprocess
            results = []
            # 只搜索 Top 5 候选股
            for c in candidates[:5]:
                stock = c.get("stock", "")
                name = c.get("name", "")
                if not stock:
                    continue
                query = f"{name} {stock} A股 今日"
                r = subprocess.run(
                    ["env", f"TAVILY_API_KEY={tavily_key}",
                     "node",
                     str(Path(__file__).parent.parent / "skills/claw-tavily-search-pro/scripts/search.mjs"),
                     query, "--topic", "news", "-n", "3"],
                    capture_output=True, text=True, timeout=30
                )
                if r.returncode == 0 and r.stdout.strip():
                    results.append(f"【{stock} {name}】{r.stdout[:300]}")
            if results:
                return {
                    "name": "Tavily补充分析师",
                    "findings": "\n".join(results)[:500],
                    "raw": {"results": results},
                }
        except Exception as e:
            logger.warning(f"Tavily补充失败: {e}")
        return {}

    def _mx_search_supplement(self, candidates: List[Dict]) -> Dict[str, Any]:
        """
        用 mx_search 并发补充所有候选股的东方财富资讯
        - Semaphore(5) 控制并发数
        - 每批5只股，批次间隔1秒
        - 返回格式：{mx_search: {...}} 作为额外分析师输出
        """
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent / "skills" / "mx-search"))
            from mx_search import MXSearch
            import concurrent.futures

            api_key = os.environ.get("MX_APIKEY") or os.environ.get("MINIMAX_API_KEY") or ""
            if not api_key:
                logger.warning("mx_search: 未找到 MX_APIKEY，跳过")
                return {}

            tool = MXSearch(api_key=api_key)
            sem = threading.Semaphore(5)
            BATCH_SIZE = 5

            def search_one(stock: str, name: str) -> tuple:
                query = f"{name} {stock} A股 最新消息"
                try:
                    with sem:
                        r = tool.search(query)
                    if not r or not r.get('success'):
                        return stock, name, []
                    results = (
                        r.get('data', {})
                        .get('data', {})
                        .get('llmSearchResponse', {})
                        .get('data', []) or []
                    )
                    return stock, name, results
                except Exception as e:
                    logger.warning(f"mx_search {stock} 失败: {e}")
                    return stock, name, []

            all_results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
                # 构建任务列表：所有候选股
                tasks = [(c.get('stock', ''), c.get('name', '')) for c in candidates]
                futures = {ex.submit(search_one, s, n): (s, n) for s, n in tasks if s}

                for i in range(0, len(tasks), BATCH_SIZE):
                    # 每批只等待已提交的这批
                    batch_keys = list(futures.keys())[i:i+BATCH_SIZE]
                    for f in concurrent.futures.as_completed(batch_keys):
                        stock, name, results = f.result()
                        for news in results[:3]:
                            title = (news.get('title') or '')[:200]
                            if title:
                                all_results.append(f"【{stock} {name}】{title}")
                    # 批次之间间隔1秒
                    if i + BATCH_SIZE < len(tasks):
                        time.sleep(1)

            if all_results:
                return {
                    "name": "mx_search补充分析师",
                    "findings": "\n".join(all_results)[:500],
                    "raw": {"results": all_results},
                }
        except Exception as e:
            logger.warning(f"mx_search补充失败: {e}")
        return {}

    def _merge_news_data(self, tavily_data: Dict, mx_search_data: Dict) -> Dict[str, Any]:
        """
        合并 Tavily 和 mx_search 的数据，按内容去重，标签合并
        
        去重逻辑：
        - 同股票 + 同内容 → 去重（保留1条）
        - 同股票 + 不同内容 → 都保留，标签合并
        - 不同股票 → 各自保留
        
        返回格式：{merged_news: {...}}
        """
        if not tavily_data and not mx_search_data:
            return {}
        
        # 按股票代码分组存储
        # 结构: {stock_code: {name: str, messages: [{content: str, tags: [str], source: str}]}}
        stock_messages: Dict[str, Dict] = {}
        
        def add_message(source: str, text: str, tags: List[str]):
            """解析消息并添加到分组"""
            import re
            # 提取股票代码
            code_match = re.search(r'\[?(\d{6})\]?', text)
            if not code_match:
                return
            stock = code_match.group(1)
            
            # 提取股票名称（如果存在）
            name_match = re.search(r'【[^】]*?([\u4e00-\u9fa5]{2,8})】', text)
            name = name_match.group(1) if name_match else stock
            
            # 提取内容（去掉股票代码和名称后的部分）
            content = re.sub(r'【[^】]*】', '', text).strip()
            if len(content) < 5:  # 内容太短，跳过
                return
            
            if stock not in stock_messages:
                stock_messages[stock] = {"name": name, "messages": []}
            
            # 检查是否已存在相同内容
            existing_contents = [m["content"] for m in stock_messages[stock]["messages"]]
            if content not in existing_contents:
                stock_messages[stock]["messages"].append({
                    "content": content,
                    "tags": tags,
                    "source": source
                })
            else:
                # 相同内容存在，合并标签
                idx = existing_contents.index(content)
                for tag in tags:
                    if tag not in stock_messages[stock]["messages"][idx]["tags"]:
                        stock_messages[stock]["messages"][idx]["tags"].append(tag)
        
        # 处理 Tavily 数据
        if tavily_data:
            tavily_results = tavily_data.get("raw", {}).get("results", [])
            for item in tavily_results:
                # 分析情绪
                if "利多" in item or "利好" in item or "增长" in item or "突破" in item:
                    tags = ["利好", "Tavily"]
                elif "利空" in item or "下跌" in item or "风险" in item:
                    tags = ["利空", "Tavily"]
                else:
                    tags = ["中性", "Tavily"]
                add_message("Tavily", item, tags)
        
        # 处理 mx_search 数据
        if mx_search_data:
            mx_results = mx_search_data.get("raw", {}).get("results", [])
            for item in mx_results:
                # 分析情绪
                if "利多" in item or "利好" in item or "增长" in item or "突破" in item or "涨停" in item:
                    tags = ["利好", "mx_search"]
                elif "利空" in item or "下跌" in item or "风险" in item:
                    tags = ["利空", "mx_search"]
                else:
                    tags = ["中性", "mx_search"]
                add_message("mx_search", item, tags)
        
        # 转换为输出格式
        merged_results = []
        for stock, data in stock_messages.items():
            messages_parts = []
            for m in data["messages"]:
                tags_str = "+".join(m["tags"])
                messages_parts.append(f"{m['content']}({tags_str})")
            messages_text = " | ".join(messages_parts)
            merged_results.append(f"【{stock} {data['name']}】{messages_text}")
        
        if merged_results:
            return {
                "name": "合并新闻分析师",
                "findings": "\n".join(merged_results)[:800],
                "raw": {"results": merged_results, "stock_messages": stock_messages},
            }
        return {}

    def _get_progress_file(self) -> Path:
        # 返回固定文件名，不包含日期
        return self.output_dir / "scored_progress.json"

    def score_candidates(
        self,
        analyst_outputs: List[Dict],
        candidates: List[Dict],
    ) -> List[Dict]:
        """
        核心打分函数 - 分批处理避免LLM超时，支持中断续打
        """
        if not candidates:
            logger.warning("无候选股票，跳过LLM打分")
            return [], []

        # ── 检查缓存文件是否为今天创建，决定是续打还是重写 ──
        from datetime import date, datetime
        progress_file = self._get_progress_file()
        existing_scored: Dict[str, Dict] = {}
        
        if progress_file.exists():
            # 检查文件修改时间
            mtime_timestamp = progress_file.stat().st_mtime
            cache_date = datetime.fromtimestamp(mtime_timestamp).date()
            today = date.today()
            
            if cache_date == today:
                # 今天创建的缓存，增量续打
                try:
                    data = json.loads(progress_file.read_text(encoding="utf-8"))
                    for s in data.get("scored", []):
                        existing_scored[s["stock"]] = s
                    logger.info(f"缓存为今天创建，增量续打：已加载 {len(existing_scored)} 只已打分股票")
                except Exception as e:
                    logger.warning(f"读取今天缓存失败: {e}")
            else:
                # 昨天或更早的缓存，全量重新开始（跳过旧缓存）
                logger.info(f"缓存过期（创建于 {cache_date}），将重新开始全量打分")
                # 删除过期缓存文件
                progress_file.unlink(missing_ok=True)
                logger.info("已删除过期缓存文件")
        else:
            logger.info("无缓存文件，全量开始打分")

        # 过滤掉已打分的候选股（只跳过LLM成功的，Route A的允许重打）
        def _was_llm_scored(stock: str) -> bool:
            prev = existing_scored.get(stock, {})
            return prev.get("scoring_method") == "llm_one_by_one"

        new_candidates = [c for c in candidates if not _was_llm_scored(c.get("stock", ""))]
        already_llm_count = len(candidates) - len(new_candidates)
        if already_llm_count > 0:
            logger.info(f"增量续打：跳过 {already_llm_count} 只已LLM打分股票，剩余 {len(new_candidates)} 只待打")

        # ── 新闻/舆情数据补充（多源去重）──
        # 1. Tavily 补充检索
        tavily_data = self._tavily_supplement(candidates)
        tavily_count = len(tavily_data.get("raw", {}).get("results", [])) if tavily_data else 0
        
        # 2. mx_search 补充检索（优先级高，本土视角）
        mx_search_data = self._mx_search_supplement(candidates)
        mx_count = len(mx_search_data.get("raw", {}).get("results", [])) if mx_search_data else 0
        
        # 3. 合并去重（按内容去重，标签合并）
        merged_news = self._merge_news_data(tavily_data, mx_search_data)
        if merged_news:
            analyst_outputs = analyst_outputs + [merged_news]
            merged_count = len(merged_news.get("raw", {}).get("results", []))
            logger.info(f"新闻补充: Tavily {tavily_count} 条 + mx_search {mx_count} 条 → 合并去重后 {merged_count} 条")

        # ── 统一缓存机制：技术数据预取 ──
        tech_data_cache: Dict[str, Dict] = {}
        stock_codes = [c.get("stock", "") for c in new_candidates if c.get("stock")]
        logger.info(f"技术数据预取开始: {len(new_candidates)} 只股票")
        
        # 检查技术数据缓存文件是否为今天创建
        tech_cache_file = BASE_DIR / "output" / "fundamental_cache" / "all_stocks_tech.json"
        
        # 如果缓存是今天创建的，检查数据完整性和有效性
        if _is_today_cache(tech_cache_file):
            logger.info("技术数据缓存为今天创建，检查数据完整性和有效性")
            cached_tech_data = _read_tech_cache(tech_cache_file)
            
            if cached_tech_data:
                # 检查哪些股票在缓存中有有效数据
                valid_cached = []
                missing_cached = []
                
                for stock in stock_codes:
                    if stock in cached_tech_data:
                        data = cached_tech_data[stock]
                        # 使用统一的函数检查数据有效性
                        if _is_valid_tech_data(data):
                            # 有效缓存数据
                            tech_data_cache[stock] = data
                            valid_cached.append(stock)
                        else:
                            # 缓存数据无效
                            missing_cached.append(stock)
                    else:
                        # 缓存中没有该股票
                        missing_cached.append(stock)
                
                logger.info(f"缓存检查完成: 有效{len(valid_cached)}只, 缺失{len(missing_cached)}只")
                
                # 如果所有股票都有有效缓存数据，跳过API调用
                if len(valid_cached) == len(stock_codes):
                    logger.info("所有股票都有有效缓存，跳过API调用")
                    for c in new_candidates:
                        stock = c.get("stock", "")
                        c["tech_data"] = tech_data_cache[stock]
                    
                    # 设置标记，表示已经处理完技术数据
                    tech_data_prefetched = True
                else:
                    # 部分数据缺失，需要API获取（断点续传）
                    logger.info(f"部分数据缺失，将继续API获取: {missing_cached[:5]}{'...' if len(missing_cached) > 5 else ''}")
                    tech_data_prefetched = False
                    # 清空tech_data_cache，只保留有效缓存的数据
                    tech_data_cache = {stock: cached_tech_data[stock] for stock in valid_cached}
            else:
                # 缓存文件读取失败
                logger.warning("缓存文件读取失败，将继续API获取")
                tech_data_prefetched = False
        else:
            if tech_cache_file.exists():
                from datetime import datetime
                mtime_timestamp = tech_cache_file.stat().st_mtime
                cache_date = datetime.fromtimestamp(mtime_timestamp).date()
                logger.info(f"技术数据缓存过期（创建于 {cache_date}），将重新获取")
            else:
                logger.info("无技术数据缓存文件，将重新获取")
            tech_data_prefetched = False
        
        # 新的获取顺序：QMT HTTP → akshare → mx_data
        # QMT HTTP 本地快速，失败则 akshare（完整历史），再失败则 mx_data
        mx_fetched = set()
        akshare_fetched = set()
        qmt_fetched = set()

        # 如果缓存无效或过期，进行API获取
        if not tech_data_prefetched:
            logger.info(f"技术数据获取开始: {len(stock_codes)} 只股票")
            
            # Step 1: QMT HTTP（本地快速，并行获取）
            logger.info("Step 1: QMT HTTP 获取")
            for stock in stock_codes:
                try:
                    td = _fetch_tech_data_from_kline(stock)
                    if td:
                        tech_data_cache[stock] = td
                        qmt_fetched.add(stock)
                except Exception:
                    pass
            logger.info(f"  QMT HTTP 成功: {len(qmt_fetched)} 只")
            
            # Step 2: akshare 兜底（QMT失败的股票）
            akshare_fetched = set()
            remaining = [s for s in stock_codes if s not in tech_data_cache]
            logger.info(f"Step 2: akshare 兜底 {len(remaining)} 只")
            for stock in remaining:
                td = None
                for attempt in range(2):
                    try:
                        td = _fetch_tech_data_inline(stock)
                        if td:
                            tech_data_cache[stock] = td
                            akshare_fetched.add(stock)
                            break
                    except Exception:
                        if attempt < 1:
                            time.sleep(3)
                if not td:
                    tech_data_cache[stock] = {}
                time.sleep(2)
            logger.info(f"  akshare 成功: {len(akshare_fetched)} 只")
            
            # Step 3: mx_data 兜底（QMT和akshare都失败的股票）
            mx_failed = [s for s in stock_codes if s not in tech_data_cache or not tech_data_cache[s]]
            if mx_failed:
                logger.info(f"Step 3: mx_data 兜底 {len(mx_failed)} 只")
                try:
                    mx_results = _fetch_tech_data_batch_via_mx(mx_failed)
                    for stock, data in mx_results.items():
                        if data:
                            tech_data_cache[stock] = data
                            mx_fetched.add(stock)
                except Exception as e:
                    logger.warning(f"mx_data 批量获取失败: {e}")
            
            logger.info(f"API获取完成: {len(tech_data_cache)} 只股票已缓存")
            logger.info(f"  - QMT HTTP: {len(qmt_fetched)} 只")
            logger.info(f"  - akshare: {len(akshare_fetched)} 只")
            logger.info(f"  - mx_data: {len(mx_fetched)} 只")
        else:
            logger.info("已从今天缓存获取技术数据，跳过API调用")
            
            # 确保所有股票都有tech_data标记
            for c in new_candidates:
                stock = c.get("stock", "")
                if stock in tech_data_cache:
                    c["tech_data"] = tech_data_cache[stock]
                else:
                    # 缓存中没有的股票（新股票），标记为空数据
                    c["tech_data"] = {}
                    tech_data_cache[stock] = {}
            
            logger.info(f"从缓存获取完成: {len(tech_data_cache)} 只股票")
        

        
        logger.info(f"预取完成: {len(tech_data_cache)} 只股票已缓存")
        logger.info(f"  - QMT HTTP: {len(qmt_fetched)} 只")
        logger.info(f"  - akshare兜底: {len(akshare_fetched)} 只")
        logger.info(f"  - mx_data: {len(mx_fetched)} 只")
        
        # ── 额外补充：昨日收盘价、PE（安全底线评分用） + 板块强弱 ──
        # 优先级：QMT HTTP → akshare → 腾讯财经 fallback
        pe_data_cache: Dict[str, Dict] = {}
        sector_info_cache: Dict[str, str] = {}
        try:
            from qmt_client import fetch_prev_close_and_pe, fetch_sector_strength, is_qmt_available
            # 获取板块强弱（一次性，全局信息）
            sector_data = fetch_sector_strength()
            if sector_data:
                hot_s = sector_data.get("hot_sectors", [])
                cold_s = sector_data.get("cold_sectors", [])
                logger.info(f"板块强弱: 强势{len(hot_s)}个, 弱势{len(cold_s)}个")
            else:
                hot_s = []
                cold_s = []
            
            # 并发获取所有候选股的昨收+PE
            def _get_pe_data(stock: str) -> tuple:
                result = fetch_prev_close_and_pe(stock)
                return stock, result
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
                futures = {ex.submit(_get_pe_data, c.get("stock", "")): c 
                           for c in new_candidates if c.get("stock")}
                for f in concurrent.futures.as_completed(futures):
                    stock, result = f.result()
                    if result:
                        pe_data_cache[stock] = result
            logger.info(f"昨收+PE获取完成: {len(pe_data_cache)} 只")
        except Exception as e:
            logger.warning(f"PE/板块信息获取失败: {e}")
        
        # 标记已经获取到tech_data的候选股
        for c in new_candidates:
            stock = c.get("stock", "")
            if stock in tech_data_cache:
                c["tech_data"] = tech_data_cache[stock]
            if stock in pe_data_cache:
                c["prev_close"] = pe_data_cache[stock].get("prev_close")
                c["pe"] = pe_data_cache[stock].get("pe")
                c["yesterday_chg"] = pe_data_cache[stock].get("yesterday_chg")
        
        # 分批处理：每批3只（减少API调用次数），失败重试2次
        BATCH_SIZE = 3
        MAX_RETRIES = 2
        all_scores = []
        route_a_fallback: List[Dict] = []  # LLM失败的暂存，稍后由Route A打

        for i in range(0, len(new_candidates), BATCH_SIZE):
            batch = new_candidates[i:i + BATCH_SIZE]
            # 技术数据：从缓存读取，避免重复API调用
            for c in batch:
                stock = c.get("stock", "")
                if not c.get("tech_data") and tech_data_cache.get(stock):
                    c["tech_data"] = tech_data_cache[stock]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(new_candidates) + BATCH_SIZE - 1) // BATCH_SIZE
            logger.info(f"LLM 打分批次 {batch_num}/{total_batches}: {len(batch)} 只 ({[c.get('stock') for c in batch]})")
            prompt = self._build_prompt(analyst_outputs, batch)

            # 重试机制：优先直连（快），失败后切 Gateway fallback，再失败则重试同一路径
            response = None
            use_llm_fallback = False
            for attempt in range(MAX_RETRIES + 1):
                if attempt > 0:
                    logger.warning(f"批次 {batch_num} 第 {attempt} 次重试...")
                    time.sleep(30)

                if use_llm_fallback or not self._use_direct_api:
                    # 路径2：走 Gateway 多模型 fallback
                    response = self._call_llm(prompt)
                else:
                    # 路径1：直连 MiniMax
                    response = self._call_api_direct(prompt)
                    if not response and self._use_direct_api:
                        # 直连失败，切换 Gateway fallback
                        logger.warning(f"批次 {batch_num} 直连失败，切换 Gateway fallback")
                        use_llm_fallback = True
                        response = self._call_llm(prompt)

                if response:
                    break

            if response:
                batch_scores = self._parse_response(response, batch)
                for s in batch_scores:
                    s["scoring_method"] = "llm_one_by_one"
                all_scores.extend(batch_scores)
                # 立即追加写入进度文件（支持中断续打）
                all_known = dict(existing_scored)
                for s in all_scores:
                    all_known[s["stock"]] = s
                progress_tmp = progress_file.with_name(progress_file.name + ".tmp")
                progress_tmp.write_text(
                    json.dumps({"date": str(date.today()), "scored": list(all_known.values())}, ensure_ascii=False),
                    encoding="utf-8"
                )
                progress_tmp.replace(progress_file)
            else:
                logger.warning(f"批次 {batch_num} LLM 调用失败 {MAX_RETRIES + 1} 次，暂存至 Route A 补打")
                for c in batch:
                    route_a_fallback.append({**c, "_pending_llm": True})

            # 每批次间隔3秒，快速完成同时避免触发限流
            if i + BATCH_SIZE < len(new_candidates):
                time.sleep(3)

        if route_a_fallback:
            logger.info(f"{len(route_a_fallback)} 只股票 LLM 失败，将交由 Route A 规则打分")
            # 标记为待Route A补打，Route A打完后会替换这些股票的结果
            for c in route_a_fallback:
                all_scores.append({
                    "stock": c["stock"],
                    "name": c["name"],
                    "news_score": 50,
                    "tech_score": 50,
                    "fundamental_score": 50,
                    "sentiment_score": 50,
                    "total_score": 50,
                    "reason": "LLM不可用，等待Route A规则打分",
                    "action": "WATCH",
                    "scoring_method": "llm_pending_route_a",
                    "_route_a_pending": True,
                })

        # 合并历史打分结果，确保所有已完成的股票都在返回列表中
        for stock, s in existing_scored.items():
            if stock not in {x["stock"] for x in all_scores}:
                all_scores.append(s)

        logger.info(f"LLM 分批打分完成: {len(all_scores)} 只 ({len(route_a_fallback)} 只待Route A补打)")
        return all_scores, route_a_fallback

    def _build_prompt(self, analyst_outputs: List[Dict], candidates: List[Dict]) -> str:
        """
        构建打分 prompt

        优化1: Chain-of-Thought - 先分析再打分
        优化2: 动态权重 - 根据市场状态调整
        优化3: 多角度验证 - 技术/基本面/情绪三个维度独立验算
        """
        # 获取市场状态
        market_regime = self._get_market_regime()
        regime_config = self._get_regime_weights(market_regime)

        analyst_lines = []
        for a in analyst_outputs:
            name = a.get("name", "?")
            findings = a.get("findings", "(无内容)")
            raw = a.get("raw", {})
            analyst_lines.append(
                f"【{name}】{findings}\n数据: {json.dumps(raw, ensure_ascii=False)[:300]}"
            )
        analysts_text = "\n\n".join(analyst_lines)

        cand_lines = []
        for i, c in enumerate(candidates, 1):
            fund = c.get("fundamental", "")
            fund_str = f" | 基本面: {fund}" if fund else ""
            tech_data = c.get("tech_data", {})
            tech_str = ""
            vol = None
            ma = None
            rsi_str = "?"
            if tech_data:
                rsi = tech_data.get("rsi")
                ma = tech_data.get("ma_trend")
                vol = tech_data.get("vol_ratio")
                rsi_str = f"{rsi:.0f}" if rsi else "?"
            vol_str = f"{vol:.1f}" if vol else "?"
            tech_str = f" | 技术: RSI={rsi_str} 均线={ma or '?'} 放量={vol_str}x"
            
            # PE信息（昨日收盘价 + 昨日涨幅）
            pe_val = c.get("pe")
            prev_close = c.get("prev_close")
            yesterday_chg = c.get("yesterday_chg")
            pe_str = f" PE={pe_val:.1f}" if pe_val and pe_val > 0 else ""
            chg_str = f" 昨涨={yesterday_chg:+.1f}%" if yesterday_chg is not None else ""
            
            # 板块信息
            sector = c.get("sector", "") or ""
            sector_str = f" | 板块={sector}" if sector else ""
            
            cand_lines.append(
                f"{i}. {c.get('stock', '')} {c.get('name', '')}"
                f" | 理由: {c.get('reason', '无')}"
                f" | 来源: {c.get('source', 'news')}"
                f"{pe_str}{sector_str}{fund_str}{tech_str}"
            )
        candidates_text = "\n".join(cand_lines)

        # 板块强弱信息（全局上下文）
        sector_context = ""
        try:
            from qmt_client import fetch_sector_strength
            sd = fetch_sector_strength()
            if sd:
                hot = sd.get("hot_sectors", [])
                cold = sd.get("cold_sectors", [])
                hot_str = ", ".join([f"{h['name']}(+{h['chg_pct']:.1f}%)" for h in hot[:3]]) if hot else "无"
                cold_str = ", ".join([f"{c['name']}({c['chg_pct']:.1f}%)" for c in cold[:3]]) if cold else "无"
                sector_context = f"""
\n## 今日板块强弱（参考）
强势板块: {hot_str}
弱势板块: {cold_str}
（候选股若属于强势板块+1分，属于弱势板块-1分）"""
        except Exception:
            pass

        prompt = f"""你是A股短线交易选股专家，专注准备启动、资金吸筹、突破新高、首板追击等短线策略。

## 市场状态: {market_regime}
权重参考: 技术动量30% 资金情绪25% 催化剂20% 安全底线15% 短线适配10%

## 今日分析师报告
{analysts_text}

## 候选股票池
{candidates_text}
{sector_context}

────────────────────────────────────────────────────────
打分流程（严格按顺序）
────────────────────────────────────────────────────────

【步骤1】公司类型快速判定
根据池子来源和财务数据判断（用于步骤7调整系数）：
- 准备启动池 → 资金吸筹型（均线粘合后突破，等待资金持续和低吸确认）
- 首板追击池 → 低位启动型
- 突破新高池 → 趋势延续型
- 强势反包池 → 超跌反弹型
- 热点龙头池 → 板块龙头型
- 资金异动池 → 低位启动型（等待放量上攻确认或回踩企稳）
- ST/*ST → 标记高风险

【步骤2】Bull/Bear 快速辩论（对每只股票，在脑中模拟多空双方）
对每只股票先扮演Bull研究员（看多），再扮演Bear研究员（看空），最后给出综合评分：
- Bull视角：这支股票的上涨逻辑是什么？催化剂在哪里？技术面有多强？
- Bear视角：这支股票的风险是什么？估值是否过高？有没有出货嫌疑？
- 综合评分：Bull和Bear谁更有说服力？给出最终分数。

【步骤3】技术动量评分（0-30分）
数据源: xtquant(优先)/XQShare K线计算
- 【均线趋势 0-8分】: MA5>MA10>MA20(完美多头)→8分; MA5>MA20→5分; 交织→2分; 空头→0分
- 【MACD信号 0-8分】: MACD金叉且柱>0→8分; MACD>0无金叉→5分; MACD<0收敛→2分; MACD<0加速→0分
- 【量价配合 0-8分】: 放量>2x且RSI 40-70→8分; 放量1.5-2x→5分; 放量1-1.5x→3分; 缩量→0分
- 【涨停强度 0-6分】: 连板>2→6分; 首板封单大→4分; 涨停开板回封→2分; 无涨停→0分

【步骤3】资金情绪评分（0-25分）
数据源: mx_search/akshare(涨跌停/北向)/分析师报告
- 【北向资金 0-8分】: 近5日持续增持→8分; 单日增持→5分; 无变化→2分; 减持→0分
- 【主力资金 0-7分】: 主力净流入>5000万→7分; 净流入→4分; 平衡→2分; 净流出→0分
- 【板块热度 0-5分】: 板块涨幅Top3且涨停潮→5分; 板块上涨→3分; 板块中性→1分
- 【市场情绪 0-5分】: 涨停>80家且炸板率<25%→5分; 涨停50-80→3分; <50→1分

【步骤4】催化剂评分（0-20分）
数据源: mx_search/Tavily/分析师报告
- 【涨停原因 0-8分】: 重大利好公告涨停→8分; 板块联动涨停→5分; 纯资金驱动→2分
- 【公告利好 0-6分】: 业绩预增/重组/中标→6分; 一般利好→3分; 无利好→0分
- 【新闻热度 0-6分】: 多平台热搜/舆论聚焦→6分; 有报道→3分; 无→0分

【步骤5】安全底线评分（0-15分）—— 防止踩雷
数据源: xtquant PERSHAREINDEX(优先)/mx_data(备用)
- 【估值安全 0-5分】: PE<30→5分; PE30-60→3分; PE>60→1分; 亏损股→0分
- 【财务安全 0-5分】: 负债率<50%→5分; 50-65%→3分; 65-80%→2分; >80%→0分
- 【盈利安全 0-5分】: ROE>10%且连续盈利→5分; ROE>0→3分; ROE<0→0分

【步骤6】短线适配评分（0-10分）
- 【流通市值 0-4分】: 小盘<50亿→4分; 中盘50-200亿→3分; 大盘>200亿→1分
- 【波动弹性 0-3分】: 近期日均振幅>5%→3分; 3-5%→2分; <3%→1分
- 【换手活跃 0-3分】: 换手率>10%→3分; 5-10%→2分; <5%→1分

【步骤7】池子加权调整
根据来源池子给予加成（同一股票多池取最高）：
- 首板追击 → ×1.1（涨停溢价）
- 突破新高/强势反包 → ×1.05（趋势动量）
- 热点龙头 → ×1.0（跟随板块）
- 准备启动/资金异动 → ×1.0（不追涨，重点看止跌确认和资金持续性）

【步骤8】综合决策
总分 = (技术0-30) + (资金0-25) + (催化剂0-20) + (安全0-15) + (适配0-10)
满分100 → ×池子调整系数 → 最终分

决策阈值:
- 总分≥65 → BUY
- 总分40-64 → WATCH
- 总分<40 → AVOID

⚠️ 硬性排除(直接AVOID):
- ST/*ST + 负债率>80% → 高风险，仅观察
- PE>100 且 ROE<0 → 估值泡沫 + 亏损
- 连续3日跌停后首日开板 → 出货嫌疑

## 输出格式
严格JSON:
{{
  "scores": [
    {{
      "stock": "代码",
      "name": "名称",
      "pool": "来源池子",
      "bull_thesis": "看多逻辑30字",
      "bear_thesis": "看空逻辑30字",
      "debate_winner": "bull|bear|tie",
      "momentum_score": 0-30,
      "sentiment_score": 0-25,
      "catalyst_score": 0-20,
      "safety_score": 0-15,
      "adapt_score": 0-10,
      "total_score": 0-100,
      "adjusted_score": 0-110（×池子系数后）,
      "momentum_detail": "均线XX MACD XX",
      "sentiment_detail": "北向XX 主力XX",
      "catalyst_detail": "涨停原因XX",
      "risk_flags": ["ST","高PE"]如有,
      "reason": "理由50字",
      "action": "BUY|WATCH|AVOID"
    }}
  ],
  "summary": "市场判断60字",
  "top_picks": ["代码1","代码2","代码3","代码4","代码5"]
}}
"""
        return prompt

    def _get_market_regime(self) -> str:
        """从 params.json 读取市场状态"""
        try:
            if self.output_dir:
                param_file = self.output_dir.parent / "params.json"
                if param_file.exists():
                    with open(param_file, encoding="utf-8") as f:
                        params = json.load(f)
                    return params.get("market_regime", "震荡")
        except Exception:
            pass
        return "震荡"

    def _get_regime_weights(self, regime: str) -> Dict:
        """根据市场状态返回权重配置"""
        configs = {
            "牛市": {
                "desc": "动量/成长权重高",
                "lynch_w": 30, "safety_w": 20, "tech_w": 50,
                "detail": "- 动量/成长权重高：技术面强势股受追捧，趋势动量最重要\n- 林奇准则：快速增长型优先（规模小、增速20%+）\n- 安全边际：降低要求，趋势确认即可\n- 技术面：均线多头+放量是关键信号"
            },
            "熊市": {
                "desc": "安全边际权重高",
                "lynch_w": 20, "safety_w": 60, "tech_w": 20,
                "detail": "- 安全边际权重高：现金为王，低估值+高ROE是关键\n- 林奇准则：困境反转型+稳定增长型优先\n- 技术面：权重降低，不追求短线动量\n- 重点：股价接近净速动资产、PE<15、负债率<50%"
            },
            "震荡": {
                "desc": "林奇/安全边际平衡",
                "lynch_w": 40, "safety_w": 40, "tech_w": 20,
                "detail": "- 林奇/安全边际平衡：价值与成长并重\n- 林奇准则：稳定增长型+隐蔽资产型优先\n- 技术面：辅助参考，不作为主要依据\n- 重点：PE合理、ROE稳定、行业龙头"
            }
        }
        return configs.get(regime, configs["震荡"])

    def _parse_response(self, response: str, candidates: List[Dict]) -> List[Dict]:
        """解析 LLM JSON 输出"""
        try:
            from stock_selection_debate.providers import extract_json_object
            data = extract_json_object(response, required_keys={"scores"})
            if data:
                scores = data.get("scores", [])
                if scores:
                    logger.info(f"LLM 打分完成: {len(scores)} 只股票")
                    summary = data.get("summary", "")
                    top = data.get("top_picks", [])
                    market_type = data.get("market_type", "")
                    logger.info(f"市场判断: {summary}")
                    logger.info(f"市场类型: {market_type}")
                    logger.info(f"重点关注: {top}")
                    return scores
        except json.JSONDecodeError as e:
            logger.warning(f"JSON解析失败: {e}")

        logger.warning("LLM输出非JSON，使用fallback")
        return self._fallback_scores(candidates)

    def _route_a_score(self, candidate: Dict) -> Dict:
        """
        Route A 规则打分 - 基于已有数据计算综合分数

        候选股票已有数据（来自基本面增强）:
        - sentiment: 利好/利空/中性（来自新闻）
        - roe_annual_latest, roe_quarter_latest: ROE数据
        - 营收增速, 净利润增长率: 成长性指标
        - 负债率: 财务杠杆
        - source: news/tech（来源）

        实时获取的技术数据:
        - RSI, 均线多头, 成交量放量
        """
        score = 50  # 基础分
        reasons = []
        news_score = 50
        fundamental_score = 50
        tech_score = 50

        # ── 1. 情绪分（news_score） ─────────────────────────────
        sentiment = candidate.get("sentiment", "中性")
        if sentiment == "利好":
            news_score += 15
            score += 8
            reasons.append("利好新闻")
        elif sentiment == "利空":
            news_score -= 10
            score -= 8
            reasons.append("利空")
        # 中性：不加分

        # ── 2. 基本面分（fundamental_score） ──────────────────
        roe = candidate.get("roe_annual_latest")
        if roe is not None:
            if roe > 15:
                fundamental_score += 15
                score += 10
                reasons.append(f"高ROE({roe:.1f}%)")
            elif roe > 10:
                fundamental_score += 8
                score += 5
                reasons.append(f"良好ROE({roe:.1f}%)")
            elif roe < 0:
                fundamental_score -= 15
                score -= 12
                reasons.append(f"负ROE({roe:.1f}%)")

        # 营收增速
        rev_growth = candidate.get("营收增速")
        if rev_growth is not None:
            if rev_growth > 20:
                fundamental_score += 10
                score += 6
                reasons.append(f"高营收增速({rev_growth:.1f}%)")
            elif rev_growth > 10:
                fundamental_score += 5
                score += 3
                reasons.append(f"营收增长({rev_growth:.1f}%)")
            elif rev_growth < 0:
                fundamental_score -= 10
                score -= 6
                reasons.append(f"营收下滑({rev_growth:.1f}%)")

        # 净利润增速
        profit_growth = candidate.get("净利润增长率")
        if profit_growth is not None:
            if profit_growth > 20:
                fundamental_score += 10
                score += 6
                reasons.append(f"高净利润增速({profit_growth:.1f}%)")
            elif profit_growth > 10:
                fundamental_score += 5
                score += 3
                reasons.append(f"净利润增长({profit_growth:.1f}%)")
            elif profit_growth < 0:
                fundamental_score -= 10
                score -= 6
                reasons.append(f"净利润下滑({profit_growth:.1f}%)")

        # 负债率（越低越好）
        debt = candidate.get("负债率")
        if debt is not None:
            if debt < 40:
                fundamental_score += 5
                score += 3
                reasons.append(f"低负债({debt:.1f}%)")
            elif debt > 70:
                fundamental_score -= 8
                score -= 5
                reasons.append(f"高负债({debt:.1f}%)")

        # ── 3. 技术分（从网络获取RSI/均线/成交量） ──────────────────
        tech_data = _fetch_tech_data_from_kline(candidate.get("stock", ""))

        if tech_data:
            # RSI：30以下超卖（加分），70以上超买（减分）
            rsi = tech_data.get("rsi")
            if rsi is not None:
                if rsi < 30:
                    tech_score += 15
                    score += 8
                    reasons.append(f"RSI超卖({rsi:.0f})")
                elif rsi < 40:
                    tech_score += 8
                    score += 5
                    reasons.append(f"RSI偏低({rsi:.0f})")
                elif rsi > 70:
                    tech_score -= 10
                    score -= 5
                    reasons.append(f"RSI超买({rsi:.0f})")

            # 均线多头：MA5 > MA20
            ma_trend = tech_data.get("ma_trend")
            if ma_trend == "多头":
                tech_score += 10
                score += 6
                reasons.append("均线多头")
            elif ma_trend == "空头":
                tech_score -= 10
                score -= 6
                reasons.append("均线空头")

            # 成交量放大：今日/5日均量 > 1.5
            vol_ratio = tech_data.get("vol_ratio")
            if vol_ratio is not None:
                if vol_ratio > 2.0:
                    tech_score += 10
                    score += 5
                    reasons.append(f"放量({vol_ratio:.1f}x)")
                elif vol_ratio > 1.5:
                    tech_score += 5
                    score += 3
                    reasons.append(f"温和放量({vol_ratio:.1f}x)")
                elif vol_ratio < 0.5:
                    tech_score -= 8
                    score -= 4
                    reasons.append(f"缩量({vol_ratio:.1f}x)")
        else:
            # 没有技术数据时，用source简单判断
            source = candidate.get("source", "")
            if source == "tech":
                tech_score += 5
                score += 3
                reasons.append("技术面选股")

        # ── 限制范围 ─────────────────────────────────────────
        score = max(0, min(100, score))
        news_score = max(0, min(100, news_score))
        fundamental_score = max(0, min(100, fundamental_score))
        tech_score = max(0, min(100, tech_score))

        # ── 决策 ─────────────────────────────────────────────
        # 参考 params.json 的 scoring_threshold
        threshold = 50
        try:
            param_file = self.output_dir.parent / "params.json"
            if param_file.exists():
                with open(param_file, encoding="utf-8") as f:
                    params = json.load(f)
                threshold = params.get("scoring_threshold", 50)
        except Exception:
            pass

        action = "BUY" if score >= threshold else "WATCH"

        return {
            "stock": candidate.get("stock", ""),
            "name": candidate.get("name", ""),
            "news_score": news_score,
            "tech_score": tech_score,
            "fundamental_score": fundamental_score,
            "sentiment_score": news_score,
            "total_score": score,
            "reason": "; ".join(reasons) if reasons else "Route A规则打分",
            "action": action,
            "scoring_method": "route_a_rules",
            "tech_data": tech_data or {},
        }

    def _fetch_kline_from_qmt_http(stock_code: str, days: int = 60) -> Optional[List[Dict]]:
        """从 QMT HTTP 获取日K线（本地快速源）"""
        try:
            from stock_selection_debate.data_fetcher import _fetch_kline_via_http
            kline = _fetch_kline_via_http(stock_code, days=days)
            return kline if kline and len(kline) >= 5 else None
        except Exception:
            return None


def _fetch_kline_from_qmt_http(stock_code: str, days: int = 60) -> Optional[List[Dict]]:
    """从 QMT HTTP 获取日K线（本地快速源）"""
    try:
        from stock_selection_debate.data_fetcher import _fetch_kline_via_http
        kline = _fetch_kline_via_http(stock_code, days=days)
        return kline if kline and len(kline) >= 5 else None
    except Exception:
        return None



def _compute_tech_from_kline(kline: List[Dict]) -> Dict:
    """从K线数据计算技术指标：RSI、均线、量比"""
    import pandas as pd
    df = pd.DataFrame(kline)
    close = df["close"]
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss
    rsi = (100 - (100 / (1 + rs))).iloc[-1]
    ma5 = close.rolling(5).mean().iloc[-1]
    ma20 = close.rolling(20).mean().iloc[-1]
    ma_trend = "多头" if ma5 > ma20 else "空头"
    vol = df["volume"]
    vol_ma5 = vol.rolling(5).mean().iloc[-1]
    vol_ratio = float(vol.iloc[-1] / vol_ma5) if vol_ma5 > 0 else 1.0
    return {"rsi": float(rsi), "ma_trend": ma_trend, "vol_ratio": vol_ratio}

def _fetch_tech_data_from_kline(stock_code: str) -> Optional[Dict]:
    """
    获取技术数据，优先级：
    1. QMT HTTP（本地） → akshare 补上一交易日
    2. akshare（网络） → mx_data 补上一交易日
    3. mx_data（备用）
    """
    # Step 1: QMT HTTP 拉 K 线
    kline = _fetch_kline_from_qmt_http(stock_code, days=60)
    if kline:
        # 检查上一交易日是否缺失，缺失则用 akshare 补
        today = datetime.date.today()
        prev = today - datetime.timedelta(days=1)
        if prev.weekday() == 5:
            prev -= datetime.timedelta(days=1)
        elif prev.weekday() == 6:
            prev -= datetime.timedelta(days=2)
        prev_str = prev.strftime("%Y-%m-%d")
        if not any(r["date"] == prev_str for r in kline):
            try:
                from stock_selection_debate.data_fetcher import get_kline_via_akshare
                prev_kline = get_kline_via_akshare(stock_code, days=10)
                if prev_kline:
                    for bar in prev_kline:
                        if bar["date"] == prev_str and bar["close"] > 0:
                            kline.append(bar)
                            break
            except Exception:
                pass
        kline = sorted(kline, key=lambda x: x["date"])
        return _compute_tech_from_kline(kline)

    # Step 2: akshare
    try:
        import akshare as ak
        import pandas as pd
        df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", adjust="qfq")
        if df is not None and len(df) >= 30:
            close = df["收盘"]
            delta = close.diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = (-delta.clip(upper=0)).rolling(14).mean()
            rs = gain / loss
            rsi = (100 - (100 / (1 + rs))).iloc[-1]
            ma5 = close.rolling(5).mean().iloc[-1]
            ma20 = close.rolling(20).mean().iloc[-1]
            ma_trend = "多头" if ma5 > ma20 else "空头"
            vol = df["成交量"]
            vol_ma5 = vol.rolling(5).mean().iloc[-1]
            vol_ratio = float(vol.iloc[-1] / vol_ma5) if vol_ma5 > 0 else 1.0
            return {"rsi": float(rsi), "ma_trend": ma_trend, "vol_ratio": vol_ratio}
    except Exception:
        pass

    # Step 3: mx_data
    try:
        result = _fetch_tech_data_batch_via_mx([stock_code])
        if stock_code in result and result[stock_code]:
            return result[stock_code]
    except Exception:
        pass

    return None

    def _fallback_scores(self, candidates: List[Dict]) -> List[Dict]:
        """Route A: 规则打分（优化版）"""
        fallback = []
        for c in candidates:
            scored = self._route_a_score(c)
            scored["reason"] = "LLM不可用，Route A规则打分: " + scored["reason"]
            fallback.append(scored)
        logger.info(f"Route A 规则打分完成: {len(fallback)} 只")
        for s in fallback:
            logger.info(f"  {s['stock']} {s['name']}: {s['total_score']}分 ({s['reason'][:50]})")
        return fallback


def _fetch_tech_data_inline(stock_code: str) -> Optional[Dict]:
    """获取单只股票的技术数据 via akshare（主渠道）"""
    import akshare as ak
    df = retry_call(
        f"akshare 技术数据 {stock_code}",
        lambda: ak.stock_zh_a_hist(symbol=stock_code, period="daily", adjust="qfq"),
        retries=4,
        base_delay=1.5,
        throttle_key="akshare",
        min_interval=1.0,
    )
    if df is None or len(df) < 30:
        return None
    close = df["收盘"]
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss
    rsi = (100 - (100 / (1 + rs))).iloc[-1]
    ma5 = close.rolling(5).mean().iloc[-1]
    ma20 = close.rolling(20).mean().iloc[-1]
    ma_trend = "多头" if ma5 > ma20 else "空头"
    vol = df["成交量"]
    vol_ma5 = vol.rolling(5).mean().iloc[-1]
    vol_ratio = vol.iloc[-1] / vol_ma5 if vol_ma5 > 0 else 1.0
    return {"rsi": float(rsi), "ma_trend": ma_trend, "vol_ratio": float(vol_ratio)}


def _fetch_tech_data_via_mx(stock_code: str) -> Optional[Dict]:
    """获取单只股票的技术数据 via mx_data（备用渠道）"""
    # 调用批量获取，统一处理逻辑
    results = _fetch_tech_data_batch_via_mx([stock_code])
    return results.get(stock_code) if results else None


def _fetch_tech_data_batch_via_mx(stock_codes: List[str]) -> Dict[str, Dict]:
    """批量获取多只股票的技术数据 via mx_data
    
    返回格式: {股票代码: 技术数据字典, ...}
    """
    if not stock_codes:
        return {}
        
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / "skills" / "mx-data"))
        from mx_data import MXData
        api_key = os.environ.get("MX_APIKEY") or os.environ.get("MINIMAX_API_KEY") or ""
        tool = MXData(api_key=api_key)
        
        # 构造批量查询：多个股票代码 + 查询条件
        stock_list = " ".join(stock_codes)
        query = f"{stock_list} 近20个交易日历史行情 包括日期开盘价收盘价成交量"
        
        logger.info(f"批量查询技术数据: {len(stock_codes)}只股票")
        result = tool.query(query)
        tables, _, _, err = tool.parse_result(result)
        
        if err or not tables:
            logger.warning(f"批量技术数据获取失败: {err}")
            return {}
        
        # 解析结果，按股票代码映射
        results = {}
        for table in tables:
            rows = table["rows"]
            if len(rows) < 20:
                continue
                
            # 从表名或第一行数据提取股票代码
            sheet_name = table["sheet_name"]
            import re
            stock_match = re.search(r'\d{6}', sheet_name)
            stock = stock_match.group(0) if stock_match else ""
            
            if not stock and rows:
                # 尝试从第一行数据找到股票代码
                first_row = rows[0]
                for key, value in first_row.items():
                    if isinstance(value, str) and re.match(r'^\d{6}$', value.strip()):
                        stock = value.strip()
                        break
            
            if not stock:
                continue
            
            # 解析 OHLCV 数据
            dates, closes, vols = [], [], []
            def clean_num(s):
                if isinstance(s, (int, float)):
                    return float(s)
                import re
                return float(re.sub(r'[^\d.]', '', str(s)) or 0)
            
            for row in rows[-20:]:
                # 兼容多种列名
                date = row.get("日期") or row.get("date") or row.get("交易日期") or ""
                close = row.get("收盘") or row.get("收盘价") or row.get("close") or row.get("最新价") or "0"
                vol = row.get("成交量") or row.get("vol") or "0"
                dates.append(date)
                closes.append(clean_num(close))
                vols.append(clean_num(vol))
            
            if len(closes) < 20:
                continue
                
            import pandas as pd
            close_s = pd.Series(closes)
            vol_s = pd.Series(vols)
            
            delta = close_s.diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = (-delta.clip(upper=0)).rolling(14).mean()
            rs = gain / loss
            rsi = (100 - (100 / (1 + rs))).iloc[-1]
            ma5 = close_s.rolling(5).mean().iloc[-1]
            ma20 = close_s.rolling(20).mean().iloc[-1]
            ma_trend = "多头" if ma5 > ma20 else "空头"
            vol_ma5 = vol_s.rolling(5).mean().iloc[-1]
            vol_ratio = vol_s.iloc[-1] / vol_ma5 if vol_ma5 > 0 else 1.0
            
            results[stock] = {"rsi": float(rsi), "ma_trend": ma_trend, "vol_ratio": float(vol_ratio)}
        
        logger.info(f"批量技术数据获取成功: {len(results)}只股票")
        return results
        
    except Exception as e:
        logger.warning(f"批量技术数据获取异常: {e}")
        return {}
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / "skills" / "mx-data"))
        from mx_data import MXData
        api_key = os.environ.get("MX_APIKEY") or os.environ.get("MINIMAX_API_KEY") or ""
        tool = MXData(api_key=api_key)
        result = tool.query(f"{stock_code} 近20个交易日历史行情 包括日期开盘价收盘价成交量")
        tables, _, _, err = tool.parse_result(result)
        if err or not tables:
            return None
        rows = tables[0]["rows"]
        if len(rows) < 20:
            return None
        # 解析 OHLCV
        dates, closes, vols = [], [], []
        import re
        def clean_num(s):
            """去掉元/円/HK$/等货币符号后转float"""
            if isinstance(s, (int, float)):
                return float(s)
            return float(re.sub(r'[^\d.]', '', str(s)) or 0)

        for row in rows[-20:]:
            # 兼容多种列名
            date = row.get("日期") or row.get("date") or row.get("交易日期") or ""
            close = row.get("收盘") or row.get("收盘价") or row.get("close") or row.get("最新价") or "0"
            vol = row.get("成交量") or row.get("vol") or "0"
            dates.append(date)
            closes.append(clean_num(close))
            vols.append(clean_num(vol))
        if len(closes) < 20:
            return None
        import pandas as pd
        close_s = pd.Series(closes)
        vol_s = pd.Series(vols)
        delta = close_s.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss
        rsi = (100 - (100 / (1 + rs))).iloc[-1]
        ma5 = close_s.rolling(5).mean().iloc[-1]
        ma20 = close_s.rolling(20).mean().iloc[-1]
        ma_trend = "多头" if ma5 > ma20 else "空头"
        vol_ma5 = vol_s.rolling(5).mean().iloc[-1]
        vol_ratio = vol_s.iloc[-1] / vol_ma5 if vol_ma5 > 0 else 1.0
        return {"rsi": float(rsi), "ma_trend": ma_trend, "vol_ratio": float(vol_ratio)}
    except Exception as e:
        logger.warning(f"mx_data 技术数据获取失败 {stock_code}: {e}")
        return None


def _is_today_cache(cache_file: Path) -> bool:
    """检查缓存文件是否是今天创建的"""
    from datetime import datetime, date
    
    if not cache_file.exists():
        return False
    
    try:
        mtime_timestamp = cache_file.stat().st_mtime
        cache_date = datetime.fromtimestamp(mtime_timestamp).date()
        today = date.today()
        
        return cache_date == today
    except Exception:
        return False


def _read_financial_cache(cache_file: Path, stock_code: str) -> Optional[Dict]:
    """从缓存文件中读取指定股票的数据
    
    Args:
        cache_file: 缓存文件路径
        stock_code: 股票代码，如果为 "__ALL__" 则返回所有数据
    
    Returns:
        指定股票的数据字典，或所有数据的字典，或None
    """
    if not cache_file.exists():
        return None
    
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)
        
        # 如果请求所有数据
        if stock_code == "__ALL__":
            return cache_data.get("data", {})
        
        # 返回指定股票的数据
        if stock_code in cache_data.get("data", {}):
            return cache_data["data"][stock_code]
    except Exception:
        pass
    
    return None


def _read_tech_cache(cache_file: Path) -> Optional[Dict]:
    """从技术数据缓存文件中读取数据"""
    if not cache_file.exists():
        return None
    
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        pass
    
    return None


def _is_valid_financial_data(data: Dict) -> bool:
    """检查财务数据是否有效"""
    if not data:
        return False
    # 关键字段：至少需要ROE数据
    roe = data.get("roe_annual_latest")
    return roe is not None and roe != ""


def _is_valid_tech_data(data: Dict) -> bool:
    """检查技术数据是否有效"""
    if not data:
        return False
    # 关键字段：RSI、MA趋势、成交量比
    rsi = data.get("rsi")
    ma_trend = data.get("ma_trend")
    vol_ratio = data.get("vol_ratio")
    return rsi is not None and ma_trend is not None and vol_ratio is not None



def _fetch_tech_data_via_xqshare(stock_code: str) -> Optional[Dict]:
    """通过 XQShare(xtquant) 获取 K线数据并计算技术指标(RSI/MA/MACD/量比)"""
    import pandas as pd
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        import xqshare
        host = os.environ.get("XQSHARE_HOST", "127.0.0.1")
        port = int(os.environ.get("XQSHARE_PORT", "18812"))
        client = xqshare.connect(host, port, auto_reconnect=True, max_retries=2)
        xtdata = client.xtdata
        stock_with_suffix = _ensure_suffix(stock_code)
        data = xtdata.get_market_data(
            stock_list=[stock_with_suffix], period='1d', count=60,
            field_list=['close', 'volume'], dividend_type='none',
        )
        if 'close' not in data or data['close'].shape[1] < 30:
            return None
        # XQShare 返回 shape=(1, N)，1行N列(天数)，需用 iloc[0,:] 取整行
        close_series = data['close']
        if close_series.ndim == 2 and close_series.shape[0] == 1:
            close = close_series.iloc[0, :].reset_index(drop=True)
        else:
            close = close_series.iloc[:, 0].reset_index(drop=True)
        vol = data.get('volume')
        if vol is not None:
            if vol.ndim == 2 and vol.shape[0] == 1:
                vol = vol.iloc[0, :].reset_index(drop=True)
            else:
                vol = vol.iloc[:, 0].reset_index(drop=True)
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss
        rsi = float((100 - (100 / (1 + rs))).iloc[-1])
        ma5 = float(close.rolling(5).mean().iloc[-1])
        ma10 = float(close.rolling(10).mean().iloc[-1])
        ma20 = float(close.rolling(20).mean().iloc[-1])
        if ma5 > ma10 > ma20:
            ma_trend = "多头"
        elif ma5 < ma10 < ma20:
            ma_trend = "空头"
        else:
            ma_trend = "交织"
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = float((macd_line - signal_line).iloc[-1])
        macd_golden_cross = bool(macd_line.iloc[-1] > signal_line.iloc[-1] and macd_line.iloc[-2] <= signal_line.iloc[-2])
        vol_ma5 = vol.rolling(5).mean().iloc[-1]
        vol_ratio = float(vol.iloc[-1] / vol_ma5) if vol_ma5 > 0 else 1.0
        return {
            "rsi": rsi, "ma_trend": ma_trend,
            "ma5": ma5, "ma10": ma10, "ma20": ma20,
            "vol_ratio": vol_ratio, "macd": round(macd_hist, 4),
            "macd_golden_cross": macd_golden_cross,
        }
    except Exception as e:
        logger.warning(f"XQShare 技术数据获取失败 {stock_code}: {e}")
        return None


def _ensure_suffix(code: str) -> str:
    """转换 6 位股票代码为 xtquant 格式"""
    code = str(code).strip()
    if code.endswith((".SH", ".SZ", ".BJ")):
        return code
    if code.startswith(("920", "8", "4")):
        return code + ".BJ"
    if code.startswith("688"):
        return code + ".SH"
    if code.startswith(("6", "9", "5")):
        return code + ".SH"
    return code + ".SZ"



def _fetch_financial_via_xtquant(stock_code: str) -> Optional[Dict]:
    """获取单只股票财务数据 via QMT HTTP API，不触发网络下载"""
    try:
        import urllib.request, json
        full_code = _ensure_suffix(stock_code)
        url = f"http://127.0.0.1:8080/financial_data?stocks={full_code}&tables=PERSHAREINDEX"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
        if not raw.get("success"):
            return None
        data = raw.get("data", {})
        if not data or full_code not in data:
            return None
        table = data[full_code].get("PERSHAREINDEX")
        if not table:
            return None
        cols = table["columns"]
        rows = table["rows"]
        if not rows:
            return None
        col_idx = {c: i for i, c in enumerate(cols)}
        def get_val(row, col):
            v = row[col_idx[col]] if col in col_idx else None
            if v is None or (isinstance(v, float) and v != v):  # NaN check
                return None
            try:
                return round(float(v), 2)
            except (ValueError, TypeError):
                return None
        def get_latest_row(pattern):
            for row in rows:
                timetag = str(row[col_idx["m_timetag"]]) if "m_timetag" in col_idx else ""
                val = get_val(row, pattern)
                if val is not None:
                    return val, timetag
            return None, ""
        annual_rows = sorted([r for r in rows if str(r[col_idx["m_timetag"]]).endswith("1231")],
                          key=lambda r: str(r[col_idx["m_timetag"]]), reverse=True)
        quarter_rows = sorted([r for r in rows if not str(r[col_idx["m_timetag"]]).endswith("1231")],
                              key=lambda r: str(r[col_idx["m_timetag"]]), reverse=True)
        annual_row = annual_rows[0] if annual_rows else None
        quarter_row = quarter_rows[0] if quarter_rows else None
        roe_annual = get_val(annual_row, "equity_roe") if annual_row else None
        if roe_annual is None:
            return None
        roe_quarter = get_val(quarter_row, "equity_roe") if quarter_row else None
        rev_growth = get_val(quarter_row, "inc_revenue_rate") if quarter_row else None
        if rev_growth is None and annual_row:
            rev_growth = get_val(annual_row, "inc_revenue_rate")
        profit_growth = get_val(quarter_row, "inc_net_profit_rate") if quarter_row else None
        if profit_growth is None and annual_row:
            profit_growth = get_val(annual_row, "inc_net_profit_rate")
        profitable_years = 0
        for row in annual_rows[:3]:
            rv = get_val(row, "equity_roe")
            if rv is not None and rv > 0:
                profitable_years += 1
        pe = None
        pb = None
        try:
            eps = get_val(annual_row, "s_fa_eps_basic")
            bps = get_val(quarter_row, "s_fa_bps") if quarter_row else None
            if eps and eps > 0:
                # 从 HTTP 获取最新收盘价
                price_url = f"http://127.0.0.1:8080/market_data3?stock={full_code}&period=1d&count=1"
                preq = urllib.request.Request(price_url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(preq, timeout=10) as pr:
                    pd_data = json.loads(pr.read().decode("utf-8"))
                close_data = pd_data.get("data", {}).get("close", {})
                dates = sorted(close_data.keys(), reverse=True)
                if dates:
                    inner = close_data[dates[0]]
                    if isinstance(inner, dict):
                        price = inner.get(full_code)
                    else:
                        price = inner
                    if price and float(price) > 0:
                        pe = round(float(price) / eps, 2)
                        if bps and bps > 0:
                            pb = round(float(price) / bps, 2)
        except Exception:
            pass
        return {
            "roe_annual_latest": roe_annual,
            "roe_quarter_latest": roe_quarter,
            "营收增速": rev_growth,
            "净利润增长率": profit_growth,
            "负债率": get_val(annual_row, "gear_ratio") if annual_row else None,
            "连续三年盈利": profitable_years >= 3,
            "pe": pe,
            "pb": pb,
        }
    except Exception as e:
        logger.warning(f"QMT HTTP 财务数据获取失败 {stock_code}: {e}")
        return None


def _fetch_financial_via_mx(stock_code: str) -> Optional[Dict]:
    """获取单只股票财务数据 via mx_data（主渠道），检查缓存是否为今天数据且有效"""
    
    cache_file = BASE_DIR / "output" / "fundamental_cache" / "all_stocks_financial.json"
    
    # 1. 检查缓存文件是否为今天创建
    if _is_today_cache(cache_file):
        cached_data = _read_financial_cache(cache_file, stock_code)
        if cached_data and _is_valid_financial_data(cached_data):
            logger.info(f"  {stock_code} 从今天缓存读取有效财务数据")
            return cached_data
        elif cached_data:
            logger.info(f"  {stock_code} 缓存数据无效，将重新获取")
    else:
        if cache_file.exists():
            from datetime import datetime
            mtime_timestamp = cache_file.stat().st_mtime
            cache_date = datetime.fromtimestamp(mtime_timestamp).date()
            logger.info(f"财务数据缓存过期（创建于 {cache_date}），将重新获取")
    
    # 2. 缓存不存在、过期或无效，尝试调用mx_data API（今天可能会失败）
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / "skills" / "mx-data"))
        from mx_data import MXData
        api_key = os.environ.get("MX_APIKEY") or os.environ.get("MINIMAX_API_KEY") or ""
        tool = MXData(api_key=api_key)
        query = f"{stock_code} 净资产收益率ROE 资产负债率 净利润同比增长率 营业总收入同比增长率"
        result = tool.query(query)
        tables, _, _, err = tool.parse_result(result)
        if err or not tables:
            return None
        rows = tables[0]["rows"]
        if not rows:
            return None
        row = rows[0]
        def get_val(*keys):
            for k in keys:
                for rk, rv in row.items():
                    if k.lower() in str(rk).lower():
                        try:
                            v = str(rv).strip().replace("%", "")
                            return round(float(v), 2) if v else None
                        except (ValueError, TypeError):
                            pass
            return None
        roe = get_val("净资产收益率", "ROE", "roe")
        if roe is None:
            return None
        return {
            "roe_annual_latest": roe,
            "roe_quarter_latest": get_val("净资产收益率(单季度)"),
            "营收增速": get_val("营业总收入同比", "营收同比"),
            "净利润增长率": get_val("净利润同比", "净利润增长"),
            "负债率": get_val("资产负债率"),
        }
    except Exception as e:
        logger.warning(f"mx_data 财务数据获取失败（今日可能已达上限） {stock_code}: {e}")
        return None


def _fetch_financial_via_akshare(stock_code: str) -> Optional[Dict]:
    """获取单只股票财务数据 via akshare（兜底渠道）"""
    try:
        import akshare as ak
        import pandas as pd
        df = retry_call(
            f"akshare 财务数据 {stock_code}",
            lambda: ak.stock_financial_analysis_indicator(symbol=stock_code, start_year="2020"),
            retries=4,
            base_delay=2,
            throttle_key="akshare",
            min_interval=1.0,
        )
        if df is None or df.empty:
            return None
        latest = df.iloc[-1]
        def get_val(cols, default=None):
            for col in cols:
                for c in df.columns:
                    if col in str(c):
                        v = latest.get(c)
                        if v is not None and not pd.isna(v):
                            try:
                                return round(float(str(v).strip().replace("%","")), 2)
                            except (ValueError, TypeError):
                                pass
            return default
        roe = get_val(["净资产收益率", "ROE"], None)
        if roe is None:
            return None
        return {
            "roe_annual_latest": roe,
            "roe_quarter_latest": get_val(["净资产收益率(单季度)"]),
            "营收增速": get_val(["营业总收入同比增长率"]),
            "净利润增长率": get_val(["净利润同比增长率"]),
            "负债率": get_val(["资产负债率"]),
        }
    except Exception as e:
        logger.warning(f"akshare 财务数据获取失败 {stock_code}: {e}")
        return None


def _build_financial_str(d: Dict) -> str:
    """将财务数据格式化为字符串，供LLM打分使用"""
    if not d:
        return "数据有限"
    parts = []
    if d.get("roe_annual_latest") is not None:
        parts.append(f"ROE(年报):{d['roe_annual_latest']}%")
    if d.get("roe_quarter_latest") is not None:
        parts.append(f"ROE(季报):{d['roe_quarter_latest']}%")
    if d.get("营收增速") is not None:
        parts.append(f"营收增速:{d['营收增速']}%")
    if d.get("净利润增长率") is not None:
        parts.append(f"净利润增长:{d['净利润增长率']}%")
    if d.get("负债率") is not None:
        parts.append(f"负债率:{d['负债率']}%")
    return ", ".join(parts) if parts else "数据有限"


# ── 候选股票生成器 ──────────────────────────────────────

XUANGU_SCREEN_CONFIGS = [
    {
        "screen_id": "startup_setup",
        "pool": "准备启动",
        "query": "A股 5日均线、10日均线、20日均线重合 上个交易日K线突破5日均线 近10日主力资金净流入 成交量放量",
        "strategy_type": "capital_absorption_dip",
        "entry_bias": "资金吸筹背离，优先等待止跌企稳或低吸挂单",
        "priority": 30,
    },
    {
        "screen_id": "breakout_high",
        "pool": "突破新高",
        "query": "A股 股价突破20日高点 上个交易日成交量放大超过1.5倍",
        "strategy_type": "momentum_breakout",
        "entry_bias": "趋势确认后可小幅追随，但需防假突破",
        "priority": 20,
    },
    {
        "screen_id": "first_limit",
        "pool": "首板追击",
        "query": "A股 上个交易日首板涨停 非ST 封单超过5000手",
        "strategy_type": "limit_follow",
        "entry_bias": "只在强承接和非一字高开时谨慎跟随",
        "priority": 10,
    },
    {
        "screen_id": "sector_leader",
        "pool": "热点龙头",
        "query": "A股 上个交易日所属板块涨幅排名前3 成分股 非ST",
        "strategy_type": "sector_leader",
        "entry_bias": "跟随强势板块，等待龙头分歧低吸或换手确认",
        "priority": 40,
    },
    {
        "screen_id": "strong_reversal",
        "pool": "强势反包",
        "query": "A股 前两个交易日下跌 上个交易日上涨 涨幅大于5%",
        "strategy_type": "reversal_confirm",
        "entry_bias": "反包后等待回踩不破或继续放量确认",
        "priority": 35,
    },
    {
        "screen_id": "capital_absorption",
        "pool": "资金异动",
        "query": "A股 近三日股价下跌 近三日每日主力资金都是净流入",
        "strategy_type": "startup_dip",
        "entry_bias": "等待放量上攻确认或回踩企稳，偏低吸，不追高",
        "priority": 25,
    },
]


XUANGU_POOL_TOP_N = 20
XUANGU_POOL_SCAN_LIMIT = 300
XUANGU_POOL_RANKING_VERSION = "pool-rank-v1"


def _xuangu_safe_filename(query: str, max_len: int = 80) -> str:
    """Mirror mx-xuangu safe_filename() so source attribution is stable."""
    safe = re.sub(r'[<>:"/\\|?*]', "_", str(query or ""))
    safe = safe.strip().replace(" ", "_")[:max_len]
    return safe or "query"


def _screening_signature(configs: List[Dict] = None) -> str:
    payload = {
        "ranking_version": XUANGU_POOL_RANKING_VERSION,
        "pool_top_n": XUANGU_POOL_TOP_N,
        "configs": [
            {
                "screen_id": c.get("screen_id"),
                "query": c.get("query"),
                "pool": c.get("pool"),
                "strategy_type": c.get("strategy_type"),
            }
            for c in (configs or XUANGU_SCREEN_CONFIGS)
        ],
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _normalize_stock_code(value: Any) -> str:
    raw = str(value or "").strip()
    match = re.search(r"\b(\d{6})\b", raw)
    return match.group(1) if match else ""


def _is_bj_stock_code(value: Any) -> bool:
    stock = _normalize_stock_code(value)
    return stock.startswith(("920", "8", "4")) if stock else False


def _parse_cn_number(value: Any) -> Optional[float]:
    """Parse mx-xuangu values such as 11.80亿, 7532.60万, 5.07%, or 1.2亿|date."""
    if value in (None, "", "--", "-", "None", "nan"):
        return None
    text = str(value).strip().replace(",", "")
    if not text or text in {"--", "-", "None", "nan"}:
        return None
    if "|" in text:
        text = text.split("|", 1)[0]
    multiplier = 1.0
    if "亿" in text:
        multiplier = 100000000.0
    elif "万" in text:
        multiplier = 10000.0
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0)) * multiplier
    except ValueError:
        return None


def _row_text_value(row: Dict, fragments: List[str]) -> str:
    for key, value in (row or {}).items():
        header = str(key or "")
        if all(fragment in header for fragment in fragments):
            return str(value or "").strip()
    return ""


def _row_numeric_value(row: Dict, key_groups: List[List[str]], deny: List[str] = None) -> Optional[float]:
    deny = deny or []
    for fragments in key_groups:
        for key, value in (row or {}).items():
            header = str(key or "")
            if any(block in header for block in deny):
                continue
            if all(fragment in header for fragment in fragments):
                parsed = _parse_cn_number(value)
                if parsed is not None:
                    return parsed
    return None


def _cap_score(value: Optional[float], cap: float, weight: float) -> float:
    if value is None or cap <= 0:
        return 0.0
    return max(0.0, min(1.0, value / cap)) * weight


def _range_score(value: Optional[float], low: float, high: float, weight: float,
                 floor: float = None, ceiling: float = None) -> float:
    if value is None:
        return 0.0
    if low <= value <= high:
        return weight
    if value < low:
        floor = low if floor is None else floor
        if low == floor:
            return 0.0
        return max(0.0, min(1.0, (value - floor) / (low - floor))) * weight
    ceiling = high if ceiling is None else ceiling
    if ceiling == high:
        return 0.0
    return max(0.0, min(1.0, (ceiling - value) / (ceiling - high))) * weight


def _xuangu_exclusion_reason(row: Dict) -> str:
    stock = _normalize_stock_code(row.get("股票代码") or row.get("代码"))
    if not stock:
        return "无有效股票代码"
    if _is_bj_stock_code(stock):
        return "北交所股票"
    name = str(row.get("股票名称") or row.get("名称") or "").strip()
    st_flag = _row_text_value(row, ["ST股票"])
    st_yes = st_flag in {"是", "true", "True", "TRUE", "1"}
    if st_yes or "ST" in name.upper() or "*ST" in name.upper() or "退" in name:
        return "ST/退市风险"
    security_type = _row_text_value(row, ["证券类型"])
    if security_type and "A股" not in security_type:
        return f"非A股:{security_type}"
    price = _row_numeric_value(row, [["最新价"]])
    if price is not None and price <= 0:
        return "价格无效"
    return ""


def _score_xuangu_row(row: Dict, cfg: Dict) -> (float, Dict):
    strategy = cfg.get("strategy_type", "")
    pct = _row_numeric_value(row, [["涨跌幅", "%"], ["CHG<70>"]], deny=["区间", "涨跌额"])
    interval_pct = _row_numeric_value(row, [["区间涨跌幅"]])
    main_flow = _row_numeric_value(row, [["主力净额"], ["主力资金净流入"]])
    north_flow = _row_numeric_value(row, [["沪深股通净买入额"], ["北向资金"]])
    volume_ratio = _row_numeric_value(row, [["量比"]])
    volume_growth = _row_numeric_value(row, [["成交量环比增长率"]])
    turnover = _row_numeric_value(row, [["换手率"]])
    amount = _row_numeric_value(row, [["成交额"]])
    pe = _row_numeric_value(row, [["市盈率"]])
    pb = _row_numeric_value(row, [["市净率"]])
    market_cap = _row_numeric_value(row, [["总市值"]])
    price = _row_numeric_value(row, [["最新价"]])
    high = _row_numeric_value(row, [["最高价", "(元)"]])
    seal = _row_numeric_value(row, [["涨停封单量"]])

    score = 0.0
    detail: Dict[str, Any] = {}

    def add(name: str, value: Optional[float], points: float) -> None:
        nonlocal score
        if abs(points) < 0.001:
            return
        score += points
        detail[name] = round(points, 2)
        if value is not None:
            detail[f"{name}_value"] = round(float(value), 4)

    add("liquidity", amount, _cap_score(amount, 3000000000.0, 8))
    add("turnover_health", turnover, _range_score(turnover, 2, 15, 6, floor=0, ceiling=45))
    if pe is not None:
        pe_points = 0.0
        if 0 < pe <= 80:
            pe_points = (80 - pe) / 80 * 4
        elif pe < 0:
            pe_points = -2
        elif pe > 150:
            pe_points = -4
        add("pe_quality", pe, pe_points)
    if pb is not None:
        pb_points = (8 - pb) / 8 * 3 if 0 < pb <= 8 else -3 if pb > 15 else 0
        add("pb_quality", pb, pb_points)
    if market_cap is not None:
        add("tradable_size", market_cap, _range_score(market_cap, 3000000000.0, 300000000000.0, 4, floor=500000000.0, ceiling=1200000000000.0))

    if strategy == "startup_dip":
        add("main_flow", main_flow, _cap_score(main_flow, 500000000.0, 26))
        add("north_flow", north_flow, _cap_score(north_flow, 200000000.0, 6))
        add("volume_ratio", volume_ratio, _range_score(volume_ratio, 1.1, 3.5, 13, floor=0.5, ceiling=8))
        add("volume_growth", volume_growth, _cap_score(volume_growth, 120.0, 8))
        add("price_not_overheated", pct, _range_score(pct, -2, 7, 13, floor=-8, ceiling=12))
        if pct is not None and pct > 10:
            add("overheat_penalty", pct, -8)
    elif strategy == "capital_absorption_dip":
        add("main_flow", main_flow, _cap_score(main_flow, 500000000.0, 30))
        add("north_flow", north_flow, _cap_score(north_flow, 200000000.0, 8))
        add("dip_with_inflow", pct, _range_score(pct, -6, 2, 16, floor=-10, ceiling=6))
        add("volume_ratio", volume_ratio, _range_score(volume_ratio, 0.8, 2.8, 8, floor=0.3, ceiling=6))
        if pct is not None and pct > 5:
            add("chase_penalty", pct, -8)
    elif strategy == "momentum_breakout":
        add("breakout_pct", pct, _range_score(pct, 2, 10, 18, floor=0, ceiling=16))
        add("volume_growth", volume_growth, _cap_score(volume_growth, 200.0, 18))
        add("volume_ratio", volume_ratio, _range_score(volume_ratio, 1.5, 4.5, 12, floor=0.8, ceiling=9))
        add("liquidity_breakout", amount, _cap_score(amount, 5000000000.0, 10))
        if price is not None and high and high > 0:
            close_pos = price / high
            add("close_near_high", close_pos, _range_score(close_pos, 0.965, 1.01, 10, floor=0.9, ceiling=1.05))
        if pct is not None and pct > 12:
            add("overheat_penalty", pct, -8)
    elif strategy == "limit_follow":
        add("seal_strength", seal, _cap_score(seal, 30000000.0, 32))
        add("limit_pct", pct, _range_score(pct, 9.6, 10.3, 18, floor=8.0, ceiling=12.5))
        add("turnover_for_board", turnover, _range_score(turnover, 0.5, 12, 10, floor=0, ceiling=35))
        add("board_liquidity", amount, _cap_score(amount, 2000000000.0, 8))
    elif strategy == "sector_leader":
        add("leader_pct", pct, _range_score(pct, 3, 10, 18, floor=0, ceiling=20))
        add("leader_liquidity", amount, _cap_score(amount, 5000000000.0, 18))
        add("turnover_confirm", turnover, _range_score(turnover, 3, 18, 12, floor=0.5, ceiling=45))
        add("volume_ratio", volume_ratio, _range_score(volume_ratio, 0.8, 4, 8, floor=0.2, ceiling=8))
        if pct is not None and pct > 20:
            add("ipo_overheat_penalty", pct, -20 if pct <= 50 else -35)
        if turnover is not None and turnover > 45:
            add("turnover_overheat_penalty", turnover, -10)
    elif strategy == "reversal_confirm":
        add("reversal_pct", pct, _range_score(pct, 5, 9.8, 22, floor=2, ceiling=13))
        add("two_day_control", interval_pct, _range_score(interval_pct, -8, 8, 8, floor=-15, ceiling=18))
        add("volume_ratio", volume_ratio, _range_score(volume_ratio, 1.0, 3.5, 10, floor=0.5, ceiling=8))
        add("reversal_liquidity", amount, _cap_score(amount, 3000000000.0, 10))
        if pct is not None and pct > 10.5:
            add("overheat_penalty", pct, -8)
    else:
        add("generic_flow", main_flow, _cap_score(main_flow, 500000000.0, 20))
        add("generic_pct", pct, _range_score(pct, 1, 9, 12, floor=-5, ceiling=15))

    return round(max(0.0, min(100.0, score)), 2), detail


def _rank_xuangu_rows_for_pool(rows: List[Dict], cfg: Dict, top_n: int = XUANGU_POOL_TOP_N) -> (List[Dict], Dict):
    ranked = []
    filtered = 0
    for idx, row in enumerate(rows or []):
        if idx >= XUANGU_POOL_SCAN_LIMIT:
            break
        exclude_reason = _xuangu_exclusion_reason(row)
        if exclude_reason:
            filtered += 1
            continue
        score, detail = _score_xuangu_row(row, cfg)
        ranked_row = dict(row)
        ranked_row["_pool_score"] = score
        ranked_row["_pool_score_detail"] = detail
        ranked_row["_pool_original_order"] = idx + 1
        ranked.append(ranked_row)

    ranked.sort(key=lambda r: (-float(r.get("_pool_score", 0)), int(r.get("_pool_original_order", 999999))))
    for rank, row in enumerate(ranked, 1):
        row["_pool_rank"] = rank

    return ranked[:top_n], {
        "raw": len(rows or []),
        "scanned": min(len(rows or []), XUANGU_POOL_SCAN_LIMIT),
        "filtered": filtered,
        "scored": len(ranked),
        "selected": min(top_n, len(ranked)),
    }


def _source_list(candidate: Dict, key: str, fallback_key: str = None) -> List[str]:
    value = candidate.get(key)
    if isinstance(value, list):
        items = value
    elif value:
        items = [value]
    elif fallback_key and candidate.get(fallback_key):
        items = [candidate.get(fallback_key)]
    else:
        items = []
    return [str(x).strip() for x in items if str(x).strip()]


def _merge_candidate_sources(candidates: List[Dict]) -> List[Dict]:
    """Merge duplicate stock candidates while preserving all screening sources."""
    merged: Dict[str, Dict] = {}
    order: List[str] = []

    def add_unique(target: List[str], values: List[str]) -> None:
        for value in values:
            if value and value not in target:
                target.append(value)

    for c in candidates or []:
        stock = str(c.get("stock", "")).strip()
        if not stock:
            continue
        if _is_bj_stock_code(stock):
            continue
        if stock not in merged:
            item = dict(c)
            item["source_pools"] = []
            item["source_queries"] = []
            item["source_reasons"] = []
            item["screen_ids"] = []
            item["strategy_types"] = []
            item["entry_biases"] = []
            item["_source_priorities"] = []
            item["_source_records"] = []
            item["_source_score_records"] = []
            merged[stock] = item
            order.append(stock)
        item = merged[stock]

        add_unique(item["source_pools"], _source_list(c, "source_pools", "pool"))
        add_unique(item["source_queries"], _source_list(c, "source_queries", "query"))
        add_unique(item["source_reasons"], _source_list(c, "source_reasons", "reason"))
        add_unique(item["screen_ids"], _source_list(c, "screen_ids", "screen_id"))
        add_unique(item["strategy_types"], _source_list(c, "strategy_types", "strategy_type"))
        add_unique(item["entry_biases"], _source_list(c, "entry_biases", "entry_bias"))
        if c.get("source") and not item.get("source"):
            item["source"] = c.get("source")
        if c.get("name") and (not item.get("name") or item.get("name") == stock):
            item["name"] = c.get("name")
        try:
            priority = int(c.get("priority", 99))
        except (TypeError, ValueError):
            priority = 99
        item["_source_priorities"].append(priority)
        item["_source_records"].append({
            "priority": priority,
            "pool": c.get("pool", ""),
            "screen_id": c.get("screen_id", ""),
            "strategy_type": c.get("strategy_type", ""),
            "entry_bias": c.get("entry_bias", ""),
            "query": c.get("query", ""),
            "reason": c.get("reason", ""),
            "pool_score": c.get("pool_score"),
            "pool_rank": c.get("pool_rank"),
        })
        if c.get("pool_score") not in (None, ""):
            item["_source_score_records"].append({
                "pool": c.get("pool", ""),
                "screen_id": c.get("screen_id", ""),
                "score": c.get("pool_score"),
                "rank": c.get("pool_rank"),
                "detail": c.get("pool_score_detail", {}),
            })

        # Preserve useful fetched data if a later duplicate has it.
        for data_key in (
            "_financial", "fundamental", "tech_data", "sector", "pe", "yesterday_chg",
            "pool_score", "pool_rank", "pool_score_detail", "pool_total_candidates",
            "pool_scored_candidates",
        ):
            if c.get(data_key) not in (None, "", {}) and item.get(data_key) in (None, "", {}):
                item[data_key] = c.get(data_key)

    result = []
    for stock in order:
        item = merged[stock]
        priorities = item.pop("_source_priorities", []) or [99]
        source_records = item.pop("_source_records", []) or []
        score_records = item.pop("_source_score_records", []) or []
        primary = min(source_records, key=lambda x: x.get("priority", 99)) if source_records else {}
        if primary.get("pool"):
            item["pool"] = primary.get("pool")
        if primary.get("screen_id"):
            item["screen_id"] = primary.get("screen_id")
        if primary.get("strategy_type"):
            item["strategy_type"] = primary.get("strategy_type")
        if primary.get("entry_bias"):
            item["entry_bias"] = primary.get("entry_bias")
        if primary.get("query"):
            item["query"] = primary.get("query")
        if primary.get("reason"):
            item["reason"] = primary.get("reason")
        if primary.get("pool_score") not in (None, ""):
            item["pool_score"] = primary.get("pool_score")
        if primary.get("pool_rank") not in (None, ""):
            item["pool_rank"] = primary.get("pool_rank")
        item["priority"] = min(priorities)
        item["source_score_records"] = sorted(
            score_records,
            key=lambda x: (
                999999 if x.get("rank") in (None, "") else int(x.get("rank", 999999)),
                -float(x.get("score") or 0),
            ),
        )
        item["screening_reason"] = "；".join(item.get("source_reasons") or [])[:300]
        result.append(item)
    return result

class CandidateGenerator:
    """从 mx-xuangu + 新闻 生成候选股票"""

    def __init__(self, skills_dir: Path, output_dir: Path):
        self.skills_dir = skills_dir
        self.output_dir = output_dir
        self._financial_cache: Dict[str, Dict] = {}  # 财务数据内存缓存
        self.screening_configs = [dict(c) for c in XUANGU_SCREEN_CONFIGS]
        self.screening_signature = _screening_signature(self.screening_configs)

    def prewarm_cache(self) -> List[Dict]:
        """
        Phase 1: xuangu筛选 → mx_data获取财务数据 → akshare兜底
        串行间隔获取每只股票的基本面数据，支持断点续传
        """
        from datetime import date  # 用于缓存更新
        
        logger.info("候选股基本面数据获取开始...")

        # 先跑 xuangu 获取候选股列表
        xuangu_candidates = self._run_xuangu_screening()
        if not xuangu_candidates:
            return []

        # 双渠道获取财务数据，优先内存缓存，避免今日mx_data已达上限的问题
        logger.info("财务数据获取开始（今日mx_data已近上限，优先内存缓存+akshare兜底）")
        
        # 检查财务数据缓存文件
        financial_cache_file = BASE_DIR / "output" / "fundamental_cache" / "all_stocks_financial.json"
        
        # 读取文件缓存数据（如果存在）
        file_cache_data = {}
        cache_is_today = _is_today_cache(financial_cache_file)
        
        if cache_is_today:
            logger.info("财务数据缓存为今天创建，将读取并检查完整性")
            file_cache_data = _read_financial_cache(financial_cache_file, "__ALL__") or {}
        else:
            if financial_cache_file.exists():
                from datetime import datetime
                mtime_timestamp = financial_cache_file.stat().st_mtime
                cache_date = datetime.fromtimestamp(mtime_timestamp).date()
                logger.info(f"财务数据缓存过期（创建于 {cache_date}），将重新获取")
            else:
                logger.info("无财务数据缓存文件，将重新获取")
        
        # 分离需要获取的股票和可以使用缓存的股票
        stocks_to_fetch = []
        stocks_from_cache = []
        
        for c in xuangu_candidates:
            stock = c.get("stock", "")
            if not stock:
                c["_financial"] = {}
                c["fundamental"] = "数据有限"
                continue
            
            # 1. 先查内存缓存
            if stock in self._financial_cache:
                cached_data = self._financial_cache[stock]
                c["_financial"] = cached_data
                c["fundamental"] = _build_financial_str(cached_data)
                stocks_from_cache.append(stock)
                continue
            
            # 2. 如果缓存是今天创建的，检查文件缓存
            if cache_is_today and file_cache_data:
                if stock in file_cache_data:
                    cached_data = file_cache_data[stock]
                    if _is_valid_financial_data(cached_data):
                        c["_financial"] = cached_data
                        c["fundamental"] = _build_financial_str(cached_data)
                        self._financial_cache[stock] = cached_data  # 存入内存缓存
                        stocks_from_cache.append(stock)
                        continue
            
            # 3. 需要API获取
            stocks_to_fetch.append((c, stock))
        
        logger.info(f"财务数据缓存检查完成: {len(stocks_from_cache)} 只使用缓存, {len(stocks_to_fetch)} 只需获取")

        # 对需要获取的股票进行API获取
        # 策略：xtquant高并行(20线程) → akshare小批量(3线程+间隔1s) → mx_data小批量(3线程+间隔1s)
        newly_fetched = {}

        # ── 第1层：xtquant 全部并行 ──
        def _fetch_xtquant(stock: str):
            fd = _fetch_financial_via_xtquant(stock)
            return stock, fd if (fd and _is_valid_financial_data(fd)) else None

        logger.info(f"  xtquant 高并行获取 {len(stocks_to_fetch)} 只...")
        xtquant_results = {}
        with ThreadPoolExecutor(max_workers=20) as ex:
            futures = {ex.submit(_fetch_xtquant, s): s for _, s in stocks_to_fetch}
            for f in as_completed(futures):
                stock, fd = f.result()
                if fd:
                    xtquant_results[stock] = fd
        logger.info(f"  xtquant 成功 {len(xtquant_results)} 只，失败 {len(stocks_to_fetch) - len(xtquant_results)} 只")

        # ── 第2层：akshare 小批量并行 ──
        akshare_stock = [s for (_, s) in stocks_to_fetch if s not in xtquant_results]
        akshare_results = {}
        if akshare_stock:
            logger.info(f"  akshare 小批量获取 {len(akshare_stock)} 只...")
            BATCH = 3
            for i in range(0, len(akshare_stock), BATCH):
                batch = akshare_stock[i:i+BATCH]
                with ThreadPoolExecutor(max_workers=3) as ex:
                    futures = {ex.submit(lambda sc: (sc, _fetch_financial_via_akshare(sc)), s): s for s in batch}
                    for f in as_completed(futures):
                        stock, fd = f.result()
                        if fd and _is_valid_financial_data(fd):
                            akshare_results[stock] = fd
                if i + BATCH < len(akshare_stock):
                    time.sleep(1)
            logger.info(f"  akshare 成功 {len(akshare_results)} 只，失败 {len(akshare_stock) - len(akshare_results)} 只")

        # ── 第3层：mx_data 小批量并行（兜底）─
        mx_stock = [s for s in akshare_stock if s not in akshare_results]
        mx_results = {}
        if mx_stock:
            logger.info(f"  mx_data 小批量兜底 {len(mx_stock)} 只...")
            BATCH = 3
            for i in range(0, len(mx_stock), BATCH):
                batch = mx_stock[i:i+BATCH]
                with ThreadPoolExecutor(max_workers=3) as ex:
                    futures = {ex.submit(lambda sc: (sc, _fetch_financial_via_mx(sc)), s): s for s in batch}
                    for f in as_completed(futures):
                        stock, fd = f.result()
                        if fd and _is_valid_financial_data(fd):
                            mx_results[stock] = fd
                if i + BATCH < len(mx_stock):
                    time.sleep(1)
            logger.info(f"  mx_data 成功 {len(mx_results)} 只，失败 {len(mx_stock) - len(mx_results)} 只")

        # 合并结果
        for stock, fd in xtquant_results.items():
            newly_fetched[stock] = fd
        for stock, fd in akshare_results.items():
            newly_fetched[stock] = fd
        for stock, fd in mx_results.items():
            newly_fetched[stock] = fd

        # 更新候选股数据
        stock_map = {s: c for c, s in stocks_to_fetch}
        for stock, fd in newly_fetched.items():
            c = stock_map.get(stock)
            if c:
                c["_financial"] = fd
                c["fundamental"] = _build_financial_str(fd)
                self._financial_cache[stock] = fd

        logger.info(f"  财务数据获取完成: xtquant {len(xtquant_results)} + akshare {len(akshare_results)} + mx {len(mx_results)} = {len(newly_fetched)} 只")
        
        # 更新文件缓存（如果有新获取的数据）
        if newly_fetched:
            logger.info(f"更新财务缓存文件: 新增 {len(newly_fetched)} 只股票数据")
            # 读取现有缓存数据
            all_cache_data = {}
            if financial_cache_file.exists():
                existing = _read_financial_cache(financial_cache_file, "__ALL__") or {}
                all_cache_data.update(existing)
            
            # 合并新数据
            all_cache_data.update(newly_fetched)
            
            # 写入更新后的缓存文件
            try:
                financial_cache_file.parent.mkdir(parents=True, exist_ok=True)
                with open(financial_cache_file, 'w', encoding='utf-8') as f:
                    json.dump({"data": all_cache_data, "updated": str(date.today())}, f, ensure_ascii=False)
                logger.info(f"财务缓存文件已更新: {len(all_cache_data)} 只股票")
            except Exception as e:
                logger.warning(f"更新财务缓存文件失败: {e}")
        
        self.candidates = xuangu_candidates
        logger.info(f"候选股基本面数据获取完成: {len(xuangu_candidates)} 只")
        return xuangu_candidates

    def generate(self, news_analyst_output: Dict, tech_analyst_output: Dict) -> List[Dict]:
        """
        生成候选股票列表
        策略：
        1. 从新闻中提取股票
        2. 用 mx-xuangu 筛选强势股
        3. 合并输出
        """
        from datetime import date  # 用于缓存更新
        
        candidates = []
        seen = set()

        # 新闻中提取（从新闻分析师的原始数据里找股票代码）
        news_candidates = self._extract_from_news(news_analyst_output)
        for c in news_candidates:
            if c["stock"] not in seen:
                seen.add(c["stock"])
                candidates.append(c)

        # mx-xuangu 筛选
        xuangu_candidates = self._run_xuangu_screening()
        for c in xuangu_candidates:
            if c["stock"] not in seen:
                seen.add(c["stock"])
                candidates.append(c)

        # 技术面筛选（根据技术分析师的结果补充）
        tech_candidates = self._extract_from_tech(tech_analyst_output)
        for c in tech_candidates:
            if c["stock"] not in seen:
                seen.add(c["stock"])
                candidates.append(c)

        # 财务数据获取，支持断点续传
        financial_cache_file = BASE_DIR / "output" / "fundamental_cache" / "all_stocks_financial.json"
        
        # 读取文件缓存数据（如果存在）
        file_cache_data = {}
        cache_is_today = _is_today_cache(financial_cache_file)
        
        if cache_is_today:
            logger.info(f"财务数据缓存为今天创建，将读取并检查完整性，共 {len(candidates)} 只股票")
            file_cache_data = _read_financial_cache(financial_cache_file, "__ALL__") or {}
        else:
            if financial_cache_file.exists():
                from datetime import datetime
                mtime_timestamp = financial_cache_file.stat().st_mtime
                cache_date = datetime.fromtimestamp(mtime_timestamp).date()
                logger.info(f"财务数据缓存过期（创建于 {cache_date}），将重新获取，共 {len(candidates)} 只股票")
            else:
                logger.info(f"无财务数据缓存文件，将重新获取，共 {len(candidates)} 只股票")
        
        # 分离需要获取的股票和可以使用缓存的股票
        stocks_to_fetch = []
        stocks_from_cache = []
        
        for c in candidates:
            stock = c.get("stock", "")
            if not stock:
                c["_financial"] = {}
                c["fundamental"] = "数据有限"
                continue
            
            # 1. 先查内存缓存
            if stock in self._financial_cache:
                cached_data = self._financial_cache[stock]
                c["_financial"] = cached_data
                c["fundamental"] = _build_financial_str(cached_data)
                stocks_from_cache.append(stock)
                continue
            
            # 2. 如果缓存是今天创建的，检查文件缓存
            if cache_is_today and file_cache_data:
                if stock in file_cache_data:
                    cached_data = file_cache_data[stock]
                    if _is_valid_financial_data(cached_data):
                        c["_financial"] = cached_data
                        c["fundamental"] = _build_financial_str(cached_data)
                        self._financial_cache[stock] = cached_data  # 存入内存缓存
                        stocks_from_cache.append(stock)
                        continue
            
            # 3. 需要API获取
            stocks_to_fetch.append((c, stock))
        
        logger.info(f"财务数据缓存检查完成: {len(stocks_from_cache)} 只使用缓存, {len(stocks_to_fetch)} 只需获取")
        
        # 对需要获取的股票进行API获取
        newly_fetched = {}
        for c, stock in stocks_to_fetch:
            logger.info(f"  获取 {stock} 财务数据...")
            
            # 尝试mx_data主渠道
            fd = _fetch_financial_via_mx(stock)
            
            # 如果mx_data失败或数据无效，用akshare兜底
            if not fd or not _is_valid_financial_data(fd):
                logger.info(f"  {stock} mx_data无效，尝试akshare兜底")
                fd = _fetch_financial_via_akshare(stock)
            
            c["_financial"] = fd or {}
            c["fundamental"] = _build_financial_str(c["_financial"])
            self._financial_cache[stock] = c["_financial"]  # 存入内存缓存
            
            # 保存获取到的有效数据，用于更新缓存
            if fd and _is_valid_financial_data(fd):
                newly_fetched[stock] = fd
            
            # 避免请求过快
            time.sleep(2)
        
        # 更新文件缓存（如果有新获取的数据）
        if newly_fetched:
            logger.info(f"更新财务缓存文件: 新增 {len(newly_fetched)} 只股票数据")
            # 读取现有缓存数据
            all_cache_data = {}
            if financial_cache_file.exists():
                existing = _read_financial_cache(financial_cache_file, "__ALL__") or {}
                all_cache_data.update(existing)
            
            # 合并新数据
            all_cache_data.update(newly_fetched)
            
            # 写入更新后的缓存文件
            try:
                financial_cache_file.parent.mkdir(parents=True, exist_ok=True)
                with open(financial_cache_file, 'w', encoding='utf-8') as f:
                    json.dump({"data": all_cache_data, "updated": str(date.today())}, f, ensure_ascii=False)
                logger.info(f"财务缓存文件已更新: {len(all_cache_data)} 只股票")
            except Exception as e:
                logger.warning(f"更新财务缓存文件失败: {e}")

        logger.info(f"候选股票生成完成: {len(candidates)} 只")
        return candidates[:15]

    def _extract_from_news(self, news_output: Dict) -> List[Dict]:
        """从新闻分析师输出中提取股票"""
        candidates = []
        import re
        raw = news_output.get("raw", [])
        seen = set()
        for item in raw:
            data = item.get("data", {})
            news_list = data.get("data", {}).get("llmSearchResponse", {}).get("data", []) if isinstance(data, dict) else []
            for article in news_list:
                content = article.get("content", "") + article.get("title", "")
                codes = re.findall(r'\b(\d{6})\b', content)
                for code in codes:
                    if code in seen:
                        continue
                    if _is_bj_stock_code(code):
                        continue
                    seen.add(code)
                    if code.startswith(("6", "3", "0", "8", "4", "9")) and len(code) == 6:
                        name_match = re.search(rf'{code}[^0-9]{{0,5}}([\u4e00-\u9fa5]{{2,8}})', content)
                        name = name_match.group(1) if name_match else code
                        sentiment = "利好" if "利好" in item.get("query", "") or "重大" in article.get("title", "") else "利空" if "利空" in item.get("query", "") else "中性"
                        candidates.append({"stock": code, "name": name, "reason": f"新闻提及:{article.get('title', '')[:30]}", "source": "news", "sentiment": sentiment})
        return candidates

    def _extract_from_tech(self, tech_output: Dict) -> List[Dict]:
        """从技术分析师输出中提取指数相关产品"""
        raw = tech_output.get("raw", {})
        candidates = []
        etf_map = {"399001": ("159901", "深证100ETF"), "399006": ("159915", "创业板ETF"), "000001": ("510050", "上证50ETF")}
        for code, info in raw.items():
            if info.get("trend") == "多头" and code in etf_map:
                etf_code, etf_name = etf_map[code]
                candidates.append({"stock": etf_code, "name": etf_name, "reason": f"{code} 均线多头", "source": "tech"})
        return candidates

    def _run_xuangu_screening(self) -> List[Dict]:
        """运行 mx-xuangu 筛选，并合并同股多来源。"""
        import csv
        results = []
        xuangu_output = self.output_dir / "xuangu"
        xuangu_output.mkdir(parents=True, exist_ok=True)

        def _latest_csv_for_query(query: str) -> Optional[Path]:
            safe_name = _xuangu_safe_filename(query)
            candidates = []
            exact = xuangu_output / f"mx_xuangu_{safe_name}.csv"
            if exact.exists():
                candidates.append(exact)
            candidates.extend(xuangu_output.glob(f"mx_xuangu_{safe_name[:32]}*.csv"))
            candidates = [p for p in set(candidates) if p.exists()]
            if not candidates:
                return None
            return max(candidates, key=lambda p: p.stat().st_mtime)

        def _read_rows(csv_file: Path, cfg: Dict, source: str = "xuangu", reason_prefix: str = "") -> Dict:
            with open(csv_file, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                ranked_rows, stats = _rank_xuangu_rows_for_pool(rows, cfg, XUANGU_POOL_TOP_N)
                for row in ranked_rows:
                    stock = _normalize_stock_code(row.get("股票代码") or row.get("代码"))
                    name = row.get("股票名称") or row.get("名称") or ""
                    score = row.get("_pool_score", 0)
                    rank = row.get("_pool_rank", 0)
                    reason = f"{reason_prefix}[{cfg['pool']}]池内评分{score:.1f}/100 排名{rank}: {cfg['query'][:18]}"
                    results.append({
                        "stock": str(stock),
                        "name": name,
                        "reason": reason,
                        "source": source,
                        "pool": cfg["pool"],
                        "screen_id": cfg["screen_id"],
                        "query": cfg["query"],
                        "strategy_type": cfg["strategy_type"],
                        "entry_bias": cfg["entry_bias"],
                        "priority": cfg.get("priority", 99),
                        "pool_score": score,
                        "pool_rank": rank,
                        "pool_score_detail": row.get("_pool_score_detail", {}),
                        "pool_total_candidates": stats.get("raw", 0),
                        "pool_scored_candidates": stats.get("scored", 0),
                        "source_pools": [cfg["pool"]],
                        "source_queries": [cfg["query"]],
                        "source_reasons": [reason],
                        "screen_ids": [cfg["screen_id"]],
                        "strategy_types": [cfg["strategy_type"]],
                        "entry_biases": [cfg["entry_bias"]],
                    })
            return stats

        for cfg in self.screening_configs:
            query = cfg["query"]
            pool = cfg["pool"]
            xuangu_success = False
            for attempt in range(3):
                try:
                    script = self.skills_dir / "mx-xuangu" / "mx_xuangu.py"
                    cmd = [sys.executable, str(script), query, "--output-dir", str(xuangu_output)]
                    r = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=55,
                        env=domestic_subprocess_env(os.environ),
                    )
                    if r.returncode == 0:
                        logger.info(f"选股 [{pool}] 完成")
                        xuangu_success = True
                        break
                    else:
                        err_msg = r.stdout[:200] if r.stdout else r.stderr[:200] if r.stderr else ""
                        logger.warning(f"选股 [{pool}] 第{attempt+1}次失败: {err_msg}")
                        if attempt < 2:
                            wait = 10 * (2 ** attempt)
                            logger.info(f"[{pool}] 等待 {wait}s 后重试...")
                            time.sleep(wait)
                except Exception as e:
                    logger.warning(f"选股 [{pool}] 第{attempt+1}次异常: {e}")
                    if attempt < 2:
                        time.sleep(10 * (2 ** attempt))
            if not xuangu_success:
                csv_fallback = _latest_csv_for_query(query)
                if csv_fallback:
                    try:
                        stats = _read_rows(csv_fallback, cfg, source="xuangu_csv_cache", reason_prefix="[CSV缓存]")
                        if stats.get("selected", 0) > 0:
                            logger.warning(f"选股 [{pool}] API失败，使用CSV缓存兜底: {csv_fallback.name}")
                        else:
                            csv_fallback = None
                    except Exception as e:
                        logger.warning(f"选股 [{pool}] CSV缓存读取失败: {e}")
                        csv_fallback = None
                if not csv_fallback:
                    logger.warning(f"选股 [{pool}] 3次全部失败，改用akshare候选")
                    fallback = _xuangu_fallback_via_akshare(cfg)
                    if fallback:
                        results.extend(fallback)
                        logger.info(f"选股 [{pool}] akshare兜底成功: {len(fallback)} 只")
                    else:
                        logger.warning(f"选股 [{pool}] akshare兜底也无结果")
                else:
                    pass
            else:
                safe_name = _xuangu_safe_filename(query)
                csv_file = xuangu_output / f"mx_xuangu_{safe_name}.csv"
                if not csv_file.exists():
                    matches = sorted(xuangu_output.glob(f"mx_xuangu_{safe_name[:32]}*.csv"))
                    csv_file = matches[-1] if matches else csv_file
                if csv_file.exists():
                    try:
                        stats = _read_rows(csv_file, cfg)
                        logger.info(
                            f"选股 [{pool}] 池内评分: 原始{stats.get('raw', 0)}条 "
                            f"扫描{stats.get('scanned', 0)}条 过滤{stats.get('filtered', 0)}条 "
                            f"入选{stats.get('selected', 0)}只"
                        )
                    except Exception as e:
                        logger.warning(f"读取CSV失败 {csv_file}: {e}")
                else:
                    logger.warning(f"选股 [{pool}] 未找到CSV文件: {csv_file.name}")
            time.sleep(3)  # 避免mx-xuangu请求频率过高

        merged = _merge_candidate_sources(results)
        logger.info(f"选股候选合并: 原始{len(results)}条 → 去重{len(merged)}只")
        return merged

def _xuangu_fallback_via_akshare(screen_cfg) -> List[Dict]:
    """
    当 mx-xuangu 失败时，通过 akshare 获取对应选股池的候选股。
    兜底不追求完全等价，但要尽量保持同一策略意图。
    """
    import akshare as ak
    from datetime import datetime, timedelta

    cfg = dict(screen_cfg or {})
    pool = cfg.get("pool", "选股池")
    results = []
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    def _make_candidate(code, name, reason, rank=None, total=None):
        if _is_bj_stock_code(code):
            return None
        try:
            fallback_rank = int(rank) if rank not in (None, "") else None
        except (TypeError, ValueError):
            fallback_rank = None
        fallback_score = None
        if fallback_rank:
            fallback_score = round(max(30.0, 62.0 - (fallback_rank - 1) * 1.5), 2)
        reason = f"[{pool}]{reason}"
        return {
            "stock": str(code).strip(),
            "name": str(name or "").strip(),
            "reason": reason,
            "source": "akshare",
            "pool": pool,
            "screen_id": cfg.get("screen_id", pool),
            "query": cfg.get("query", ""),
            "strategy_type": cfg.get("strategy_type", ""),
            "entry_bias": cfg.get("entry_bias", ""),
            "priority": cfg.get("priority", 99),
            "pool_score": fallback_score,
            "pool_rank": fallback_rank,
            "pool_score_detail": {
                "source": "akshare_fallback_rank",
                "rank": fallback_rank,
                "score_proxy": fallback_score,
            } if fallback_rank else {},
            "pool_total_candidates": total,
            "pool_scored_candidates": total,
            "source_pools": [pool],
            "source_queries": [cfg.get("query", "")],
            "source_reasons": [reason],
            "screen_ids": [cfg.get("screen_id", pool)],
            "strategy_types": [cfg.get("strategy_type", "")],
            "entry_biases": [cfg.get("entry_bias", "")],
        }

    def _append_from_df(df, reason, limit=20):
        if df is None:
            return
        total = min(len(df), limit)
        for rank, (_, row) in enumerate(df.head(limit).iterrows(), 1):
            code = str(row.get('代码', '')).strip()
            name = str(row.get('名称', '')).strip()
            if len(code) == 6 and code.isdigit():
                candidate = _make_candidate(code, name, reason, rank=rank, total=total)
                if candidate:
                    results.append(candidate)

    try:
        if pool == "准备启动":
            # 近10日资金流入榜作为主线，再尽量过滤短期涨幅过高的票。
            df = retry_call(
                f"akshare 选股兜底 {pool} 10日资金流",
                lambda: ak.stock_individual_fund_flow_rank(indicator='10日'),
                retries=4,
                base_delay=2,
                throttle_key="akshare",
                min_interval=1.0,
            )
            if df is not None and len(df) > 0:
                work = df.copy()
                pct_col = next((c for c in work.columns if "涨跌幅" in str(c)), None)
                if pct_col:
                    try:
                        work[pct_col] = work[pct_col].astype(str).str.replace("%", "", regex=False).astype(float)
                        work = work[work[pct_col] <= 10]
                    except Exception:
                        pass
                _append_from_df(work, "akshare10日资金流入且短涨幅不过热")

        elif pool == "首板追击":
            # 昨日首板（首日涨停）
            prev = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            df = retry_call(
                f"akshare 选股兜底 {pool} 首板",
                lambda: ak.stock_zt_pool_previous_em(date=prev),
                retries=4,
                base_delay=2,
                throttle_key="akshare",
                min_interval=1.0,
            )
            first_limit = df[df['涨停价'] == df['最新价']].head(20)
            _append_from_df(first_limit, "akshare首板")

        elif pool == "热点龙头":
            # 强势股中按成交额排序取头部
            df = retry_call(
                f"akshare 选股兜底 {pool} 强势股",
                lambda: ak.stock_zt_pool_strong_em(date=yesterday),
                retries=4,
                base_delay=2,
                throttle_key="akshare",
                min_interval=1.0,
            )
            df_sorted = df.sort_values('成交额', ascending=False).head(20)
            _append_from_df(df_sorted, "akshare强势股成交额")

        elif pool == "资金异动":
            # 3日资金流入榜，若接口带涨跌幅字段则优先保留近3日下跌股。
            try:
                df = retry_call(
                    f"akshare 选股兜底 {pool} 3日资金流",
                    lambda: ak.stock_individual_fund_flow_rank(indicator='3日'),
                    retries=4,
                    base_delay=2,
                    throttle_key="akshare",
                    min_interval=1.0,
                )
                if df is not None and len(df) > 0:
                    work = df.copy()
                    pct_col = next((c for c in work.columns if "涨跌幅" in str(c)), None)
                    if pct_col:
                        try:
                            work[pct_col] = work[pct_col].astype(str).str.replace("%", "", regex=False).astype(float)
                            down = work[work[pct_col] < 0]
                            if len(down) > 0:
                                work = down
                        except Exception:
                            pass
                    _append_from_df(work, "akshare3日资金流入且价格走弱")
            except Exception as e:
                logger.warning(f"资金异动akshare失败: {e}")

        elif pool in ("突破新高", "强势反包"):
            # 涨停股池中按涨幅排序（强势股）
            df = retry_call(
                f"akshare 选股兜底 {pool} 涨停强势",
                lambda: ak.stock_zt_pool_strong_em(date=yesterday),
                retries=4,
                base_delay=2,
                throttle_key="akshare",
                min_interval=1.0,
            )
            df_sorted = df.sort_values('涨跌幅', ascending=False).head(20)
            reason = "akshare强势突破" if pool == "突破新高" else "akshare强势反包"
            _append_from_df(df_sorted, reason)

    except Exception as e:
        logger.warning(f"[_xuangu_fallback_via_akshare] [{pool}] 异常: {e}")

    return results
# ── 测试入口 ───────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    scorer = LLMScorer()

    test_candidates = [
        {"stock": "000001", "name": "平安银行", "reason": "均线多头排列", "source": "xuangu"},
        {"stock": "600519", "name": "贵州茅台", "reason": "基本面稳健", "source": "news"},
        {"stock": "159915", "name": "创业板ETF", "reason": "创业板均线多头", "source": "tech"},
    ]
    test_analysts = [
        {"name": "新闻分析师", "color": "📰", "findings": "今日市场情绪较好，有利好消息", "raw": {}},
        {"name": "技术分析师", "color": "📈", "findings": "深证均线多头，创业板MACD金叉", "raw": {}},
        {"name": "基本面分析师", "color": "📊", "findings": "金融板块估值偏低", "raw": {}},
        {"name": "情绪分析师", "color": "🌡️", "findings": "市场情绪偏暖", "raw": {}},
    ]

    print("开始LLM打分测试...")
    scores, pending = scorer.score_candidates(test_analysts, test_candidates)
    print(json.dumps(scores, ensure_ascii=False, indent=2))
    if pending:
        print(f"\n{len(pending)} 只待Route A补打: {[c['stock'] for c in pending]}")
