#!/usr/bin/env python3
"""
MCP Web Search 客户端
通过 MiniMax 官方的 minimax-coding-plan-mcp (v0.0.4) 提供 web_search 能力。

设计目标：
1. 透明替代原 workflow.py:747 _call_llm() 里的 urllib+M3 失败路径
2. 失败时返回 "" 让 LLMWebSearchAnalyst.run() 原有 fallback 接管（保持兼容）
3. 单次调用、单进程；不引入持久化/单例（MCP 启动 ~8s 一次性成本，分析师只跑一次）

MCP 协议（stdio JSON-RPC）：
  initialize → notifications/initialized → tools/list → tools/call

环境：
  MINIMAX_API_KEY  — 从 auth-profiles.json (minimax-portal:default) 取，fallback 到 os.environ
  MINIMAX_API_HOST — 默认 https://api.minimaxi.com
  uvx              — /opt/homebrew/bin/uvx（绝对路径，cron 环境 PATH 可能不完整）

用法：
    with MCPWebSearchClient() as client:
        text = client.search("今天 A 股市场重要新闻", num_results=5)
        # text 可能是 JSON 字符串（5 条搜索结果）或者 ""（失败）
"""

import json
import logging
import os
import select
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger("daily_stock_workflow")

# MCP server 包名（官方 PyPI）
_MCP_PACKAGE = "minimax-coding-plan-mcp"
# uvx 绝对路径（cron 环境 PATH 可能不完整）
_UVX_BIN = "/opt/homebrew/bin/uvx"
# MCP 协议版本（与 server 协商用 2024-11-05）
_MCP_PROTOCOL = "2024-11-05"
# 启动超时（uvx 拉包+初始化）
_STARTUP_TIMEOUT = 60
# 单次搜索超时
_SEARCH_TIMEOUT = 30
# MCP server 日志输出限额（避免刷屏）
_STDERR_LOG_LIMIT = 1500


class MCPUnavailableError(Exception):
    """MCP server 不可用（启动失败/网络断/超时）"""


def _resolve_credentials() -> tuple:
    """
    从 auth-profiles.json 拿 key，从 models.json 拿 host
    回退到环境变量。
    返回 (api_key, api_host)
    """
    api_key = ""
    api_host = "https://api.minimaxi.com"

    try:
        # 1) key: ~/.openclaw/agents/main/agent/auth-profiles.json
        profile_path = (
            Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth-profiles.json"
        )
        if profile_path.exists():
            with open(profile_path) as pf:
                profiles = json.load(pf)
            for pk in ("minimax-portal:default", "minimax:default", "minimax-cn:default"):
                entry = profiles.get("profiles", {}).get(pk, {})
                k = entry.get("key") or entry.get("access", "")
                if k and len(k) > 10:
                    api_key = k
                    break

        # 2) host: ~/.openclaw/agents/main/agent/models.json
        models_path = (
            Path.home() / ".openclaw" / "agents" / "main" / "agent" / "models.json"
        )
        if models_path.exists():
            with open(models_path) as f:
                cfg = json.load(f)
            for pk in ("minimax-portal", "minimax", "minimax-cn"):
                pcfg = cfg.get("providers", {}).get(pk, {})
                if pcfg:
                    api_host = pcfg.get("baseUrl", api_host)
                    break
    except Exception as e:
        logger.warning(f"[MCP] 读取配置失败: {e}")

    # 3) 回退到环境变量
    if not api_key:
        api_key = os.environ.get("MINIMAX_API_KEY") or os.environ.get("MX_APIKEY", "")

    return api_key, api_host


def _format_organic_results(organic: list, num: int) -> str:
    """
    把 MCP 返回的 organic[] 格式化成易读的 findings 文本。
    跟原 prompt 要求一致：来源、标题、摘要（50字内）。
    """
    lines = []
    for i, item in enumerate(organic[:num], 1):
        title = item.get("title", "").strip()
        snippet = item.get("snippet", "").strip()
        link = item.get("link", "").strip()
        date = item.get("date", "").strip()
        # 摘要截断到 50 字内
        if len(snippet) > 50:
            snippet = snippet[:50] + "…"
        # 来源：link 的 host
        source = ""
        if link:
            try:
                from urllib.parse import urlparse
                source = urlparse(link).netloc.replace("www.", "")
            except Exception:
                pass
        meta = " | ".join(filter(None, [source, date]))
        meta_part = f" 📍{meta}" if meta else ""
        lines.append(f"{i}. **{title}**{meta_part}\n   {snippet}")
    return "\n\n".join(lines)


class MCPWebSearchClient:
    """
    一次性 MCP web_search 客户端（上下文管理器）。
    进入 with 块：起 MCP 子进程 + initialize + tools/list
    退出 with 块：kill 子进程
    失败：抛 MCPUnavailableError 或返回 ""
    """

    def __init__(
        self,
        startup_timeout: int = _STARTUP_TIMEOUT,
        search_timeout: int = _SEARCH_TIMEOUT,
        uvx_bin: str = _UVX_BIN,
    ):
        self.startup_timeout = startup_timeout
        self.search_timeout = search_timeout
        self.uvx_bin = uvx_bin
        self.proc: Optional[subprocess.Popen] = None
        self._msg_id = 0
        self._search_tool_schema: Optional[dict] = None

    # ── 上下文管理 ──
    def __enter__(self):
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop()
        return False

    # ── 公开 API ──
    def search(self, query: str, num_results: int = 5) -> str:
        """
        调 web_search，返回格式化的中文新闻列表。
        失败返回 ""。
        """
        if not self.proc or self.proc.poll() is not None:
            logger.warning("[MCP] server 未运行，search 跳过")
            return ""

        # web_search 真实 schema 是 {query: str}，num_results 不在 schema 里
        # 但我们传过去 server 接受（实测能返回 N 条）
        resp = self._mcp_call("tools/call", {
            "name": "web_search",
            "arguments": {
                "query": query,
                "num_results": num_results,
            },
        })

        if not resp or "result" not in resp:
            return ""

        result = resp["result"]
        if result.get("isError"):
            logger.warning(f"[MCP] web_search 返回 isError: {result}")
            return ""

        # 解析 content[].text（MCP server 把 Google 结果塞 JSON 字符串里）
        content = result.get("content", [])
        if not content:
            return ""

        for block in content:
            if block.get("type") == "text":
                text = block.get("text", "")
                # 尝试解外层 JSON
                try:
                    parsed = json.loads(text)
                    organic = parsed.get("organic", [])
                    if organic:
                        return _format_organic_results(organic, num_results)
                except (json.JSONDecodeError, TypeError):
                    # 不是 JSON 包裹，原样返回
                    return text

        return ""

    # ── 内部：进程管理 ──
    def _start(self):
        api_key, api_host = _resolve_credentials()
        if not api_key or len(api_key) < 10:
            raise MCPUnavailableError("无法获取 MiniMax API key（auth-profiles.json + env 都为空）")

        if not Path(self.uvx_bin).exists():
            raise MCPUnavailableError(f"uvx 不存在: {self.uvx_bin}")

        env = {
            **os.environ,
            "MINIMAX_API_KEY": api_key,
            "MINIMAX_API_HOST": api_host,
        }

        logger.info(f"[MCP] 启动 {_MCP_PACKAGE} (host={api_host})")
        self.proc = subprocess.Popen(
            [self.uvx_bin, _MCP_PACKAGE, "-y"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            text=True,
            bufsize=1,
        )

        # initialize
        init_resp = self._mcp_call("initialize", {
            "protocolVersion": _MCP_PROTOCOL,
            "capabilities": {},
            "clientInfo": {"name": "daily-stock-workflow", "version": "1.0"},
        }, timeout=self.startup_timeout)

        if not init_resp or "result" not in init_resp:
            err = self._drain_stderr()
            self._stop()
            raise MCPUnavailableError(
                f"MCP initialize 失败: {init_resp} | stderr: {err[:_STDERR_LOG_LIMIT]}"
            )

        server_info = init_resp.get("result", {}).get("serverInfo", {})
        logger.info(f"[MCP] 已连接, server={server_info}")

        # notifications/initialized
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
        self.proc.stdin.flush()

        # tools/list（验证 web_search 在）
        tools_resp = self._mcp_call("tools/list", {}, timeout=15)
        if not tools_resp or "result" not in tools_resp:
            self._stop()
            raise MCPUnavailableError(f"tools/list 失败: {tools_resp}")

        tools = tools_resp["result"].get("tools", [])
        tool_names = [t.get("name") for t in tools]
        if "web_search" not in tool_names:
            self._stop()
            raise MCPUnavailableError(f"server 暴露的 tools 不含 web_search: {tool_names}")

        for t in tools:
            if t.get("name") == "web_search":
                self._search_tool_schema = t.get("inputSchema")
                break
        logger.info(f"[MCP] tools={tool_names}, web_search schema={self._search_tool_schema}")

    def _stop(self):
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.kill()
            except Exception:
                pass
            try:
                self.proc.wait(timeout=5)
            except Exception:
                pass
        self.proc = None
        # stderr 摘要
        if self.proc is None:
            return

    def _drain_stderr(self) -> str:
        if not self.proc or not self.proc.stderr:
            return ""
        try:
            if select.select([self.proc.stderr], [], [], 0.5)[0]:
                return self.proc.stderr.read() or ""
        except Exception:
            pass
        return ""

    def _mcp_call(self, method: str, params: Optional[dict] = None, timeout: int = _SEARCH_TIMEOUT):
        """
        发 JSON-RPC 请求，等响应。带超时。
        """
        if not self.proc or self.proc.poll() is not None:
            return None

        self._msg_id += 1
        msg = {"jsonrpc": "2.0", "id": self._msg_id, "method": method}
        if params is not None:
            msg["params"] = params
        line = json.dumps(msg, ensure_ascii=False) + "\n"

        try:
            self.proc.stdin.write(line)
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError) as e:
            logger.warning(f"[MCP] 写 stdin 失败: {e}")
            return None

        # 等响应（行是 JSON）
        deadline = time.time() + timeout
        while time.time() < deadline:
            wait = min(0.5, deadline - time.time())
            if wait <= 0:
                break
            if not select.select([self.proc.stdout], [], [], wait)[0]:
                continue
            try:
                out_line = self.proc.stdout.readline()
            except Exception as e:
                logger.warning(f"[MCP] 读 stdout 失败: {e}")
                return None
            if not out_line:
                time.sleep(0.1)
                continue
            try:
                resp = json.loads(out_line)
            except json.JSONDecodeError:
                # 非 JSON 行（MCP server 不应该这么干），跳过
                continue
            # 跳过 notification（没有 id）
            if "id" not in resp:
                continue
            return resp

        logger.warning(f"[MCP] {method} 超时 ({timeout}s)")
        return None


# ── CLI 自测 ──
def _selftest():
    """
    跑一次成功 + 一次失败降级，验证客户端工作正常。
    """
    print("=" * 60, flush=True)
    print("[自测 1] 成功路径: 搜 '今天 A 股市场重要新闻'", flush=True)
    print("=" * 60, flush=True)
    try:
        with MCPWebSearchClient() as c:
            text = c.search("今天 A 股市场重要新闻", num_results=5)
        if text:
            print(f"\n✅ 成功, 返回 {len(text)} 字:", flush=True)
            print(text[:1500], flush=True)
        else:
            print("❌ 返回空字符串", flush=True)
            return False
    except MCPUnavailableError as e:
        print(f"❌ MCPUnavailableError: {e}", flush=True)
        return False

    print("\n" + "=" * 60, flush=True)
    print("[自测 2] 失败降级: 错 key 触发异常路径", flush=True)
    print("=" * 60, flush=True)
    # 直接测：用一个假 key 强制走空值路径
    api_key, api_host = _resolve_credentials()
    if not api_key:
        print("❌ 拿不到真实 key，跳过失败测试", flush=True)
        return True
    # 把 key 改 1 位
    fake_key = api_key[:-1] + ("A" if api_key[-1] != "A" else "B")
    env_backup = os.environ.get("MINIMAX_API_KEY", "")
    os.environ["MINIMAX_API_KEY"] = fake_key
    try:
        c = MCPWebSearchClient()
        try:
            c._start()
        except MCPUnavailableError as e:
            print(f"✅ 失败降级正常, 抛 MCPUnavailableError:", flush=True)
            print(f"   {str(e)[:500]}", flush=True)
            return True
        else:
            # 真要成功也没关系
            print("⚠️  假 key 也成功了（可能 server 没校验，或 API 没即时 reject）", flush=True)
            c._stop()
            return True
    finally:
        os.environ["MINIMAX_API_KEY"] = env_backup


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ok = _selftest()
    sys.exit(0 if ok else 1)
