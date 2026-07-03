#!/usr/bin/env python3
"""
每日选股工作流 - 主程序
===========================
Phase 1: 并行5大分析师（新闻/技术/大盘情绪/舆情）
Phase 2: 汇总 + 初步选股决策
Phase 3: 回测验证 → 最终操作建议
Phase 4: 盘中自动买卖（mx-moni）+ 实时推送（飞书）

每日 Cron: workflow=09:00 | intraday-buy=09:31-14:50 | intraday-monitor=14:50
"""

import os
import sys
import json
import time
import hashlib
import fcntl
import logging
import traceback
import re
import subprocess
import threading
import importlib.util
from contextlib import contextmanager
from datetime import datetime, date
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional
from llm_scorer import _is_valid_tech_data, _build_financial_str
from domestic_network import domestic_subprocess_env, retry_call
from stock_selection_debate.run_debate_phase import (
    run_debate_phase,
    debate_phase_to_phase2_format,
    _apply_pool_money_flow_seed,
    _apply_quant_confidence_overlay,
)

# ── 临时 logger（在Backtrader导入失败时用到）──────────────
import logging
_logger = logging.getLogger("daily_stock_workflow")

# Backtrader Phase 3 回测引擎（可选，失败时降级到旧实现）
try:
    from backtest import run_signal_backtest as _bt_run_signal_backtest
    _BT_AVAILABLE = True
except Exception as e:
    _logger.warning(f"Backtrader 导入失败: {e}，使用旧版回测")
    _BT_AVAILABLE = False


# ── 路径设置 ──────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
SKILLS_DIR = Path(os.environ.get("OPENCLAW_WORKSPACE", "./workspace")) / "skills"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# ── 日志配置 ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / f"workflow_{date.today().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("daily_stock_workflow")


def _load_mx_data_class():
    """Load mx-data by absolute file path; sys.path import is brittle in cron/tests."""
    mx_file = SKILLS_DIR / "mx-data" / "mx_data.py"
    if not mx_file.exists():
        raise ModuleNotFoundError(f"mx_data.py not found: {mx_file}")
    module_name = "_openclaw_mx_data_runtime"
    spec = importlib.util.spec_from_file_location(module_name, mx_file)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load mx_data spec: {mx_file}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.MXData


@contextmanager
def _exclusive_file_lock(lock_path: Path, *, nonblocking: bool = False):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(lock_path, "a+", encoding="utf-8")
    flags = fcntl.LOCK_EX | (fcntl.LOCK_NB if nonblocking else 0)
    try:
        fcntl.flock(fh.fileno(), flags)
        yield fh
    finally:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            fh.close()


def _try_acquire_workflow_lock() -> Optional[Any]:
    """Acquire a daily singleton lock before the workflow starts.

    fcntl locks are process-scoped on macOS, so a repeated trigger from the same
    OpenClaw runtime can re-enter.  mkdir is atomic and blocks both same-process
    and cross-process duplicate launches.
    """
    date_key = date.today().strftime("%Y%m%d")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    lock_path = OUTPUT_DIR / f"daily_stock_workflow_{date_key}.lock"
    lock_dir = OUTPUT_DIR / f"daily_stock_workflow_{date_key}.lockdir"
    owner_file = lock_dir / "owner.json"
    stale_seconds = int(os.getenv("DAILY_STOCK_WORKFLOW_LOCK_STALE_SECONDS", str(12 * 3600)))

    def _pid_alive(pid: Any) -> bool:
        try:
            pid_int = int(pid)
        except Exception:
            return False
        if pid_int <= 0:
            return False
        try:
            os.kill(pid_int, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except Exception:
            return True

    def _read_owner() -> Dict[str, Any]:
        try:
            return json.loads(owner_file.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _remove_stale_lock() -> bool:
        try:
            for child in lock_dir.iterdir():
                if child.is_file() or child.is_symlink():
                    child.unlink(missing_ok=True)
            lock_dir.rmdir()
            return True
        except FileNotFoundError:
            return True
        except Exception as e:
            logger.warning(f"清理旧工作流锁失败: {e}")
            return False

    while True:
        try:
            lock_dir.mkdir(mode=0o755)
            break
        except FileExistsError:
            owner = _read_owner()
            pid = owner.get("pid")
            if _pid_alive(pid):
                logger.warning(f"已有今日选股工作流实例正在运行: pid={pid}, started_at={owner.get('started_at')}")
                return None
            try:
                age = time.time() - lock_dir.stat().st_mtime
            except FileNotFoundError:
                continue
            if age < stale_seconds:
                logger.warning(f"发现无存活PID但尚未过期的工作流锁，暂不抢占: {owner}")
                return None
            logger.warning(f"发现过期工作流锁，准备清理后重试: {owner}")
            if not _remove_stale_lock():
                return None

    payload = {
        "pid": os.getpid(),
        "started_at": datetime.now().isoformat(),
        "lock_dir": str(lock_dir),
    }
    tmp_owner = owner_file.with_name(f"{owner_file.name}.{os.getpid()}.tmp")
    tmp_owner.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp_owner, owner_file)
    lock_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return {"lock_dir": lock_dir, "lock_path": lock_path, "owner": payload}


def _release_workflow_lock(lock_fh: Optional[Any]) -> None:
    if not lock_fh:
        return
    if isinstance(lock_fh, dict):
        lock_dir = Path(lock_fh.get("lock_dir", ""))
        lock_path = Path(lock_fh.get("lock_path", ""))
        try:
            owner_file = lock_dir / "owner.json"
            owner_file.unlink(missing_ok=True)
            lock_dir.rmdir()
            lock_path.unlink(missing_ok=True)
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning(f"释放工作流锁失败: {e}")
        return
    try:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
    finally:
        lock_fh.close()


def _stable_digest(data: Any) -> str:
    payload = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _candidate_universe_signature(candidates: List[Dict[str, Any]], screening_signature: str = "") -> str:
    items = []
    for c in candidates or []:
        items.append({
            "stock": c.get("stock", ""),
            "name": c.get("name", ""),
            "pool": c.get("pool", ""),
            "screen_ids": c.get("screen_ids") or c.get("screen_id") or [],
            "pool_score": c.get("pool_score"),
            "pool_rank": c.get("pool_rank"),
        })
    return _stable_digest({
        "screening_signature": screening_signature or "",
        "stocks": sorted(items, key=lambda x: x.get("stock", "")),
    })


def _daily_push_marker_file() -> Path:
    return OUTPUT_DIR / f"daily_report_push_{date.today().strftime('%Y%m%d')}.json"


def _read_daily_push_marker() -> Dict[str, Any]:
    marker = _daily_push_marker_file()
    if not marker.exists():
        return {}
    try:
        return json.loads(marker.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _daily_report_already_pushed(report: Dict[str, Any]) -> bool:
    if os.getenv("DAILY_STOCK_WORKFLOW_FORCE_PUSH", "0") == "1":
        return False
    data = _read_daily_push_marker()
    if data.get("date") != date.today().isoformat() or data.get("status") != "success":
        return False
    marker_digest = data.get("report_digest")
    return bool(marker_digest) and marker_digest == _stable_digest(report)


def _daily_report_failure_already_notified(reason: str) -> bool:
    if os.getenv("DAILY_STOCK_WORKFLOW_FORCE_PUSH", "0") == "1":
        return False
    data = _read_daily_push_marker()
    return (
        data.get("date") == date.today().isoformat()
        and data.get("status") == "failed"
        and data.get("reason") == reason
        and data.get("notified") is True
    )


def _mark_daily_report_push_status(report: Dict[str, Any], status: str, **extra: Any) -> None:
    marker = _daily_push_marker_file()
    tmp = marker.with_name(f"{marker.name}.{os.getpid()}.tmp")
    top_picks = ((report.get("phase2") or {}).get("top_picks") or [])
    data = {
        "date": date.today().isoformat(),
        "status": status,
        "updated_at": datetime.now().isoformat(),
        "report_digest": _stable_digest(report),
        "top_picks_count": len(top_picks),
        "top_picks": [
            {
                "stock": s.get("stock", ""),
                "name": s.get("name", ""),
                "signal": s.get("signal", ""),
                "confidence": s.get("confidence", ""),
            }
            for s in top_picks
        ],
    }
    data.update(extra)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
    tmp.replace(marker)


def _mark_daily_report_pushed(report: Dict[str, Any]) -> None:
    _mark_daily_report_push_status(report, "success", pushed_at=datetime.now().isoformat())

# ── 环境变量检查 ──────────────────────────────────────────
def check_env():
    critical = []
    warnings = []
    if not os.getenv("MX_APIKEY"):
        critical.append("MX_APIKEY")
    if not os.getenv("MX_API_URL"):
        warnings.append("MX_API_URL (有默认值，将使用 https://mkapi2.dfcfs.com/finskillshub")
    if critical:
        logger.error(f"缺少关键环境变量: {', '.join(critical)}，程序无法正常运行")
        import sys
        sys.exit(1)
    if warnings:
        logger.warning(f"缺少可选环境变量: {', '.join(warnings)}")
    logger.info("环境变量检查通过")


def _phase1_context_file() -> Path:
    return OUTPUT_DIR / "phase1_context.json"


def _is_today_file(path: Path) -> bool:
    """检查文件是否今天创建（是则使用，否则删除重建）"""
    if not path.exists():
        return True  # 不存在 = 用
    try:
        stat = path.stat()
        import datetime as _dt
        mtime = _dt.datetime.fromtimestamp(stat.st_mtime)
        today = _dt.datetime.now()
        return mtime.date() == today.date()
    except:
        return False  # 出错当不是今天，删掉重建


def _save_phase1_context(phase1_results: List[Dict[str, Any]]) -> None:
    """Persist Phase 1 market context so checkpoint resume keeps news/sentiment."""
    if not phase1_results:
        return
    path = _phase1_context_file()
    tmp = path.with_name(path.name + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(phase1_results, f, ensure_ascii=False, indent=2)
            f.flush()
        tmp.replace(path)
        logger.info(f"Phase 1 上下文已保存: {path}")
    except Exception as e:
        logger.warning(f"Phase 1 上下文保存失败: {e}")


def _load_phase1_context() -> List[Dict[str, Any]]:
    path = _phase1_context_file()
    if not path.exists():
        return []
    # 非今天创建的缓存，视为无效，删除后由调用方重建
    if not _is_today_file(path):
        logger.info(f"Phase 1 上下文非今日创建，删除重建: {path}")
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            logger.info(f"Resume 模式：已恢复 Phase 1 上下文 {len(data)} 条")
            return data
    except Exception as e:
        logger.warning(f"Phase 1 上下文读取失败: {e}")
    return []


def _ensure_resume_market_context(phase1_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """For resume runs, keep Feishu card market news and sentiment from being empty."""
    existing_names = {r.get("name") for r in phase1_results if isinstance(r, dict)}
    missing = []
    if "新闻分析师" not in existing_names:
        missing.append(NewsAnalyst())
    if "市场情绪分析师" not in existing_names:
        missing.append(MarketSentimentAnalyst())
    if not missing:
        return phase1_results

    logger.info(f"Resume 模式：补充缺失市场上下文: {[a.name for a in missing]}")
    supplements = []
    with ThreadPoolExecutor(max_workers=len(missing)) as pool:
        futures = {pool.submit(a.run): a for a in missing}
        for future in as_completed(futures):
            analyst = futures[future]
            try:
                supplements.append(future.result())
            except Exception as e:
                logger.warning(f"Resume 市场上下文补充失败 {analyst.name}: {e}")
                supplements.append({"status": "error", "name": analyst.name, "error": str(e)})
    merged = phase1_results + supplements
    _save_phase1_context(merged)
    return merged

# ── 飞书推送 ──────────────────────────────────────────────
FEISHU_ENABLED = os.getenv("FEISHU_WEBHOOK_URL") is not None

def _feishu_response_success(resp) -> bool:
    body = (resp.text or "")[:500]
    if resp.status_code != 200:
        logger.warning(f"飞书推送失败[{resp.status_code}]: {body}")
        return False
    try:
        data = resp.json()
    except Exception:
        logger.warning(f"飞书推送失败: 响应不是JSON: {body}")
        return False
    code = data.get("code", data.get("StatusCode"))
    msg = data.get("msg", data.get("StatusMessage", ""))
    if code not in (0, "0"):
        logger.warning(f"飞书推送失败: code={code} msg={msg} body={body}")
        return False
    logger.info(f"飞书推送成功: code={code} msg={msg}")
    return True


def feishu_push_card(content: dict, webhook_url: str = None) -> bool:
    """发送飞书卡片消息"""
    if not webhook_url:
        webhook_url = os.getenv("FEISHU_WEBHOOK_URL")
    if not webhook_url:
        logger.warning("FEISHU_WEBHOOK_URL 未配置，飞书推送功能不可用")
        return False

    import requests
    try:
        payload = {"msg_type": "interactive", "card": content}
        r = requests.post(webhook_url, json=payload, timeout=15)
        return _feishu_response_success(r)
    except Exception as e:
        logger.warning(f"飞书推送异常: {e}")
        return False


def feishu_push_text(msg: str, webhook_url: str = None) -> bool:
    """发送飞书文本消息（兼容旧调用）"""
    if not webhook_url:
        webhook_url = os.getenv("FEISHU_WEBHOOK_URL")
    if not webhook_url:
        logger.warning("FEISHU_WEBHOOK_URL 未配置，飞书文本推送跳过")
        return False
    import requests
    try:
        payload = {"msg_type": "text", "content": {"text": msg}}
        r = requests.post(webhook_url, json=payload, timeout=10)
        return _feishu_response_success(r)
    except Exception as e:
        logger.warning(f"飞书推送异常: {e}")
        return False

# ── 历史行情辅助函数（mx-data优先，腾讯备用）────────────────
def _get_stock_hist_prices_mx(stock_code: str, days: int = 20) -> Optional[list]:
    """
    获取个股/指数历史收盘价
    主：QMT HTTP API（支持个股和指数，最快）
    备1：mx-data（东方财富妙想）
    备2：腾讯行情API
    备3：akshare
    返回: [price1, price2, ...] 最近的在前，或 None
    """
    # 防御性检查：跳过无效代码
    if not stock_code or stock_code in ("N/A", "", "None"):
        return None

    # 方案0：QMT HTTP API（支持个股+指数，最优先）
    try:
        import urllib.request, json
        # 指数代码后缀映射（6位数字代码容易和个股混淆）
        INDEX_SUFFIX = {"000001": ".SH", "399001": ".SZ", "399006": ".SZ",
                         "000300": ".SH", "000016": ".SH", "000905": ".SH"}
        suffix = INDEX_SUFFIX.get(stock_code) or (
            ".BJ" if stock_code.startswith(("920", "8", "4")) else
            ".SZ" if stock_code.startswith(("00", "30")) else
            ".SH"
        )
        url = f"http://127.0.0.1:8080/market_data3?stock={stock_code}{suffix}&period=1d&count={days * 2}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = json.loads(resp.read().decode())
        close_data = raw.get("data", {}).get("close", {})
        if close_data:
            # 按日期正序排列（最旧的在前）
            sorted_dates = sorted(close_data.keys(), reverse=False)
            prices = []
            for dt in sorted_dates:
                inner = close_data[dt]
                if isinstance(inner, dict):
                    vals = [v for v in inner.values() if v and float(v) > 0]
                    if vals:
                        prices.append(vals[0])
            if len(prices) >= min(days, 5):
                return prices[-days:] if len(prices) >= days else prices
    except Exception:
        pass

    # 方案1：mx-data
    try:
        suffix = (
            ".BJ" if stock_code.startswith(("920", "8", "4")) else
            ".SZ" if stock_code.startswith(("00", "30")) else
            ".SH"
        )
        cmd = [
            sys.executable,
            str(SKILLS_DIR / "mx-data" / "mx_data.py"),
            # 加后缀避免A+H股被识别为港股
            f"{stock_code}{suffix} 近{days}日收盘价",
        ]
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            env=domestic_subprocess_env(os.environ),
        )
        output = r.stdout
        if r.returncode == 0 and "错误" not in output and "上限" not in output:
            import re
            # 去掉价格中的货币单位（港元/元/美元等），再提取数字
            clean = re.sub(r'([\d.]+)\s*(港元|元|美元|人民币|新元|台币)', r'\1', output)
            # 去掉日期后的星期标识，如 "2026-05-08(日)" → "2026-05-08"
            clean = re.sub(r'(\d{4}-\d{2}-\d{2})\([日一二三四五六]\)', r'\1', clean)
            rows = re.findall(r'\|\s*(\d{4}-\d{2}-\d{2})\s*\|\s*([\d.]+)\s*\|', clean)
            if rows:
                prices = [float(p[1]) for p in rows]
                # 有至少5条数据即可计算MA，最少返回1条（数据不足时用已有数据）
                if len(prices) >= min(days, 5):
                    return prices[-days:] if len(prices) >= days else prices
    except Exception:
        pass

    # 方案2：腾讯行情API
    try:
        import urllib.request
        import json
        prefix = "sh" if stock_code.startswith(('6', '5', '9')) else "sz"
        url = (f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
               f"?_var=kline_dayhfq&param={prefix}{stock_code},day,,,{days},qfq")
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://gu.qq.com"
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
        raw = raw[raw.index("=") + 1:]
        obj = json.loads(raw)
        key = f"{prefix}{stock_code}"
        data = obj.get("data", {}).get(key, {}).get("qfqday", [])
        if data:
            return [float(row[2]) for row in data[-days:]]
    except Exception:
        pass

    # 方案3：akshare（最可靠的A股数据源，加重试防限流）
    try:
        import akshare as ak
        import pandas as pd
        today = date.today().strftime('%Y%m%d')
        # 用1年历史，确保有足够数据
        start_dt = pd.Timestamp(today) - pd.Timedelta(days=365)
        start = start_dt.strftime('%Y%m%d')
        for attempt in range(3):
            try:
                df = ak.stock_zh_a_hist(
                    symbol=stock_code, period='daily',
                    start_date=start, end_date=today, adjust='qfq'
                )
                if df is not None and len(df) >= 2:
                    closes = df['收盘'].tolist()
                    return closes[-days:]
                return None
            except Exception as e:
                if 'timeout' in str(e).lower() or 'aborted' in str(e).lower():
                    import time
                    time.sleep(2)
                    continue
                raise
    except Exception:
        pass

    return None


# ── 分析师基类 ────────────────────────────────────────────
class Analyst:
    """分析师基类"""
    name: str = "Base"
    color: str = "⚪"  # emoji标签

    def run(self) -> Dict[str, Any]:
        raise NotImplementedError

# ── Phase 1: 四大分析师 ────────────────────────────────────

class NewsAnalyst(Analyst):
    """新闻分析师 - akshare 东方财富实时新闻（含正文内容）"""
    name = "新闻分析师"
    color = "📰"

    def run(self) -> Dict[str, Any]:
        import akshare as ak
        start = time.time()
        try:
            news_items = []
            # 全市场新闻
            for keyword in ["A股", "市场"]:
                try:
                    df = retry_call(
                        f"akshare 新闻 {keyword}",
                        lambda: ak.stock_news_em(symbol=keyword),
                        retries=3,
                        base_delay=2,
                        throttle_key="akshare-news",
                        min_interval=1.0,
                    )
                    if df is not None and len(df) > 0:
                        for _, row in df.iterrows():
                            content = str(row.get("新闻内容", "") or "").strip()
                            if len(content) > 20:  # 只保留有实际内容的条目
                                news_items.append({
                                    "title": str(row.get("新闻标题", "")).strip(),
                                    "content": content,
                                    "source": str(row.get("文章来源", "") or "").strip(),
                                    "time": str(row.get("发布时间", "") or "").strip(),
                                })
                except Exception as e:
                    logger.warning(f"akshare news [{keyword}] 失败: {e}")

            elapsed = time.time() - start
            return {
                "status": "success" if news_items else "partial",
                "elapsed": elapsed,
                "color": self.color,
                "name": self.name,
                "findings": self._format(news_items),
                "raw": news_items,
            }
        except Exception as e:
            logger.error(f"NewsAnalyst 异常: {e}\n{traceback.format_exc()}")
            return {"status": "error", "elapsed": time.time()-start, "error": str(e)}

    def _format(self, items: List[Dict]) -> str:
        if not items:
            return "今日暂无重大财经新闻"
        lines = []
        for i, it in enumerate(items[:15], 1):  # 最多15条，每条含标题+正文摘要
            title = it.get("title", "")[:50]
            content = it.get("content", "")[:150]
            source = it.get("source", "")
            time_str = it.get("time", "")
            if content and len(content) > 20:
                lines.append(f"{i}. 【{title}】{content}")
                if source:
                    lines.append(f"   📍来源:{source} {time_str}")
        return "\n".join(lines) if lines else "今日暂无实质内容新闻"


class TechnicalAnalyst(Analyst):
    """技术分析师 - mx-data CLI"""
    name = "技术分析师"
    color = "📈"

    def run(self) -> Dict[str, Any]:
        start = time.time()
        try:
            # 通过 subprocess 调用 mx_data.py
            indices = ["000001", "399001", "399006"]
            index_data = {}

            # mx_data.py 默认输出到 /root/.openclaw/workspace/mx_data/output
            # 在 macOS 上这个路径不存在，手动解析脚本输出

            for code in indices:
                try:
                    # 优先用 QMT HTTP API（支持指数）
                    import urllib.request, json as _json
                    suffix = ".SZ" if code.startswith(("00", "30")) else ".SH"
                    url = f"http://127.0.0.1:8080/market_data3?stock={code}{suffix}&period=1d&count=20"
                    try:
                        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                        with urllib.request.urlopen(req, timeout=10) as resp:
                            raw = _json.loads(resp.read().decode())
                        close_data = raw.get("data", {}).get("close", {})
                        dates = sorted(close_data.keys(), reverse=True)
                        prices = [list(close_data[d].values())[0] for d in dates if close_data[d]]
                        if not prices:
                            raise ValueError("QMT返回空")
                        logger.info(f"  技术分析 {code} via QMT HTTP: {len(prices)}条")
                    except Exception:
                        # 降级到 mx-data
                        prices = _get_stock_hist_prices_mx(code, days=20)
                    if prices:
                        latest = prices[-1]
                        ma5 = sum(prices[-5:])/5 if len(prices)>=5 else None
                        ma10 = sum(prices[-10:])/10 if len(prices)>=10 else None
                        ma20 = sum(prices[-20:])/20 if len(prices)>=20 else None
                        trend = "多头" if (ma5 and ma10 and ma20 and ma5>ma10>ma20) else "震荡/空头"
                        index_data[code] = {
                            "latest": round(latest, 2),
                            "ma5": round(ma5, 2) if ma5 else None,
                            "ma10": round(ma10, 2) if ma10 else None,
                            "ma20": round(ma20, 2) if ma20 else None,
                            "trend": trend
                        }
                        _ma5 = f"{ma5:.2f}" if ma5 is not None else "N/A"
                        _ma10 = f"{ma10:.2f}" if ma10 is not None else "N/A"
                        _ma20 = f"{ma20:.2f}" if ma20 is not None else "N/A"
                        logger.info(f"  技术分析 {code}: MA5={_ma5}, MA10={_ma10}, MA20={_ma20}")
                    else:
                        logger.warning(f"技术分析 {code} 失败: 无法获取数据")
                except Exception as e:
                    logger.warning(f"技术分析 {code} 失败: {e}")

            elapsed = time.time() - start
            return {
                "status": "success",
                "elapsed": elapsed,
                "color": self.color,
                "name": self.name,
                "findings": self._summarize(index_data),
                "raw": index_data,
            }
        except Exception as e:
            logger.error(f"技术分析师异常: {e}\n{traceback.format_exc()}")
            return {"status": "error", "elapsed": time.time()-start, "error": str(e)}

    def _extract_prices(self, raw: Dict) -> List[float]:
        # 解析 mx_data.py 的 rawTable 结构
        # 路径: data.data.searchDataResultDTO.dataTableDTOList[0].rawTable
        try:
            inner = raw.get("data", {}).get("data", {})
            dto_list = inner.get("searchDataResultDTO", {}).get("dataTableDTOList", [])
            if not dto_list:
                return []
            raw_table = dto_list[0].get("rawTable", {})

            for k, v in raw_table.items():
                if k == "headName":
                    continue
                prices = [float(x) for x in v if x]

                logger.info(f"  提取价格: {len(prices)} 个数据点, 最新={prices[-1] if prices else None}")
                return prices
        except Exception as e:
            logger.warning(f"价格提取失败: {e}")
        return []

    def _summarize(self, data: Dict) -> str:
        if not data:
            return "未能获取技术数据"
        lines = []
        for code, info in data.items():
            lines.append(f"  {code}: 现价={info.get('latest')}, MA5={info.get('ma5')}, MA10={info.get('ma10')}, 趋势={info.get('trend')}")
        return "\n".join(lines)


class LLMWebSearchAnalyst(Analyst):
    """
    媒体舆情分析师 - 通过 mx-search 获取热点新闻、板块政策和风险事件。
    不再依赖独立 web_search，避免认证失败被误标为 success。
    """
    name = "媒体舆情分析师"
    color = "📰"

    QUERIES = [
        "今天A股市场热点新闻和主线题材",
        "今日A股热点板块和产业政策消息",
        "今日A股风险事件和市场情绪新闻",
    ]

    def run(self) -> Dict[str, Any]:
        start = time.time()
        raw_results = []
        errors = []
        try:
            self._load_local_env()
            mx = self._load_mx_search_client()
            items = []
            seen_titles = set()

            for query in self.QUERIES:
                try:
                    result = mx.search(query)
                    raw_results.append({"query": query, "result": result})
                    status = result.get("status")
                    if status != 0:
                        errors.append(f"{query}: status={status} message={result.get('message', '')}")
                        continue
                    for item in self._extract_items(result):
                        title = str(item.get("title") or "").strip()
                        if not title:
                            continue
                        key = re.sub(r"\s+", "", title)
                        if key in seen_titles:
                            continue
                        seen_titles.add(key)
                        item = dict(item)
                        item["query"] = query
                        items.append(item)
                        if len(items) >= 12:
                            break
                except Exception as e:
                    errors.append(f"{query}: {type(e).__name__}: {str(e)[:160]}")
                if len(items) >= 12:
                    break
                time.sleep(float(os.getenv("MEDIA_SENTIMENT_MX_SEARCH_INTERVAL_SEC", "1.2")))

            elapsed = time.time() - start
            if not items:
                err = "; ".join(errors) if errors else "mx-search 未返回有效新闻"
                logger.error(f"媒体舆情分析师 mx-search 失败: {err}")
                return {
                    "status": "error",
                    "elapsed": elapsed,
                    "color": self.color,
                    "name": self.name,
                    "error": err,
                    "raw": {"source": "mx-search", "queries": self.QUERIES, "errors": errors},
                }

            formatted_findings = self._format_findings(items)
            findings = _summarize_news_one_line(formatted_findings, max_len=360)
            if not findings or findings == "今日无重要舆情":
                findings = formatted_findings
            return {
                "status": "success",
                "elapsed": elapsed,
                "color": self.color,
                "name": self.name,
                "findings": findings,
                "raw": {
                    "source": "mx-search",
                    "queries": self.QUERIES,
                    "count": len(items),
                    "items": items,
                    "formatted_findings": formatted_findings,
                    "errors": errors,
                    "raw_results": raw_results,
                },
            }
        except Exception as e:
            logger.error(f"媒体舆情分析师异常: {e}\n{traceback.format_exc()}")
            return {"status": "error", "elapsed": time.time()-start, "error": str(e)}

    def _load_local_env(self) -> None:
        for env_file in (BASE_DIR / ".env", Path.home() / ".openclaw" / ".env"):
            try:
                if not env_file.exists():
                    continue
                for line in env_file.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    if key and key not in os.environ:
                        os.environ[key] = value
            except Exception as e:
                logger.warning(f"媒体舆情分析师读取.env失败 {env_file}: {e}")

    def _load_mx_search_client(self):
        import importlib.util as _importlib_util
        mx_file = SKILLS_DIR / "mx-search" / "mx_search.py"
        if not mx_file.exists():
            raise FileNotFoundError(f"mx_search.py not found: {mx_file}")
        spec = _importlib_util.spec_from_file_location("_openclaw_mx_search_runtime", mx_file)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load mx_search spec: {mx_file}")
        module = _importlib_util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.MXSearch(api_key=os.environ.get("MX_APIKEY", ""))

    @staticmethod
    def _extract_items(result: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = result.get("data") or {}
        inner = data.get("data") or {}
        search_response = inner.get("llmSearchResponse") or {}
        items = search_response.get("data") or []
        return [x for x in items if isinstance(x, dict)]

    @staticmethod
    def _format_findings(items: List[Dict[str, Any]]) -> str:
        lines = []
        for i, item in enumerate(items[:12], 1):
            title = str(item.get("title") or "无标题").strip()
            content = re.sub(r"\s+", " ", str(item.get("content") or "").strip())
            source = str(item.get("insName") or item.get("source") or "").strip()
            date_text = str(item.get("date") or "").strip()[:19]
            lines.append(f"{i}. {title}")
            meta = " ".join(x for x in [source, date_text] if x)
            if meta:
                lines.append(f"   📍来源:{meta}")
            if content:
                lines.append(f"   {content[:180]}")
        return "\n".join(lines) if lines else "今日无重要舆情"


class FundamentalAnalyst(Analyst):
    name = "基本面分析师"
    color = "📊"

    def run(self) -> Dict[str, Any]:
        start = time.time()
        try:
            findings_lines = []
            raw_data = {}

            # 查询大盘整体估值
            index_queries = [
                ("上证指数 市盈率", "上证PE"),
                ("上证指数 市净率", "上证PB"),
                ("深证成指 市盈率", "深证PE"),
            ]

            for query, label in index_queries:
                try:
                    cmd = [
                        sys.executable,
                        str(SKILLS_DIR / "mx-data" / "mx_data.py"),
                        query,
                    ]
                    r = subprocess.run(cmd, capture_output=True, text=True, timeout=25)
                    if r.returncode == 0:
                        val = self._parse_pe_pb(r.stdout)
                        if val:
                            findings_lines.append(f"  {label}: {val}")
                            raw_data[label] = val
                except Exception as e:
                    logger.warning(f"  基本面 [{label}] 异常: {e}")

            elapsed = time.time() - start
            if findings_lines:
                findings = "\n".join(findings_lines)
            else:
                findings = "基本面数据暂时无法获取，建议关注估值合理板块"

            return {
                "status": "success",
                "elapsed": elapsed,
                "color": self.color,
                "name": self.name,
                "findings": findings,
                "raw": raw_data,
            }
        except Exception as e:
            logger.error(f"基本面分析师异常: {e}\n{traceback.format_exc()}")
            return {"status": "error", "elapsed": time.time()-start, "error": str(e)}

    def _parse_pe_pb(self, stdout: str) -> Optional[str]:
        numbers = re.findall(r"\d+\.\d+", stdout)
        if numbers:
            return numbers[-1]
        return None


class SentimentAnalyst(Analyst):
    """情绪分析师 - 通过 mx-search 搜索情绪指标"""
    name = "情绪分析师"
    color = "🌡️"

    def run(self) -> Dict[str, Any]:
        start = time.time()
        try:
            bullish_queries = [
                "今日北向资金净流入",
                "今日涨停数量",
                "今日主力净流入板块",
            ]
            bearish_queries = [
                "今日北向资金净流出",
                "今日主力净流出板块",
            ]

            bullish_counts = []
            bearish_counts = []

            for q in bullish_queries:
                try:
                    cmd = [
                        sys.executable,
                        str(SKILLS_DIR / "mx-search" / "mx_search.py"),
                        q,
                        str(OUTPUT_DIR / "mx_search"),
                    ]
                    r = subprocess.run(cmd, capture_output=True, text=True, timeout=25)
                    if r.returncode == 0:
                        # 从输出JSON中提取结果数量
                        import re
                        json_file = OUTPUT_DIR / "mx_search" / f"mx_search_{q}.json"
                        if json_file.exists():
                            with open(json_file) as f2:
                                data = json.load(f2)
                            news_list = data.get("data", {}).get("llmSearchResponse", {}).get("data", []) if isinstance(data, dict) else []
                            bullish_counts.append(len(news_list))
                        else:
                            # 从stdout粗略提取
                            count = len(re.findall(r'找到了.*?条', r.stdout))
                            bullish_counts.append(max(count, 1))
                    else:
                        bullish_counts.append(0)
                except Exception as e:
                    logger.warning(f"情绪查询 [{q}] 异常: {e}")
                    bullish_counts.append(0)

            for q in bearish_queries:
                try:
                    cmd = [
                        sys.executable,
                        str(SKILLS_DIR / "mx-search" / "mx_search.py"),
                        q,
                        str(OUTPUT_DIR / "mx_search"),
                    ]
                    r = subprocess.run(cmd, capture_output=True, text=True, timeout=25)
                    if r.returncode == 0:
                        import re
                        json_file = OUTPUT_DIR / "mx_search" / f"mx_search_{q}.json"
                        if json_file.exists():
                            with open(json_file) as f2:
                                data = json.load(f2)
                            news_list = data.get("data", {}).get("llmSearchResponse", {}).get("data", []) if isinstance(data, dict) else []
                            bearish_counts.append(len(news_list))
                        else:
                            count = len(re.findall(r'找到了.*?条', r.stdout))
                            bearish_counts.append(max(count, 1))
                    else:
                        bearish_counts.append(0)
                except Exception as e:
                    logger.warning(f"情绪查询 [{q}] 异常: {e}")
                    bearish_counts.append(0)

            total_bullish = sum(bullish_counts)
            total_bearish = sum(bearish_counts)
            total = total_bullish + total_bearish

            if total == 0:
                sentiment = "中性"
                verdict = "多空信号均不明显"
            elif total_bullish > total_bearish * 1.5:
                sentiment = "偏多"
                verdict = f"做多信号强({total_bullish} vs {total_bearish})"
            elif total_bearish > total_bullish * 1.5:
                sentiment = "偏空"
                verdict = f"做空信号强({total_bearish} vs {total_bullish})"
            else:
                sentiment = "分歧"
                verdict = f"多空均衡({total_bullish} vs {total_bearish})"

            elapsed = time.time() - start
            findings = (
                f"市场情绪: {sentiment} | {verdict}\n"
                f"  利好信号数量: {total_bullish}\n"
                f"  利空信号数量: {total_bearish}"
            )
            return {
                "status": "success",
                "elapsed": elapsed,
                "color": self.color,
                "name": self.name,
                "findings": findings,
                "raw": {
                    "bullish": total_bullish,
                    "bearish": total_bearish,
                    "sentiment": sentiment,
                },
            }
        except Exception as e:
            logger.error(f"情绪分析师异常: {e}\n{traceback.format_exc()}")
            return {"status": "error", "elapsed": time.time()-start, "error": str(e)}


class MarketSentimentAnalyst(Analyst):
    """市场情绪分析师 - akshare(主：涨停/跌停/炸板/连板) + mx-data(备用)"""
    name = "市场情绪分析师"
    color = "🌡️"

    def run(self) -> Dict[str, Any]:
        start = time.time()
        try:
            limit_up_count = None
            limit_down_count = None
            breakout_rate = None  # 炸板率
            continuous_height = None  # 连板高度

            # ── Step 1: akshare 主查涨停/跌停/炸板/连板 ─────────
            try:
                import akshare as ak
                import pandas as pd
                # 取最近交易日（今天如果是周六/周日，取上周五）
                today_dt = pd.Timestamp.today()
                if today_dt.dayofweek >= 5:  # 周六/周日
                    trade_date = (today_dt - pd.Timedelta(days=today_dt.dayofweek - 4)).strftime('%Y%m%d')
                else:
                    trade_date = today_dt.strftime('%Y%m%d')
                # 涨停池
                df_up = retry_call(
                    "akshare 涨停池",
                    lambda: ak.stock_zt_pool_em(date=trade_date),
                    retries=3,
                    base_delay=2,
                    throttle_key="akshare-zt-pool",
                    min_interval=1.0,
                )
                if df_up is not None and len(df_up) > 0:
                    limit_up_count = len(df_up)
                    broke_count = (df_up["炸板次数"] > 0).sum() if "炸板次数" in df_up.columns else 0
                    breakout_rate = (broke_count / limit_up_count * 100) if limit_up_count > 0 else 0
                    continuous_height = int(df_up["连板数"].max()) if "连板数" in df_up.columns else 0
                # 跌停池
                df_dt = retry_call(
                    "akshare 跌停池",
                    lambda: ak.stock_zt_pool_dtgc_em(date=trade_date),
                    retries=3,
                    base_delay=2,
                    throttle_key="akshare-dtgc-pool",
                    min_interval=1.0,
                )
                if df_dt is not None and len(df_dt) > 0:
                    limit_down_count = len(df_dt)
            except Exception as e:
                logger.warning(f"MarketSentiment akshare 异常: {e}")

            # ── Step 2: mx-data 备用（涨停/跌停家数）───────────
            if limit_up_count is None or limit_down_count is None:
                for q, key in [("今日A股涨停家数", "limit_up"), ("今日A股跌停家数", "limit_down")]:
                    try:
                        cmd = [
                            sys.executable,
                            str(SKILLS_DIR / "mx-data" / "mx_data.py"),
                            q,
                            str(OUTPUT_DIR / "mx_data"),
                        ]
                        r = subprocess.run(
                            cmd,
                            capture_output=True,
                            text=True,
                            timeout=40,
                            env=domestic_subprocess_env(os.environ),
                        )
                        if r.returncode == 0 and "error" not in r.stdout.lower():
                            json_file = OUTPUT_DIR / "mx_data" / f"mx_data_{q}.json"
                            if json_file.exists():
                                with open(json_file) as f:
                                    data = json.load(f)
                                rows = data.get("data", {}).get("dataTableDTOList", [{}])
                                if rows:
                                    table = rows[0].get("table", {})
                                    values = list(table.values())[0] if table else []
                                    val = values[0] if values else None
                                    if key == "limit_up" and limit_up_count is None:
                                        limit_up_count = int(val) if val is not None else None
                                    elif key == "limit_down" and limit_down_count is None:
                                        limit_down_count = int(val) if val is not None else None
                    except Exception as e:
                        logger.warning(f"MarketSentiment mx-data [{q}] 异常: {e}")

            # ── Step 3: 判断情绪阶段 ─────────────────────────
            phase = "未知"
            recommendation = ""
            clues = []

            if limit_up_count is not None:
                clues.append(f"涨停{limit_up_count}家")
            if limit_down_count is not None:
                clues.append(f"跌停{limit_down_count}家")
            if breakout_rate is not None:
                clues.append(f"炸板率{breakout_rate:.1f}%")
            if continuous_height is not None:
                clues.append(f"连板高度{continuous_height}板")

            # 亢奋：涨停>80 + 炸板率<15% + 连板>5
            if (limit_up_count or 0) > 80 and (breakout_rate or 50) < 15 and (continuous_height or 0) > 5:
                phase = "亢奋"
                recommendation = "重仓主线龙头"
            # 冰点：涨停<20 + 跌停>10 + 连板<3
            elif (limit_up_count or 100) < 20 and (limit_down_count or 0) > 10 and (continuous_height or 10) < 3:
                phase = "冰点"
                recommendation = "轻仓或空仓"
            # 分歧：涨停波动大 or 炸板率>25%
            elif (breakout_rate or 0) > 25:
                phase = "分歧"
                recommendation = "半仓参与辨识度高标的"
            # 修复：涨停20-80 之间，且不是明显分歧
            elif (limit_up_count or 0) >= 20 and (limit_up_count or 0) <= 80:
                phase = "修复"
                recommendation = "正常仓位，精选个股"
            else:
                phase = "中性"
                recommendation = "观察为主"

            elapsed = time.time() - start
            findings = (
                f"市场情绪: **{phase}** | {recommendation}\n"
                f"  {' | '.join(clues)}"
            )
            return {
                "status": "success",
                "elapsed": elapsed,
                "color": self.color,
                "name": self.name,
                "findings": findings,
                "raw": {
                    "limit_up_count": limit_up_count,
                    "limit_down_count": limit_down_count,
                    "breakout_rate": breakout_rate,
                    "continuous_height": continuous_height,
                    "phase": phase,
                },
            }
        except Exception as e:
            logger.error(f"情绪分析师异常: {e}\n{traceback.format_exc()}")
            return {"status": "error", "elapsed": time.time()-start, "error": str(e)}

            phase = "未知"
            recommendation = ""
            clues = []

            if limit_up_count is not None:
                clues.append(f"涨停{limit_up_count}家")
            if limit_down_count is not None:
                clues.append(f"跌停{limit_down_count}家")
            if breakout_rate is not None:
                clues.append(f"炸板率{breakout_rate:.1f}%")
            if continuous_height is not None:
                clues.append(f"连板高度{continuous_height}板")

            # 亢奋：涨停>80 + 炸板率<15% + 连板>5
            if (limit_up_count or 0) > 80 and (breakout_rate or 50) < 15 and (continuous_height or 0) > 5:
                phase = "亢奋"
                recommendation = "重仓主线龙头"
            # 冰点：涨停<20 + 跌停>10 + 连板<3
            elif (limit_up_count or 100) < 20 and (limit_down_count or 0) > 10 and (continuous_height or 10) < 3:
                phase = "冰点"
                recommendation = "轻仓或空仓"
            # 分歧：涨停波动大 or 炸板率>25%
            elif (breakout_rate or 0) > 25:
                phase = "分歧"
                recommendation = "半仓参与辨识度高标的"
            # 修复：涨停20-80 之间，且不是明显分歧
            elif (limit_up_count or 0) >= 20 and (limit_up_count or 0) <= 80:
                phase = "修复"
                recommendation = "正常仓位，精选个股"
            else:
                phase = "中性"
                recommendation = "观察为主"

            elapsed = time.time() - start
            findings = (
                f"市场情绪: **{phase}** | {recommendation}\n"
                f"  {' | '.join(clues)}"
            )
            return {
                "status": "success",
                "elapsed": elapsed,
                "color": self.color,
                "name": self.name,
                "findings": findings,
                "raw": {
                    "phase": phase,
                    "recommendation": recommendation,
                    "limit_up_count": limit_up_count,
                    "limit_down_count": limit_down_count,
                    "breakout_rate": float(breakout_rate) if breakout_rate is not None else None,
                    "continuous_height": continuous_height,
                },
            }
        except Exception as e:
            logger.error(f"市场情绪分析师异常: {e}\n{traceback.format_exc()}")
            return {"status": "error", "elapsed": time.time()-start, "error": str(e)}


class ZtGeneAnalyst(Analyst):
    """涨停基因分析师 - 通过akshare获取今日涨停/强势股数据，提取涨停基因"""
    name = "涨停基因分析师"
    color = "🔥"

    def run(self) -> Dict[str, Any]:
        import akshare as ak
        from datetime import timedelta
        start = time.time()
        # 用上一交易日（akshare接口在非交易日返回空）
        today = date.today()
        wd = today.weekday()
        if wd == 0:  # 周一 → 上周五
            prev_td = today - timedelta(days=3)
        elif wd >= 5:  # 周六/周日 → 上周五
            prev_td = today - timedelta(days=wd - 4)  # 周六-4=2, 周日-4=1
        else:  # 周二～周五 → 减1天
            prev_td = today - timedelta(days=1)
        today_str = prev_td.strftime('%Y%m%d')
        try:
            # 涨停股池
            try:
                zt_df = ak.stock_zt_pool_em(date=today_str)
                zt_stocks = zt_df[zt_df['涨跌幅'] >= 9.9] if zt_df is not None else None
                zt_count = len(zt_stocks) if zt_stocks is not None else 0
            except Exception as e:
                logger.warning(f"涨停池获取失败: {e}")
                zt_count = 0
                zt_stocks = None

            # 炸板股池
            try:
                zbgc_df = ak.stock_zt_pool_zbgc_em(date=today_str)
                zbgc_count = len(zbgc_df) if zbgc_df is not None else 0
            except Exception:
                zbgc_count = 0

            # 强势股池
            try:
                strong_df = ak.stock_zt_pool_strong_em(date=today_str)
                strong_count = len(strong_df) if strong_df is not None else 0
                # 提取成交额最大的前5只强势股
                if strong_df is not None and len(strong_df) > 0:
                    top_strong = strong_df.nlargest(5, '成交额')[['代码', '名称', '涨跌幅', '成交额', '换手率']].to_dict('records')
                else:
                    top_strong = []
            except Exception as e:
                logger.warning(f"强势股获取失败: {e}")
                strong_count = 0
                top_strong = []

            # 涨停股中成交额最大的前5只（最强势）
            hot_zt = []
            if zt_stocks is not None and len(zt_stocks) > 0:
                hot_zt = zt_stocks.nlargest(5, '成交额')[['代码', '名称', '涨跌幅', '成交额']].to_dict('records')

            elapsed = time.time() - start

            # 格式化 findings
            lines = []
            lines.append(f"今日涨停: {zt_count} 只 | 炸板: {zbgc_count} 只 | 强势股: {strong_count} 只")
            if zt_count > 0:
                lines.append(f"\n🔥 最强势涨停TOP5（按成交额）:")
                for i, s in enumerate(hot_zt, 1):
                    code = str(s.get('代码', ''))
                    name = str(s.get('名称', ''))
                    chg = s.get('涨跌幅', 0)
                    amount = s.get('成交额', 0)
                    amount_str = f"{amount/1e8:.1f}亿" if amount else "-"
                    lines.append(f"  {i}. {code} {name} 涨{chg:.2f}% 成交{amount_str}")
            if strong_count > 0:
                lines.append(f"\n📈 强势股TOP5（按成交额）:")
                for i, s in enumerate(top_strong, 1):
                    code = str(s.get('代码', ''))
                    name = str(s.get('名称', ''))
                    chg = s.get('涨跌幅', 0)
                    hx = s.get('换手率', 0)
                    amount = s.get('成交额', 0)
                    amount_str = f"{amount/1e8:.1f}亿" if amount else "-"
                    lines.append(f"  {i}. {code} {name} 涨{chg:.2f}% 换手{hx:.1f}% 成交{amount_str}")

            findings = '\n'.join(lines) if lines else "今日涨停数据获取失败"
            return {
                "status": "success",
                "elapsed": elapsed,
                "color": self.color,
                "name": self.name,
                "findings": findings,
                "raw": {
                    "zt_count": zt_count,
                    "zbgc_count": zbgc_count,
                    "strong_count": strong_count,
                    "hot_zt": hot_zt,
                    "top_strong": top_strong,
                },
            }
        except Exception as e:
            logger.error(f"涨停基因分析师异常: {e}\n{traceback.format_exc()}")
            return {"status": "error", "elapsed": time.time()-start, "error": str(e)}


# ── Phase 2: Route B - LLM 智能选股（主力路径）───────────────

def route_b_phase2(analyst_results: List[Dict[str, Any]], gen=None, dry_run: bool = False, model: str = "volcengine-plan/ark-code-latest") -> Dict[str, Any]:
    """Route B: 候选股票生成 → LLM打分 → 排序输出（主力路径）"""
    from llm_scorer import CandidateGenerator, LLMScorer

    successful = [r for r in analyst_results if r.get("status") == "success"]
    logger.info(f"Route B: {len(successful)} 个分析师成功，尝试LLM打分...")

    news_output = next((r for r in successful if r["name"] == "新闻分析师"), {})
    tech_output = next((r for r in successful if r["name"] == "技术分析师"), {})

    # 复用 Phase 1 预热结果：优先用已缓存的候选股列表，不重复跑 xuangu
    if gen is not None and hasattr(gen, 'candidates') and gen.candidates:
        xuangu_candidates = gen.candidates
        logger.info(f"Route B 复用 Phase 1 预热候选股: {len(xuangu_candidates)} 只")
    else:
        logger.warning("Phase 1 预热结果不可用，降级为重新跑 xuangu")
        gen = CandidateGenerator(SKILLS_DIR, OUTPUT_DIR)
        xuangu_candidates = gen._run_xuangu_screening()

    # 新闻候选
    news_candidates = gen._extract_from_news(news_output)
    tech_candidates = gen._extract_from_tech(tech_output)

    # 合并去重
    seen = set()
    candidates = []
    for c in list(news_candidates) + list(xuangu_candidates) + list(tech_candidates):
        if c["stock"] not in seen:
            seen.add(c["stock"])
            candidates.append(c)

    if not candidates:
        raise RuntimeError("候选股票池为空")

    logger.info(f"Route B 候选股池: {len(candidates)} 只")

    if dry_run:
        logger.info("[DRY-RUN] 跳过 LLM 实时打分")
        scored = [
            {**c, "news_score": 50, "tech_score": 50,
             "fundamental_score": 50, "sentiment_score": 50,
             "total_score": 50, "reason": c.get("reason", ""), "action": "WATCH", "route": "B"}
            for c in candidates[:10]
        ]
    else:

        # ── 修复：注入财务和技术数据，绕过 route_b_phase2 不走 generate() 的问题 ──
        fin_file = OUTPUT_DIR / "fundamental_cache" / "all_stocks_financial.json"
        if fin_file.exists():
            try:
                fin_data = json.loads(fin_file.read_text(encoding="utf-8")) or {}
                records = fin_data.get("data", fin_data) if isinstance(fin_data, dict) else {}
                injected = 0
                for c in candidates:
                    stock = c.get("stock", "")
                    if stock in records and records[stock].get("roe_annual_latest") is not None:
                        c["_financial"] = records[stock]
                        c["fundamental"] = _build_financial_str(records[stock])
                        injected += 1
                logger.info(f"财务数据已注入 {injected}/{len(candidates)} 只候选股")
            except Exception as e:
                logger.warning(f"财务数据注入失败: {e}")

        # ★ 72 只 all_stocks_tech.json 孤儿缓存注入块已删除（xqshare 实时拉取代）
        # ───────────────────────────────────────────────────────────────────────────

        logger.info("Route B LLM 多因子打分中...")
        scorer = LLMScorer(timeout=300, output_dir=OUTPUT_DIR, model=model)
        scored, route_a_pending = scorer.score_candidates(successful, candidates)
        # scoring_method 已在 score_candidates() 中标记 llm_one_by_one / llm_pending_route_a

        # ★ 全量技术面预取（不管 Route A 还是 Route B，62 只全走 xqshare 实时）
        from concurrent.futures import ThreadPoolExecutor, as_completed
        tech_cache: Dict[str, Dict] = {}
        all_stocks_to_fetch = list({c["stock"] for c in candidates if c.get("stock")})
        logger.info(f"全量技术面预取开始: {len(all_stocks_to_fetch)} 只（xqshare 优先）")
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(_fetch_stock_tech, s): s for s in all_stocks_to_fetch}
            for future in as_completed(futures):
                stock = futures[future]
                try:
                    result = future.result(timeout=15)
                    if result:
                        tech_cache[stock] = result
                except Exception:
                    pass
        logger.info(f"全量技术面预取完成: 成功 {len(tech_cache)}/{len(all_stocks_to_fetch)}")
        # ★ 回写 _technical 到 scored 和 candidates
        for c in scored:
            stock = c.get("stock", "")
            if stock in tech_cache:
                c["_technical"] = tech_cache[stock]
        for c in candidates:
            stock = c.get("stock", "")
            if stock in tech_cache:
                c["_technical"] = tech_cache[stock]

        # LLM失败的候选股，由Route A规则补打分
        if route_a_pending:
            logger.info(f"Route B 中 {len(route_a_pending)} 只股票LLM失败，交由Route A规则补打分...")
            pending_stocks = [c["stock"] for c in route_a_pending]
            # Route A 走纯规则打分，复用上预取的 tech_cache
            # Route A打分
            for c in route_a_pending:
                stock = c["stock"]
                rule_score = _apply_rule_score(c, news_output, tech_cache)
                action = "BUY" if rule_score >= 65 else "WATCH" if rule_score >= 40 else "AVOID"
                route_a_result = {
                    **c,
                    "news_score": rule_score,
                    "tech_score": rule_score,
                    "fundamental_score": rule_score,
                    "sentiment_score": rule_score,
                    "total_score": rule_score,
                    "action": action,
                    "scoring_method": "route_a",
                    "reason": c.get("reason", "") + " [Route A规则补打]",
                }
                # 替换scored中的占位条目
                for idx, s in enumerate(scored):
                    if s["stock"] == stock and s.get("_route_a_pending"):
                        scored[idx] = route_a_result
                        break

    scored.sort(key=lambda x: x.get("adjusted_score", x.get("total_score", 0)), reverse=True)
    top_picks = scored[:5]

    logger.info(f"Route B 完成: Top {len(top_picks)} -> {[p['stock'] for p in top_picks]}")
    # _push_phase2_feishu(scored, top_picks)  # 已在 run_daily_workflow 末尾发送完整卡片，此处省略

    return {
        "candidates": candidates, "scored": scored, "top_picks": top_picks,
        "all_analysts": successful, "timestamp": datetime.now().isoformat(),
        "phase": "route_b_complete",
    }


# ── Phase 2: 选股辩论（替代原有 LLM 打分）─────────────────────

def run_phase2_debate(analyst_results: List[Dict[str, Any]], gen=None, dry_run: bool = False,
                       model: str = "volcengine-plan/ark-code-latest", resume: bool = False) -> Dict[str, Any]:
    """
    Phase 2: 完全版辩论
    - 复用 Phase 1 预热的候选股列表（gen.candidates）
    - xqshare 获取每只候选股的 K 线数据
    - 知识库注入（蜡烛图 + 趋势技术 + Murphy）
    - 四角色独立评估 + 矛盾识别 + 交叉辩论 + 裁判判决
    - 输出：ranked_candidates → top_picks
    """
    from stock_selection_debate import StockDebateEngine
    from stock_selection_debate.data_fetcher import (
        get_kline_via_mx_data, get_kline_via_tencent, build_debate_packet, load_phase1_cache
    )

    def _get_prev_trading_date():
        """通过 xtquant 获取上一交易日（今天之前的最近交易日）"""
        try:
            import xtquant as xq
            today = date.today()
            dates = xq.get_trading_dates("SH", "SH", max(1, today.toordinal() - 365), today.toordinal())
            prev = [d for d in dates if d < today]
            return prev[-1].strftime("%Y%m%d") if prev else None
        except Exception:
            from datetime import timedelta
            today = date.today()
            wd = today.weekday()
            if wd == 0:
                return (today - timedelta(days=3)).strftime("%Y%m%d")
            else:
                return (today - timedelta(days=1)).strftime("%Y%m%d")

    def _expected_completed_kline_date(prev_td: Optional[str]) -> Optional[str]:
        """Return the completed daily K-line date expected from local cache.

        The workflow usually runs before market open, where the latest completed
        daily bar is the previous trading day. During manual after-close resume,
        local QMT/XQShare may already contain today's completed daily bar; accept
        it after the configured close threshold to avoid unnecessary akshare
        fallback storms.
        """
        today_key = date.today().strftime("%Y%m%d")
        after_close_at = os.getenv("DAILY_KLINE_ACCEPT_TODAY_AFTER", "15:10").replace(":", "")
        now_hm = datetime.now().strftime("%H%M")
        after_close = now_hm >= after_close_at
        today_is_trading = date.today().weekday() < 5
        try:
            shared_path = os.path.expanduser("~/.openclaw/agents/shared")
            if shared_path not in sys.path:
                sys.path.insert(0, shared_path)
            from trading_calendar import is_a_share_trading_day
            today_is_trading = bool(is_a_share_trading_day())
        except Exception:
            pass
        if today_is_trading and after_close:
            return today_key
        return prev_td

    def _ensure_suffix(code: str) -> str:
        if code.endswith((".SZ", ".SH", ".BJ")):
            return code
        if code.startswith(("920", "8", "4")):
            suffix = ".BJ"
        elif code.startswith(("00", "30", "002", "003")):
            suffix = ".SZ"
        else:
            suffix = ".SH"
        return code + suffix

    def _parse_http_kline(data: dict) -> list:
        """
        解析 QMT HTTP API 返回的 K线数据。
        格式: {"close": {"20260429": {"000547.SZ": 24.06}, ...},
               "open":  {...}, "high": {...}, "low": {...}, "volume": {...}}
        每日的值是 {stock_code: price} 的嵌套 dict。
        """
        close_data = data.get("close", {})
        open_data = data.get("open", {})
        high_data = data.get("high", {})
        low_data = data.get("low", {})
        volume_data = data.get("volume", {})
        # 合并所有日期
        all_dates = sorted(set().union(*[
            d.keys() for d in [close_data, open_data, high_data, low_data, volume_data] if d
        ]))
        records = []
        for dt in all_dates:
            def extract(d, date_str):
                """从 {"20260429": {"000547.SZ": 24.06}} 提取价格"""
                inner = d.get(date_str, {})
                if inner and isinstance(inner, dict):
                    vals = list(inner.values())
                    return vals[0] if vals else 0.0
                return 0.0
            records.append({
                "date": dt,
                "open": extract(open_data, dt),
                "high": extract(high_data, dt),
                "low": extract(low_data, dt),
                "close": extract(close_data, dt),
                "volume": extract(volume_data, dt),
            })
        return records

    def fetch_kline(stock):
        """
        K线获取策略：
        1. QMT HTTP 获取本地缓存 → 检查最新日期是否 = 上一交易日
        2. 若非最新 → akshare 兜底
        3. mx-data / 腾讯行情最终兜底
        """
        prev_td = _get_prev_trading_date()
        expected_td = _expected_completed_kline_date(prev_td)
        today_str = date.today().strftime("%Y%m%d")
        logger.info(f"K线期望日期: {expected_td} (上一交易日={prev_td}, 当前={today_str})")

        def _retry(label, fn, retries=3, delay=1.5):
            last_err = None
            for attempt in range(retries):
                try:
                    return fn()
                except Exception as e:
                    last_err = e
                    if attempt < retries - 1:
                        logger.warning(f"{label} 第{attempt + 1}次失败，重试: {e}")
                        time.sleep(delay * (attempt + 1))
                    else:
                        raise last_err

        # Step 1: QMT HTTP 获取本地缓存（20条）→ 检查是否最新
        try:
            import urllib.request, json
            full_code = _ensure_suffix(stock)
            url = f"http://127.0.0.1:8080/market_data3?stock={full_code}&period=1d&count=20"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with _retry(f"K线(HTTP) {stock}", lambda: urllib.request.urlopen(req, timeout=15)) as resp:
                raw = resp.read().decode("utf-8")
            data = json.loads(raw)
            if data.get("success") and data.get("data"):
                close_data = data["data"].get("close", {})
                if close_data:
                    latest_date = sorted(close_data.keys())[-1].replace("-", "")
                    logger.info(f"K线(HTTP) {stock} 本地最新={latest_date}, 期望={expected_td}")
                    if latest_date == expected_td:
                        # 本地已是最新，获取完整120条
                        url120 = f"http://127.0.0.1:8080/market_data3?stock={full_code}&period=1d&count=120"
                        req120 = urllib.request.Request(url120, headers={"User-Agent": "Mozilla/5.0"})
                        with _retry(f"K线(HTTP120) {stock}", lambda: urllib.request.urlopen(req120, timeout=15)) as resp:
                            raw120 = resp.read().decode("utf-8")
                        data120 = json.loads(raw120)
                        if data120.get("success") and data120.get("data"):
                            records = _parse_http_kline(data120["data"])
                            if records:
                                logger.info(f"K线(HTTP缓存最新) {stock}: {len(records)} 条")
                                return records
                    else:
                        logger.info(f"K线(HTTP) {stock} 本地={latest_date} ≠ 期望={expected_td}，尝试akshare...")
        except Exception as e:
            logger.info(f"K线(HTTP) {stock} 失败: {e}，尝试akshare...")

        # Step 2: akshare 兜底
        try:
            import akshare as ak
            import pandas as pd
            today_str = date.today().strftime("%Y%m%d")
            start_dt = pd.Timestamp.today() - pd.Timedelta(days=180)
            start_str = start_dt.strftime("%Y%m%d")
            df = _retry(
                f"K线(akshare) {stock}",
                lambda: ak.stock_zh_a_hist(symbol=stock, period="daily",
                                           start_date=start_str, end_date=today_str, adjust="qfq"),
            )
            if df is not None and len(df) >= 2:
                records = []
                for _, row in df.iterrows():
                    records.append({
                        "date": str(row["日期"])[:10],
                        "open": float(row["开盘"]), "high": float(row["最高"]),
                        "low": float(row["最低"]), "close": float(row["收盘"]),
                        "volume": float(row["成交量"]),
                    })
                if records:
                    logger.info(f"K线(akshare) {stock}: {len(records)} 条")
                    return records
        except Exception as e:
            logger.warning(f"K线(akshare) {stock} 失败: {e}")

        # Step 4: mx-data 兜底（额度可用时补历史收盘；成交量可能不足，但好过空K线）
        try:
            records = _retry(f"K线(mx-data) {stock}", lambda: get_kline_via_mx_data(stock, 120), retries=2)
            if records and len(records) >= 5:
                logger.info(f"K线(mx-data) {stock}: {len(records)} 条")
                return records
        except Exception as e:
            logger.warning(f"K线(mx-data) {stock} 失败: {e}")

        # Step 5: 腾讯行情最终兜底
        try:
            records = _retry(f"K线(Tencent) {stock}", lambda: get_kline_via_tencent(stock, 120), retries=3)
            if records and len(records) >= 5:
                logger.info(f"K线(Tencent) {stock}: {len(records)} 条")
                return records
        except Exception as e:
            logger.warning(f"K线(Tencent) {stock} 失败: {e}")

        logger.warning(f"K线获取失败: {stock}")
        return []

    logger.info("Phase 2: 选股辩论开始...")

    # 如果所有候选股已在 checkpoint 中完成，跳过 K 线获取，直接用 checkpoint 结果
    cp = {"date": "", "completed": [], "failed": [], "results": {}, "candidates": [], "screening_signature": "", "candidate_signature": ""}
    cp_file = OUTPUT_DIR / "debate_checkpoint.json"
    cp_lock_file = cp_file.with_name(cp_file.name + ".lock")
    if cp_file.exists():
        if not _is_today_file(cp_file):
            logger.info(f"辩论 checkpoint 非今日创建，删除重建: {cp_file}")
            try:
                cp_file.unlink(missing_ok=True)
            except Exception:
                pass
        else:
            try:
                with open(cp_file) as f:
                    cp = json.load(f)
                if cp.get("date") != date.today().strftime("%Y%m%d"):
                    cp = {"date": date.today().strftime("%Y%m%d"), "completed": [], "failed": [], "results": {}, "candidates": [], "screening_signature": "", "candidate_signature": ""}
            except:
                cp = {"date": date.today().strftime("%Y%m%d"), "completed": [], "failed": [], "results": {}, "candidates": [], "screening_signature": "", "candidate_signature": ""}
    done_set = set(cp["completed"])

    resumed_report_signature = ""

    def _load_resume_candidates_from_report() -> List[Dict[str, Any]]:
        report_file = OUTPUT_DIR / f"daily_report_{date.today().strftime('%Y%m%d')}.json"
        if not report_file.exists():
            return []
        try:
            report = json.loads(report_file.read_text(encoding="utf-8"))
            phase2 = report.get("phase2") or {}
            report_candidates = phase2.get("candidates") or []
            if report_candidates:
                nonlocal resumed_report_signature
                resumed_report_signature = phase2.get("screening_signature") or ""
                logger.info(f"Resume 模式：从今日报告恢复候选池 {len(report_candidates)} 只")
                return report_candidates
        except Exception as e:
            logger.warning(f"Resume 候选池恢复失败: {e}")
        return []

    def _load_resume_candidates_from_checkpoint() -> List[Dict[str, Any]]:
        # 兜底：当今日报告还没生成时，从 debate_checkpoint.json 重建候选池
        # 适用于工作流在报告生成前被中断、后续 resume 续跑的场景
        try:
            cp_candidates = cp.get("candidates") or []
            if isinstance(cp_candidates, list) and cp_candidates:
                logger.info(f"Resume 模式：从 checkpoint 候选池快照恢复 {len(cp_candidates)} 只")
                return [dict(c) for c in cp_candidates if isinstance(c, dict)]

            cp_results = cp.get("results") or {}
            if not cp_results:
                return []
            rebuilt: List[Dict[str, Any]] = []
            for code, rec in cp_results.items():
                if not isinstance(rec, dict):
                    continue
                # 复用 results 中已存在的 phase1 字段，避免重新跑 Phase 1 筛选
                rebuilt.append({
                    "stock": rec.get("stock") or rec.get("stock_code") or code,
                    "name": rec.get("name") or rec.get("stock_name") or "",
                    "stock_code": rec.get("stock_code") or code,
                    "stock_name": rec.get("stock_name") or "",
                    "pool": rec.get("pool") or "",
                    "source_pools": rec.get("source_pools") or [],
                    "source_queries": rec.get("source_queries") or [],
                    "source_reasons": rec.get("source_reasons") or [],
                    "screen_id": rec.get("screen_id") or "",
                    "screen_ids": rec.get("screen_ids") or [],
                    "strategy_type": rec.get("strategy_type") or "",
                    "strategy_types": rec.get("strategy_types") or [],
                    "entry_bias": rec.get("entry_bias") or "",
                    "entry_biases": rec.get("entry_biases") or [],
                    "screening_reason": rec.get("screening_reason") or "",
                    "pool_score": rec.get("pool_score"),
                    "pool_rank": rec.get("pool_rank"),
                    "pool_score_detail": rec.get("pool_score_detail") or {},
                    "pool_total_candidates": rec.get("pool_total_candidates"),
                    "pool_scored_candidates": rec.get("pool_scored_candidates"),
                    "source_score_records": rec.get("source_score_records") or [],
                    "sector": rec.get("sector") or "",
                    "money_flow": rec.get("money_flow") or {},
                    "data_quality_flags": rec.get("data_quality_flags") or [],
                    "phase1_score": rec.get("phase1_score"),
                })
            if rebuilt:
                logger.info(
                    f"Resume 模式：从 checkpoint 恢复候选池 {len(rebuilt)} 只（今日报告尚未生成）"
                )
            return rebuilt
        except Exception as e:
            logger.warning(f"Resume checkpoint 候选池恢复失败: {e}")
            return []

    # 获取候选股（resume 时必须优先复用今日报告里的原候选池）
    if gen is not None and hasattr(gen, "candidates") and gen.candidates:
        candidates = gen.candidates
        logger.info(f"复用 Phase 1 候选股: {len(candidates)} 只")
    elif resume:
        candidates = _load_resume_candidates_from_report()
        if not candidates:
            # 兜底：报告没生成时，从 checkpoint 恢复（避免鸡生蛋）
            candidates = _load_resume_candidates_from_checkpoint()
        if not candidates:
            raise RuntimeError("Resume 模式无法从今日报告或 checkpoint 恢复候选池，拒绝重新筛选以避免候选池变化")
        # 当走 checkpoint 兜底时，签名直接从 checkpoint 取
        if not resumed_report_signature and cp.get("screening_signature"):
            resumed_report_signature = cp["screening_signature"]
            logger.info(f"Resume 模式：从 checkpoint 复用 screening_signature")
    else:
        logger.warning("Phase 1 预热结果不可用，尝试从分析师结果提取")
        from llm_scorer import CandidateGenerator
        gen2 = CandidateGenerator(SKILLS_DIR, OUTPUT_DIR)
        candidates = gen2._run_xuangu_screening()
        logger.warning(f"降级重新获取候选股: {len(candidates)} 只")

    def _candidate_stock(c: Dict[str, Any]) -> str:
        return str(c.get("stock") or c.get("stock_code") or "").strip()

    def _candidate_name(c: Dict[str, Any]) -> str:
        return str(c.get("name") or c.get("stock_name") or "").strip()

    normalized_candidates: List[Dict[str, Any]] = []
    for c in candidates:
        if not isinstance(c, dict):
            continue
        stock = _candidate_stock(c)
        if not stock:
            continue
        item = dict(c)
        name = _candidate_name(item)
        item["stock"] = stock
        item["stock_code"] = item.get("stock_code") or stock
        item["name"] = name
        item["stock_name"] = item.get("stock_name") or name
        normalized_candidates.append(item)
    if len(normalized_candidates) != len(candidates):
        logger.warning(f"候选池字段修正后过滤空代码: {len(candidates)} -> {len(normalized_candidates)}")
    candidates = normalized_candidates

    def _checkpoint_candidate_snapshot(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        snapshot: List[Dict[str, Any]] = []
        for c in items:
            if not isinstance(c, dict):
                continue
            item = dict(c)
            item["stock"] = _candidate_stock(item)
            item["stock_code"] = item.get("stock_code") or item["stock"]
            item["name"] = _candidate_name(item)
            item["stock_name"] = item.get("stock_name") or item["name"]
            if item["stock"]:
                snapshot.append(item)
        return snapshot

    current_screening_signature = getattr(gen, "screening_signature", "") if gen is not None else resumed_report_signature
    if not current_screening_signature:
        try:
            from llm_scorer import _screening_signature
            current_screening_signature = _screening_signature()
        except Exception:
            current_screening_signature = ""
    current_candidate_signature = _candidate_universe_signature(candidates, current_screening_signature)
    cp_sig = cp.get("screening_signature", "")
    cp_candidate_sig = cp.get("candidate_signature", "")
    if resume and cp_candidate_sig and cp.get("results") and not cp.get("candidates"):
        # 旧 checkpoint 没有候选池快照，无法用“已完成子集”重算全集签名。
        # 保留原签名，避免把有效的已完成结果误清空。
        current_candidate_signature = cp_candidate_sig
    candidate_sig_ok = cp_candidate_sig == current_candidate_signature
    old_cp_has_results = bool(cp.get("completed") or cp.get("results"))
    if old_cp_has_results and (
        (current_screening_signature and cp_sig != current_screening_signature)
        or not candidate_sig_ok
    ):
        logger.warning(
            f"Checkpoint 签名变化 screening {cp_sig or '(missing)'} -> {current_screening_signature or '(missing)'}, "
            f"candidates {cp_candidate_sig[:12] or '(missing)'} -> {current_candidate_signature[:12]}，清空旧辩论结果"
        )
        cp = {
            "date": date.today().strftime("%Y%m%d"),
            "completed": [],
            "failed": [],
            "results": {},
            "candidates": _checkpoint_candidate_snapshot(candidates),
            "screening_signature": current_screening_signature,
            "candidate_signature": current_candidate_signature,
        }
        try:
            with _exclusive_file_lock(cp_lock_file):
                tmp = cp_file.with_name(f"{cp_file.name}.{os.getpid()}.tmp")
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(cp, f, ensure_ascii=False)
                    f.flush()
                tmp.replace(cp_file)
        except Exception as e:
            logger.warning(f"重置 checkpoint 失败: {e}")
    elif current_screening_signature and not cp.get("screening_signature"):
        cp["screening_signature"] = current_screening_signature
    if not cp.get("candidate_signature"):
        cp["candidate_signature"] = current_candidate_signature

    def _save_outer_checkpoint(current_cp: Dict[str, Any]) -> None:
        with _exclusive_file_lock(cp_lock_file):
            current_cp["date"] = date.today().strftime("%Y%m%d")
            current_cp["screening_signature"] = current_screening_signature
            current_cp["candidate_signature"] = current_candidate_signature
            current_cp["candidates"] = _checkpoint_candidate_snapshot(candidates)
            tmp = cp_file.with_name(f"{cp_file.name}.{os.getpid()}.outer.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(current_cp, f, ensure_ascii=False)
                f.flush()
            tmp.replace(cp_file)

    def _recover_structured_failed_pm(current_cp: Dict[str, Any]) -> Dict[str, Any]:
        failed_items = [
            (code, result)
            for code, result in (current_cp.get("results") or {}).items()
            if isinstance(result, dict) and result.get("decision_source") == "StructuredFailed"
        ]
        if not failed_items:
            return current_cp
        from stock_selection_debate.debate_engine import rerun_portfolio_manager_from_checkpoint

        def _mark_pm_failed_for_retry(code: str) -> None:
            current_cp.setdefault("results", {}).pop(code, None)
            current_cp["completed"] = [c for c in current_cp.get("completed", []) if c != code]
            if code not in current_cp.get("failed", []):
                current_cp.setdefault("failed", []).append(code)

        logger.warning(f"Checkpoint 中发现 {len(failed_items)} 只基金经理裁决失败，尝试只从 PM 节点断点续跑")
        recovered_count = 0
        for code, result in failed_items:
            try:
                recovered = rerun_portfolio_manager_from_checkpoint(result)
                if recovered and recovered.get("decision_source") != "StructuredFailed":
                    current_cp["results"][code] = recovered
                    recovered_count += 1
                    _save_outer_checkpoint(current_cp)
                    logger.info(
                        f"PM 断点续跑成功: {code} {recovered.get('stock_name', '')} "
                        f"{recovered.get('signal')} {recovered.get('confidence')}分"
                    )
                else:
                    _mark_pm_failed_for_retry(code)
                    _save_outer_checkpoint(current_cp)
                    logger.warning(f"PM 断点续跑未恢复: {code}")
            except Exception as e:
                _mark_pm_failed_for_retry(code)
                _save_outer_checkpoint(current_cp)
                logger.warning(f"PM 断点续跑异常: {code}: {e}")
        logger.warning(f"PM 断点续跑完成: {recovered_count}/{len(failed_items)} 只恢复")
        return current_cp

    cp = _recover_structured_failed_pm(cp)
    done_set = set(cp["completed"])

    all_done = False
    if candidates:
        # 检查所有候选股是否都已在 checkpoint 中完成
        all_done = all(_candidate_stock(c) in done_set for c in candidates)
        if all_done:
            logger.info(f"所有 {len(candidates)} 只候选股已在 checkpoint 完成（{len(done_set)} 只），跳过 K 线获取")
        else:
            missing = [_candidate_stock(c) for c in candidates if _candidate_stock(c) not in done_set]
            logger.info(f"候选股 {len(candidates)} 只，checkpoint {len(done_set)} 只，{len(missing)} 只未完成: {missing[:5]}")

    if all_done:
        logger.info(f"所有 {len(candidates)} 只候选股已在 checkpoint 完成，直接使用结果；仍重建 K 线数据包用于量化做多分")
        saved_results = {_candidate_stock(c): cp["results"][_candidate_stock(c)] for c in candidates if _candidate_stock(c) in cp["results"]}
        results = list(saved_results.values())
        phase1_cache = load_phase1_cache(OUTPUT_DIR)
        try:
            from stock_selection_debate.data_fetcher import _prefetch_debate_data
            _prefetch_debate_data(candidates)
        except Exception as e:
            logger.warning(f"Route-B 评分数据预获取失败（继续逐票拉取）: {e}")
        debate_packets = []
        failed_stocks = []
        for i, c in enumerate(candidates):
            stock = _candidate_stock(c)
            name = _candidate_name(c)
            try:
                kline = fetch_kline(stock)
            except Exception as e:
                logger.warning(f"K线获取失败 {stock} {name}，使用空数据包评分: {e}")
                kline = []
                failed_stocks.append(stock)
            packet = build_debate_packet(stock, name, phase1_cache, kline)
            for source_key in (
                "pool", "source_pools", "source_queries", "source_reasons",
                "screen_id", "screen_ids", "strategy_type", "strategy_types",
                "entry_bias", "entry_biases", "screening_reason",
                "pool_score", "pool_rank", "pool_score_detail",
                "pool_total_candidates", "pool_scored_candidates", "source_score_records",
            ):
                if c.get(source_key) not in (None, "", []):
                    packet[source_key] = c.get(source_key)
            _apply_pool_money_flow_seed(packet, c)
            tech_data = c.get("tech_data") or {}
            if tech_data and isinstance(tech_data, dict):
                rsi = tech_data.get("rsi")
                if rsi is not None:
                    packet.setdefault("indicators", {})["rsi_14"] = round(float(rsi), 1)
                ma = tech_data.get("ma_trend")
                if ma:
                    packet.setdefault("kline_summary", {})["ma_system"] = ma
            pe = c.get("pe")
            if pe and pe > 0:
                packet.setdefault("financial", {})["pe_ttm"] = round(float(pe), 1)
            yc = c.get("yesterday_chg")
            if yc is not None:
                packet.setdefault("kline_summary", {})["yesterday_chg"] = round(float(yc), 2)
            sector = c.get("sector") or ""
            if sector:
                packet["sector"] = sector
                packet["data_quality_flags"] = [
                    f for f in packet.get("data_quality_flags", [])
                    if f != "SECTOR_MISSING"
                ]
            fin = c.get("_financial")
            if fin and isinstance(fin, dict):
                for k, v in fin.items():
                    if v is not None and packet["financial"].get(k) is None:
                        packet["financial"][k] = v
            c["_financial"] = packet.get("financial")
            c["money_flow"] = packet.get("money_flow")
            c["_data_quality_flags"] = packet.get("data_quality_flags", [])
            c["kline_summary"] = packet.get("kline_summary", {})
            c["indicators"] = packet.get("indicators", {})
            c["kline_raw"] = packet.get("kline_raw", [])
            debate_packets.append(packet)
        if failed_stocks:
            logger.warning(f"评分数据包 K线缺失 {len(failed_stocks)} 只: {failed_stocks[:5]}...")
        market_context = ''
    else:
        # 加载 Phase 1 财务缓存
        phase1_cache = load_phase1_cache(OUTPUT_DIR)
        logger.info(f"Phase1 财务缓存: {len(phase1_cache)} 只，候选股 {len(candidates)} 只")
        try:
            from stock_selection_debate.data_fetcher import _prefetch_debate_data
            _prefetch_debate_data(candidates)
        except Exception as e:
            logger.warning(f"Route-B 辩论前预获取失败（继续逐票拉取）: {e}")

        # 获取 K 线（多级备用）
        debate_packets = []
        failed_stocks = []
        for i, c in enumerate(candidates):
            stock = _candidate_stock(c)
            name = _candidate_name(c)
            if stock in done_set and stock in cp.get("results", {}):
                logger.info(f"Checkpoint 已有LLM结果，仍重建评分数据包: {stock} {name}")
            try:
                kline = fetch_kline(stock)
            except Exception as e:
                logger.warning(f"K线获取失败 {stock} {name}，使用空数据包评分: {e}")
                failed_stocks.append(stock)
                kline = []
            packet = build_debate_packet(stock, name, phase1_cache, kline)
            # 保留第一阶段筛选来源，让辩论和盘中买入知道这是追涨、低吸还是资金吸筹。
            for source_key in (
                "pool", "source_pools", "source_queries", "source_reasons",
                "screen_id", "screen_ids", "strategy_type", "strategy_types",
                "entry_bias", "entry_biases", "screening_reason",
                "pool_score", "pool_rank", "pool_score_detail",
                "pool_total_candidates", "pool_scored_candidates", "source_score_records",
            ):
                if c.get(source_key) not in (None, "", []):
                    packet[source_key] = c.get(source_key)
            # 资金流兜底：如果实时链路拿不到主力净流入，复用候选池已计算的 main_flow_value。
            _apply_pool_money_flow_seed(packet, c)
            # ── Phase1 预热数据注入（覆盖 build_debate_packet 重新计算的值）───
            # tech_data: RSI + 均线
            tech_data = c.get("tech_data") or {}
            if tech_data and isinstance(tech_data, dict):
                rsi = tech_data.get("rsi")
                if rsi is not None:
                    packet.setdefault("indicators", {})["rsi_14"] = round(float(rsi), 1)
                ma = tech_data.get("ma_trend")
                if ma:
                    packet.setdefault("kline_summary", {})["ma_system"] = ma
            # PE: 从 score_candidates 预取的 c["pe"]（akshare 直读，最准确）
            pe = c.get("pe")
            if pe and pe > 0:
                packet.setdefault("financial", {})["pe_ttm"] = round(float(pe), 1)
            # 昨日涨幅: c["yesterday_chg"]
            yc = c.get("yesterday_chg")
            if yc is not None:
                packet.setdefault("kline_summary", {})["yesterday_chg"] = round(float(yc), 2)
            # 板块: 优先用 Phase1 已获取的 sector（避免重复调用 mx）
            sector = c.get("sector") or ""
            if sector:
                packet["sector"] = sector
                packet["data_quality_flags"] = [
                    f for f in packet.get("data_quality_flags", [])
                    if f != "SECTOR_MISSING"
                ]
            # _financial: 注入完整财务数据（包含 build_debate_packet 未能获取的字段）
            fin = c.get("_financial")
            if fin and isinstance(fin, dict):
                for k, v in fin.items():
                    if v is not None and packet["financial"].get(k) is None:
                        packet["financial"][k] = v
            c["kline_summary"] = packet.get("kline_summary", {})
            c["indicators"] = packet.get("indicators", {})
            c["kline_raw"] = packet.get("kline_raw", [])
            debate_packets.append(packet)

        if failed_stocks:
            logger.warning(f"K线获取失败共 {len(failed_stocks)} 只，跳过: {failed_stocks[:5]}...")
        logger.info(f"辩论数据包准备完成: {len(debate_packets)} 只")

        # 构建市场整体上下文（给辩论引擎参考）
        market_parts = []
        for r in analyst_results:
            name = r.get('name', '')
            findings = r.get('findings', '') or ''
            if name == '市场情绪分析师' and findings and '未知' not in findings:
                market_parts.append(f"【市场情绪】{findings.strip()}")
            elif name == '新闻分析师' and findings and '无结果' not in findings and '未发现' not in findings:
                clean = findings.strip()[:300]
                if clean:
                    market_parts.append(f"【市场新闻】{clean}")
        # 选股辩论只注入买入/观察口径，避免复用持仓卖出规则干扰新开仓判断。
        market_parts.append(
            "【短线选股技术参考】重点看未来1-3个交易日的新开仓可买性："
            "均线多头或突破回踩有效、放量上攻、资金承接、MACD/KDJ改善可加分；"
            "趋势破位、放量下跌、严重超买叠加顶部反转、K线数据缺失应降级。"
        )
        market_context = '\n'.join(market_parts) if market_parts else ''
        logger.info(f"辩论市场上下文: {market_context[:200] if market_context else '(空)'}...")

        # ── 断点续跑 checkpoint ─────────────────────────────
        def load_checkpoint_unlocked():
            if cp_file.exists():
                try:
                    with open(cp_file) as f:
                        cp = json.load(f)
                    sig_ok = (
                        (not current_screening_signature or cp.get("screening_signature") == current_screening_signature)
                        and cp.get("candidate_signature") == current_candidate_signature
                    )
                    if cp.get("date") == date.today().strftime("%Y%m%d") and sig_ok:
                        return cp
                except:
                    pass
            return {
                "date": date.today().strftime("%Y%m%d"),
                "completed": [],
                "failed": [],
                "results": {},
                "candidates": _checkpoint_candidate_snapshot(candidates),
                "screening_signature": current_screening_signature,
                "candidate_signature": current_candidate_signature,
            }

        def save_checkpoint_unlocked(cp):
            cp["date"] = date.today().strftime("%Y%m%d")
            if current_screening_signature:
                cp["screening_signature"] = current_screening_signature
            cp["candidate_signature"] = current_candidate_signature
            cp["candidates"] = _checkpoint_candidate_snapshot(candidates)
            tmp = cp_file.with_name(f"{cp_file.name}.{os.getpid()}.{threading.get_ident()}.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(cp, f, ensure_ascii=False)
                f.flush()
            tmp.replace(cp_file)

        def load_checkpoint():
            with _exclusive_file_lock(cp_lock_file):
                return load_checkpoint_unlocked()

        def save_checkpoint(cp):
            with _exclusive_file_lock(cp_lock_file):
                save_checkpoint_unlocked(cp)

        _lock = threading.Lock()
        def checkpoint_cb(code, result):
            with _lock:
                try:
                    with _exclusive_file_lock(cp_lock_file):
                        cp = load_checkpoint_unlocked()
                        if result:
                            if code not in cp["completed"]:
                                cp["completed"].append(code)
                            cp["results"][code] = result
                            node_status = cp.setdefault("node_status", {})
                            node_status[code] = {
                                **(node_status.get(code) or {}),
                                "data_packet_ready": True,
                                "bull": bool((result or {}).get("bull_history")),
                                "bear": bool((result or {}).get("bear_history")),
                                "judge": bool((result or {}).get("research_plan")),
                                "risk": bool((result or {}).get("debate_history")),
                                "pm": bool((result or {}).get("decision_source")),
                                "scoring_done": False,
                                "updated_at": datetime.now().isoformat(),
                            }
                            cp["failed"] = [f for f in cp.get("failed", []) if f != code]
                        else:
                            if code not in cp.get("failed", []):
                                cp["failed"].append(code)
                            cp.setdefault("node_status", {}).setdefault(code, {})["pm"] = False
                        save_checkpoint_unlocked(cp)
                    logger.info(f"[checkpoint_cb] {code} sig={result.get('signal') if result else None}")
                except Exception as e:
                    logger.error(f"[checkpoint_cb] save FAILED: {e}")

        cp = load_checkpoint()
        cp_date = cp.get("date", "")
        today_str = date.today().strftime("%Y%m%d")
        # checkpoint 日期不是今天就清空 failed，防止旧失败阻塞今日运行
        if cp_date != today_str:
            logger.warning(f"Checkpoint 日期 {cp_date} != 今天 {today_str}，清空 failed set")
            cp["failed"] = []
            save_checkpoint(cp)
        done_set = set(cp["completed"])
        failed_set = set(cp["failed"])
        # 旧版 checkpoint（completed 列表为空，failed 却有大量股票）说明是 2026-05-17 残留
        # 这种情况视为「从未成功」→ 全部 failed 股都重试
        if len(done_set) == 0 and len(failed_set) > 50:
            logger.warning(f"旧版残留 checkpoint：completed=0, failed={len(failed_set)}，全部重试")
            failed_set = set()
            cp["failed"] = []
            save_checkpoint(cp)
        saved_results = {_candidate_stock(c): cp["results"][_candidate_stock(c)] for c in candidates if _candidate_stock(c) in cp["results"]}
        logger.info(f"Checkpoint: {len(done_set)} 已完成, {len(failed_set)} 失败, 已保存结果 {len(saved_results)} 只")

        # 过滤待辩论的候选股；失败股多半是外部模型/网络瞬时问题，下一轮直接重试。
        retry_failed = [p for p in debate_packets if p.get("stock_code") in failed_set and p.get("stock_code") not in saved_results]
        fresh_pending = [p for p in debate_packets
                         if p.get("stock_code") not in done_set
                         and p.get("stock_code") not in failed_set]
        pending_packets = fresh_pending + retry_failed
        if retry_failed:
            logger.warning(f"Checkpoint 中 {len(retry_failed)} 只失败股将重试")
        logger.info(f"待辩论: {len(pending_packets)} 只（跳过 {len(done_set)} 已完成，重试 {len(retry_failed)} 失败）")

        # 执行辩论（新 LangGraph 版本）
        debate = StockDebateEngine(model=model, max_debate_rounds=1)
        logger.info(f"===== 辩论引擎启动 ===== 待辩论: {len(pending_packets)} 只, 市场上下文长度: {len(market_context)}chars")
        if pending_packets:
            try:
                safe_parallel = max(1, min(3, int(os.getenv("DEBATE_MAX_PARALLEL", "3"))))
                new_results = debate.run(pending_packets, market_context=market_context, checkpoint_cb=checkpoint_cb, max_parallel=safe_parallel)
                logger.info(f"辩论引擎返回 new_results={len(new_results)} 只")
            except Exception as e:
                logger.error(f"辩论引擎异常: {e}")
                new_results = []
        else:
            new_results = []

        # 合并已保存 + 新结果
        results = list(saved_results.values()) + new_results
        if candidates and not results:
            raise RuntimeError("Phase 2 没有可用辩论结果，拒绝生成空的 route_b_complete 报告")
        unresolved_model_failures = [
            r for r in results
            if isinstance(r, dict)
            and (
                r.get("decision_source") == "StructuredFailed"
                or r.get("signal") in ("MODEL_FAILED", "PENDING_RETRY")
            )
        ]
        if unresolved_model_failures:
            names = ", ".join(
                f"{r.get('stock_code') or r.get('stock', '')}{r.get('stock_name') or r.get('name', '')}"
                for r in unresolved_model_failures[:5]
            )
            raise RuntimeError(
                f"Phase 2 存在 {len(unresolved_model_failures)} 只模型失败/待重试候选，"
                f"拒绝生成成功早报: {names}"
            )
    logger.info(f"辩论引擎返回: {len(results)} 只, 前3只: {[{k:r[k] for k in ['stock_code','stock_name','confidence','signal']} for r in results[:3]]}")

    # 转换结果格式（通过 name 反查原始 stock code，避免 debate 返回的 stock_code 为空）
    name_to_stock = {_candidate_name(c): _candidate_stock(c) for c in candidates}
    source_by_stock = {}
    for c in candidates:
        stock_key = str(c.get("stock", "")).zfill(6) if c.get("stock") else ""
        if stock_key and stock_key not in source_by_stock:
            source_by_stock[stock_key] = c

    def _format_position_ratio(value: Any, decision_text: str = "") -> str:
        raw = value
        has_percent = False
        if raw in (None, ""):
            m = re.search(r'position_ratio\s*[=：:]\s*([0-9.]+)\s*(%)?', decision_text or "", re.IGNORECASE)
            if not m:
                return "0%"
            raw = m.group(1)
            has_percent = bool(m.group(2))
        elif isinstance(raw, str):
            has_percent = "%" in raw
            raw = raw.strip().rstrip("%")
        try:
            ratio = float(raw)
        except (TypeError, ValueError):
            return "0%"
        pct = ratio if has_percent or ratio > 1 else ratio * 100
        return f"{round(max(0.0, min(100.0, pct))):.0f}%"

    def _extract_reason(decision_text: str) -> str:
        m = re.search(r'reason\s*[=：:]\s*([^\n]+)', decision_text or "", re.IGNORECASE)
        return m.group(1).strip().strip(",，}") if m else ""

    # ★ 6-04 修复：建 packet_map 以便 c 字典构造时反查 _financial / _technical / pe / rsi
    packet_map = {p.get("stock_code", ""): p for p in debate_packets if p.get("stock_code")}

    ranked = []
    for r in results:
        name = r.get("stock_name", "")
        stock = r.get("stock_code", "") or name_to_stock.get(name, "")
        source_meta = source_by_stock.get(str(stock).zfill(6), {})
        # ★ 6-04 修复：从 packet_map 拿 packet 以回填 _financial / _technical / pe / rsi
        pkt = packet_map.get(stock, {})
        c = {
            "stock": stock,
            "name": name,
            "pool": r.get("pool") or source_meta.get("pool", ""),
            "source": r.get("source") or source_meta.get("source", ""),
            "source_pools": r.get("source_pools") or source_meta.get("source_pools", []),
            "source_queries": r.get("source_queries") or source_meta.get("source_queries", []),
            "source_reasons": r.get("source_reasons") or source_meta.get("source_reasons", []),
            "screen_id": r.get("screen_id") or source_meta.get("screen_id", ""),
            "screen_ids": r.get("screen_ids") or source_meta.get("screen_ids", []),
            "strategy_type": r.get("strategy_type") or source_meta.get("strategy_type", ""),
            "strategy_types": r.get("strategy_types") or source_meta.get("strategy_types", []),
            "entry_bias": r.get("entry_bias") or source_meta.get("entry_bias", ""),
            "entry_biases": r.get("entry_biases") or source_meta.get("entry_biases", []),
            "screening_reason": r.get("screening_reason") or source_meta.get("screening_reason", ""),
            "pool_score": r.get("pool_score") if r.get("pool_score") not in (None, "") else source_meta.get("pool_score"),
            "pool_rank": r.get("pool_rank") if r.get("pool_rank") not in (None, "") else source_meta.get("pool_rank"),
            "pool_score_detail": r.get("pool_score_detail") or source_meta.get("pool_score_detail", {}),
            "pool_total_candidates": r.get("pool_total_candidates") or source_meta.get("pool_total_candidates"),
            "pool_scored_candidates": r.get("pool_scored_candidates") or source_meta.get("pool_scored_candidates"),
            "source_score_records": r.get("source_score_records") or source_meta.get("source_score_records", []),
            "sector": r.get("sector") or source_meta.get("sector", ""),
            "money_flow": r.get("money_flow", {}),
            "data_contract": r.get("data_contract", {}) or pkt.get("data_contract", {}),
            "signal": r.get("signal", "WATCH"),
            "buy_score": r.get("buy_score", _buy_score_value(r)),
            "confidence": r.get("confidence", 50),
            "final_decision": r.get("final_decision", ""),
            "position_ratio": _format_position_ratio(r.get("position_ratio"), r.get("final_decision", "")),
            "reason": r.get("reason", "") or _extract_reason(r.get("final_decision", "")),
            "decision_source": r.get("decision_source", ""),
            "raw_final_decision": r.get("raw_final_decision", ""),
            "decision_models": r.get("decision_models", {}),
            "evidence_refs": r.get("evidence_refs", []),
            "missing_data_used": r.get("missing_data_used", []),
            "unsupported_claims": r.get("unsupported_claims", []),
            "evidence_validation": r.get("evidence_validation", {}),
            "data_quality_flags": r.get("data_quality_flags", []),
            "final_score": r.get("confidence", 50),
            # ★ 6-04 修复：_run_single 返回 bull_history/bear_history，但 c 字典要 bull_argument/bear_argument
            "bull_argument": r.get("bull_history", ""),
            "bear_argument": r.get("bear_history", ""),
            # ★ 6-04 修复：研究计划 + 辩论 history（之前完全没回填）
            "research_plan": r.get("research_plan", ""),
            "debate_history": r.get("debate_history", ""),
            "bull_history": r.get("bull_history", ""),
            "bear_history": r.get("bear_history", ""),
            # ★ 6-04 修复：财务/技术面/PE/RSI 从 packet 注入（之前 ranked_cand 完全缺这 4 个字段）
            "_financial": pkt.get("financial", {}),
            "_technical": pkt.get("indicators", {}),
            "kline_summary": pkt.get("kline_summary", {}),
            "indicators": pkt.get("indicators", {}),
            "kline_raw": pkt.get("kline_raw", []),
            "kline_count": len([x for x in (pkt.get("kline_raw") or []) if isinstance(x, dict) and x]),
            "pe": (pkt.get("financial", {}) or {}).get("pe_ttm"),
            "rsi": (pkt.get("indicators", {}) or {}).get("rsi_14"),
        }
        # 注入 risk_flags：从 bear_argument 和 final_decision 提取风险关键词
        risk_flags = []
        bear_text = (r.get("bear_argument", "") or "") + (r.get("final_decision", "") or "")
        if any(k in bear_text for k in ["高PB", "PB>", "市净率过高"]):
            risk_flags.append("高PB")
        if any(k in bear_text for k in ["负债率", "高负债", "债务风险"]):
            risk_flags.append("高负债")
        if any(k in bear_text for k in ["换手率异常", "换手率过高", "换手率异常"]):
            risk_flags.append("换手异常")
        if any(k in bear_text for k in ["高位", "接近涨停", "高位追涨"]):
            risk_flags.append("高位")
        if any(k in bear_text for k in ["主力净流出", "超大单流出", "大单流出"]):
            risk_flags.append("主力流出")
        if any(k in bear_text for k in ["MACD收缩", "MACD背离", "顶背离"]):
            risk_flags.append("MACD走弱")
        if c.get("signal") == "AVOID":
            risk_flags.append("AVOID信号")
        c["risk_flags"] = list(set(risk_flags))  # 去重
        # 断点复用旧结果时，仍按候选池主力资金做一次兜底，避免历史缺失直接带入今日报告。
        try:
            _apply_pool_money_flow_seed(c, source_meta or c)
        except Exception:
            pass
        try:
            _apply_quant_confidence_overlay(c, pkt or {}, source_meta or c)
        except Exception as e:
            logger.warning(f"{stock} 量化做多分覆盖失败，保留基金经理原始分: {e}")
        ranked.append(c)

    try:
        cp_latest = cp if isinstance(cp, dict) else {}
        node_status = cp_latest.get("node_status") or {}
        for c in ranked:
            stock_key = str(c.get("stock", "")).zfill(6)
            if stock_key in node_status:
                node_status[stock_key]["scoring_done"] = True
        if node_status:
            cp_latest["node_status"] = node_status
            _save_outer_checkpoint(cp_latest)
    except Exception as e:
        logger.warning(f"checkpoint 节点状态补写失败: {e}")

    buy_list = [r for r in ranked if r.get("signal") == "BUY"]
    watch_list = [r for r in ranked if r.get("signal") == "WATCH"]
    avoid_list = [r for r in ranked if r.get("signal") == "AVOID"]
    logger.info(f"ranked 前5只: {ranked[:5]}")

    result = {
        "ranked_candidates": ranked,
        "buy_list": buy_list,
        "watch_list": watch_list,
        "avoid_list": avoid_list,
    }

    # 转换为 phase2 格式（兼容 daily_report 结构）
    phase2 = debate_phase_to_phase2_format(result)
    phase2["candidates"] = candidates
    phase2["all_analysts"] = [r for r in analyst_results if r.get("status") == "success"]
    phase2["timestamp"] = datetime.now().isoformat()
    phase2["screening_signature"] = current_screening_signature
    try:
        phase2["checkpoint_summary"] = {
            "completed": len((cp or {}).get("completed") or []),
            "failed": len((cp or {}).get("failed") or []),
            "results": len((cp or {}).get("results") or {}),
            "node_status": len((cp or {}).get("node_status") or {}),
        }
    except Exception:
        phase2["checkpoint_summary"] = {}

    logger.info(f"Phase 2 辩论完成: BUY={len(result.get('buy_list',[]))} "
                 f"WATCH={len(result.get('watch_list',[]))} "
                 f"AVOID={len(result.get('avoid_list',[]))}")

    return phase2


# ── Phase 2 路由选择器（B为主，A为备）────────────────────────

def run_phase2_with_fallback(analyst_results: List[Dict[str, Any]], gen=None, dry_run: bool = False, model: str = "volcengine-plan/ark-code-latest") -> Dict[str, Any]:
    """默认Route B，失败时自动fallback到Route A"""
    route_b_error = None
    # 尝试 Route B
    try:
        result = route_b_phase2(analyst_results, gen=gen, dry_run=dry_run, model=model)
        logger.info("✅ Phase 2 Route B 成功")
        return result
    except Exception as e:
        logger.warning(f"⚠️ Route B 失败: {e}，自动切换到 Route A")
        route_b_error = str(e)

    # Fallback Route A
    try:
        result = route_a_phase2(analyst_results, gen=gen)
        result["fallback"] = True
        result["fallback_reason"] = route_b_error
        return result
    except Exception as e2:
        logger.error(f"❌ Route A 也失败了: {e2}")
        return {
            "candidates": [], "scored": [], "top_picks": [],
            "phase": "all_failed",
            "error": f"B: {route_b_error} | A: {e2}",
        }


def _write_financial_cache(gen) -> None:
    """将 Phase 1 预热获取的财务数据写回本地缓存文件"""
    try:
        fc = gen._financial_cache
        if not fc:
            return
        cache_file = OUTPUT_DIR / "fundamental_cache" / "all_stocks_financial.json"
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        # 合并已有缓存
        existing = {}
        if cache_file.exists():
            try:
                with open(cache_file) as f:
                    existing = json.load(f).get("data", {})
            except Exception:
                pass
        merged = {**existing, **fc}
        payload = {
            "update_date": date.today().isoformat(),
            "data": merged,
        }
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        logger.info(f"财务缓存已写回: {len(fc)} 只 → {cache_file}")
    except Exception as e:
        logger.warning(f"写回财务缓存失败: {e}")


def _push_phase2_feishu(scored: List[Dict], top_picks: List[Dict]):
    webhook = os.getenv("FEISHU_WEBHOOK_URL")
    if not webhook:
        return
    # 统计打分方式
    llm_count = len([s for s in scored if s.get("scoring_method") == "llm_one_by_one"])
    route_a_count = len([s for s in scored if s.get("scoring_method") == "route_a"])
    method_note = f"LLM成功{llm_count}只"
    if route_a_count > 0:
        method_note += f" | Route A规则补打分{route_a_count}只"

    lines = [f"📋 选股结果 {date.today()}（{method_note}）\n"]
    for i, s in enumerate(top_picks, 1):
        emoji = {"BUY": "🟢", "WATCH": "🟡", "AVOID": "🔴"}.get(s.get("action", "WATCH"), "⚪")
        sm = s.get("scoring_method", "?")
        tag = {"llm_one_by_one": "B-LLM", "route_a": "A规则", "llm_fallback": "B-fallback"}.get(sm, sm)
        lines.append(
            f"{i}. {emoji} {s.get('stock')} {s.get('name')} "
            f"总分:{s.get('total_score', 0)} {tag}"
        )
        reason = s.get("reason", "")
        if reason:
            lines.append(f"   理由: {reason}")
        lines.append("")
    lines.append(f"共扫描 {len(scored)} 只候选股票")
    feishu_push_text("\n".join(lines), webhook)

# ── Phase 3a: 选股回测（验证候选股本身好不好）─────────────

def run_backtest_selection(scored_candidates: List[Dict], lookback_days: int = 5) -> Dict[str, Any]:
    """
    选股回测：验证候选股在信号日后N天内的表现（快照法）
    - 问题：这批股票本身好不好？（不涉及仓位/止损止盈）
    - 方法：若有信号日收盘价，持有N天后对比大盘指数相对收益
    """

    if not scored_candidates:
        return {"type": "selection", "status": "no_candidates"}

    stocks = [c["stock"] for c in scored_candidates[:5]]

    results = []
    for candidate in scored_candidates[:5]:
        stock = candidate.get("stock")
        real_signal = candidate.get("signal", "WATCH")  # 来自 Phase2 的真实信号
        try:
            prices = _get_stock_hist_prices_mx(stock, days=lookback_days + 5)
            if prices and len(prices) >= 2:
                entry = prices[-2]  # 信号日前一天
                exit = prices[-1]   # 持有N天后
                ret = (exit - entry) / entry * 100
                results.append({
                    "stock": stock,
                    "name": candidate.get("name", stock),
                    "entry": entry,
                    "exit": exit,
                    "return_pct": round(ret, 2),
                    "signal": real_signal,  # 真实信号（ WATCH/BUY）
                })
                logger.info(f"选股回测 {stock}: {real_signal} 收益率 {ret:.2f}%")
            else:
                results.append({"stock": stock, "name": candidate.get("name", stock),
                                 "signal": real_signal, "error": "价格数据不足"})
        except Exception as e:
            logger.warning(f"选股回测 {stock} 异常: {e}")
            results.append({"stock": stock, "name": candidate.get("name", stock),
                             "signal": real_signal, "error": str(e)})

    total = [r for r in results if "return_pct" in r]
    avg_ret = sum(r["return_pct"] for r in total) / len(total) if total else 0
    win_rate = len([r for r in total if r["return_pct"] > 0]) / len(total) if total else 0

    return {
        "type": "selection",
        "status": "done",
        "lookback_days": lookback_days,
        "stocks": results,
        "avg_return_pct": round(avg_ret, 2),
        "win_rate": round(win_rate * 100, 1),
        "summary": f"平均收益 {avg_ret:.2f}%，胜率 {win_rate*100:.0f}%",
    }


# ── Phase 3b: 持仓周期回测（验证20日持仓策略相对大盘的超额收益）──

def run_backtest_strategy(scored_candidates: List[Dict], hold_days: int = 20, lookback_days: int = None) -> Dict[str, Any]:
    """
    持仓周期回测：验证若按信号买入、持有20个交易日后相对大盘的超额收益
    - 使用 Backtrader 事件驱动引擎（T+1 / 佣金 / 滑点 / 止损止盈）
    - 备：旧版快照算法（Backtrader 不可用时）
    """

    if not scored_candidates:
        return {"type": "strategy", "status": "no_candidates", "trades": []}

    stocks = [c["stock"] for c in scored_candidates[:5]]
    if not stocks:
        return {"type": "strategy", "status": "no_candidates", "trades": []}
    if lookback_days is None:
        lookback_days = int(os.getenv("BACKTEST_STRATEGY_LOOKBACK_DAYS", "120"))
    lookback_days = max(hold_days + 5, int(lookback_days))

    # 构造 Phase 2 格式信号（用于 Backtrader）
    phase2_result_for_bt = {
        "ranked_candidates": [
            {
                "stock": c["stock"],
                "name": c.get("name", c["stock"]),
                "signal": c.get("signal") or c.get("action"),
                "action": c.get("action") or c.get("signal"),
                "confidence": _confidence_value(c),
                "total_score": c.get("total_score"),
                "position_ratio": c.get("position_ratio"),
                "final_decision": c.get("final_decision", ""),
                # 策略模拟用于验证早报 Top5 的组合表现，WATCH 也参与模拟买入。
                "simulate_buy": True,
            }
            for c in scored_candidates[:5]
        ]
    }

    # 尝试 Backtrader 回测
    if _BT_AVAILABLE:
        try:
            bt_result = _bt_run_signal_backtest(
                phase2_result=phase2_result_for_bt,
                hold_days=hold_days,
                initial_cash=float(os.getenv("BACKTEST_INITIAL_CASH", "1000000")),
                lookback_days=lookback_days,
                verbose=False,
            )

            if bt_result.get('status') == 'ok':
                r = bt_result
                per_stock = r.get('per_stock', {})
                loaded_stocks = set(r.get('stocks', []))
                
                # 提取每只股票的回测结果
                trades = []
                for candidate in scored_candidates[:5]:
                    stock = candidate["stock"]
                    stock_result = per_stock.get(stock, {})
                    if 'return_pct' in stock_result:
                        # Backtrader 有实际成交
                        trades.append({
                            "stock": stock,
                            "name": candidate.get("name", stock),
                            "signal": candidate.get("signal", "WATCH"),
                            "entry_date": stock_result.get("entry_date", ""),
                            "entry_price": stock_result.get("entry_price"),
                            "exit_date": stock_result.get("exit_date", ""),
                            "exit_price": stock_result.get("exit_price"),
                            "return_pct": round(stock_result.get('return_pct', 0), 2),
                            "pnl": stock_result.get('pnl'),
                            "confidence": _confidence_value(candidate),
                            "status": "backtested",
                        })
                    elif 'entry_price' in stock_result:
                        # 有买入但未卖出（仍在持仓）
                        trades.append({
                            "stock": stock,
                            "name": candidate.get("name", stock),
                            "signal": candidate.get("signal", "WATCH"),
                            "entry_date": stock_result.get("entry_date", ""),
                            "entry_price": stock_result.get("entry_price"),
                            "exit_date": "",
                            "exit_price": None,
                            "return_pct": None,
                            "confidence": _confidence_value(candidate),
                            "status": "holding",
                        })
                    else:
                        if stock not in loaded_stocks:
                            reason_type = "data_missing"
                            error_msg = "K线数据不足，无法回测"
                        else:
                            reason_type = "not_filled"
                            error_msg = "Top5模拟未形成成交"
                        trades.append({
                            "stock": stock,
                            "name": candidate.get("name", stock),
                            "signal": candidate.get("signal", "WATCH"),
                            "confidence": _confidence_value(candidate),
                            "status": "no_trade",
                            "reason_type": reason_type,
                            "error": error_msg,
                        })
                
                avg_ret = r.get('total_return', 0)
                total_return = r.get('total_return', 0)
                
                # 沪深300基准
                try:
                    hs300_prices = _get_stock_hist_prices_mx("000300", days=lookback_days)
                    if hs300_prices and len(hs300_prices) >= 2:
                        hs300_ret = (hs300_prices[-1] - hs300_prices[0]) / hs300_prices[0] * 100
                    else:
                        hs300_ret = None
                except Exception:
                    hs300_ret = None
                
                alpha = (avg_ret - hs300_ret) if hs300_ret is not None else None
                valid = [t for t in trades if t.get('return_pct') is not None]
                avg_stock_ret = sum(t['return_pct'] for t in valid) / len(valid) if valid else 0
                
                logger.info(f"Backtrader 持仓回测: 最终收益 {total_return:.2f}%, Alpha {alpha:.2f}%")
                logger.info(f"Per-stock: {[{t['stock']: t.get('return_pct')} for t in trades]}")

                return {
                    "type": "strategy",
                    "status": "done",
                    "hold_days": hold_days,
                    "lookback_days": lookback_days,
                    "fromdate": r.get("fromdate"),
                    "todate": r.get("todate"),
                    "trades": trades,
                    "total_return_pct": round(total_return, 2),
                    "avg_return_pct": round(avg_stock_ret, 2),
                    "hs300_return_pct": round(hs300_ret, 2) if hs300_ret is not None else None,
                    "alpha_pct": round(alpha, 2) if alpha is not None else None,
                    "sharpe_ratio": r.get('sharpe_ratio'),
                    "max_drawdown": r.get('max_drawdown'),
                    "win_rate": r.get('win_rate'),
                    "summary": (
                        f"持仓{hold_days}天组合收益 {total_return:+.2f}%"
                        + (f" | 已平仓均收益 {avg_stock_ret:+.2f}%" if valid else " | 当前全部持有中")
                        + (f" | 沪深300基准 {hs300_ret:+.2f}% | Alpha {alpha:+.2f}%"
                           if alpha is not None else "")
                        + f" | 夏普 {r.get('sharpe_ratio', 'N/A')}"
                    ),
                }
        except Exception as e:
            logger.warning(f"Backtrader 回测失败，降级到旧算法: {e}")

    # ── 备：旧版快照算法 ──────────────────────────────────
    logger.info("使用旧版快照回测算法")
    results = []
    for candidate in scored_candidates[:5]:
        stock = candidate["stock"]
        real_signal = candidate.get("signal", "WATCH")
        try:
            prices = _get_stock_hist_prices_mx(stock, days=lookback_days)
            if prices and len(prices) >= hold_days:
                entry = prices[-hold_days]
                exit_price = prices[-1]
                ret = (exit_price - entry) / entry * 100
                results.append({
                    "stock": stock,
                    "name": candidate.get("name", stock),
                    "signal": real_signal,
                    "confidence": _confidence_value(candidate),
                    "entry_price": entry,
                    "exit_price": exit_price,
                    "return_pct": round(ret, 2),
                    "status": "backtested",
                })
                logger.info(f"持仓回测 {stock}: {real_signal} 持有{hold_days}天 {entry}→{exit_price} {ret:+.2f}%")
            else:
                results.append({"stock": stock, "name": candidate.get("name", stock),
                                 "signal": real_signal, "confidence": _confidence_value(candidate),
                                 "status": "no_trade", "error": "价格数据不足"})
        except Exception as e:
            logger.warning(f"持仓回测 {stock} 异常: {e}")
            results.append({"stock": stock, "name": candidate.get("name", stock),
                             "signal": real_signal, "confidence": _confidence_value(candidate),
                             "status": "no_trade", "error": str(e)})

    # 沪深300基准
    try:
        hs300_prices = _get_stock_hist_prices_mx("000300", days=lookback_days)
        if hs300_prices and len(hs300_prices) >= 2:
            hs300_ret = (hs300_prices[-1] - hs300_prices[0]) / hs300_prices[0] * 100
            logger.info(f"沪深300基准: {hs300_ret:+.2f}%")
        else:
            hs300_ret = None
    except Exception:
        hs300_ret = None

    valid = [r for r in results if "return_pct" in r]
    avg_ret = sum(r["return_pct"] for r in valid) / len(valid) if valid else 0
    alpha = (avg_ret - hs300_ret) if (hs300_ret is not None and valid) else None

    return {
        "type": "strategy",
        "status": "done",
        "hold_days": hold_days,
        "lookback_days": lookback_days,
        "fromdate": "",
        "todate": date.today().strftime("%Y-%m-%d"),
        "trades": results,
        "avg_return_pct": round(avg_ret, 2),
        "hs300_return_pct": round(hs300_ret, 2) if hs300_ret is not None else None,
        "alpha_pct": round(alpha, 2) if alpha is not None else None,
        "summary": (
            f"持仓{hold_days}天均收益 {avg_ret:+.2f}%"
            + (f" | 沪深300基准 {hs300_ret:+.2f}% | Alpha {alpha:+.2f}%"
               if alpha is not None else "")
        ),
    }


# ── Phase 4: 盘中执行 ────────────────────────────────────

def execute_trades(signals: List[Dict], dry_run: bool = True) -> List[Dict]:
    """执行交易信号"""
    api_url = os.getenv("MX_API_URL", "https://mkapi2.dfcfs.com/finskillshub")
    api_key = os.getenv("MX_APIKEY")
    webhook = os.getenv("FEISHU_WEBHOOK_URL")

    if not api_key:
        logger.error("MX_APIKEY 未设置，无法执行交易")
        return []

    import requests
    trade_url = f"{api_url}/api/claw/mockTrading/trade"
    headers = {"Content-Type": "application/json", "apikey": api_key}

    executed = []
    for signal in signals:
        stock = signal["stock"]
        action = signal["action"]  # "buy" or "sell"
        price = signal.get("price")
        quantity = signal.get("quantity", 100)
        reason = signal.get("reason", "")

        mode = "[DRY-RUN]" if dry_run else "[REAL]"
        logger.info(f"{mode} {action.upper()} {stock} @ {price} x {quantity} ({reason})")

        msg = f"📋 {mode} {action.upper()} {stock}\n💰 价格: {price}\n📊 数量: {quantity}\n📝 理由: {reason}"
        feishu_push_text(msg, webhook)

        if dry_run:
            executed.append({**signal, "executed": False, "dry_run": True})
            continue

        try:
            payload = {
                "type": action,
                "stockCode": stock,
                "price": price,
                "quantity": quantity,
                "useMarketPrice": price is None,
            }
            r = requests.post(trade_url, headers=headers, json=payload, timeout=15)
            result = r.json()
            logger.info(f"交易结果: {result}")

            exec_msg = f"✅ 成交 {stock} {action}\n📦 响应: {json.dumps(result, ensure_ascii=False)}"
            feishu_push_text(exec_msg, webhook)

            executed.append({**signal, "executed": True, "response": result})
        except Exception as e:
            logger.error(f"交易失败 {stock}: {e}")
            feishu_push_text(f"❌ 交易失败 {stock}: {e}", webhook)
            executed.append({**signal, "executed": False, "error": str(e)})

    return executed


# ── Route A 辅助函数 ───────────────────────────────────

def _fetch_stock_tech(stock: str, days: int = 20) -> Optional[Dict]:
    """
    拉取个股技术数据（收盘价+计算MA/MACD/成交量比率）
    用于 Route A 纯规则打分
    主：xqshare (xtquant) 实时拉 60 日 K 线全量指标；备：mx-data；末备：腾讯
    """
    # ★ 主路径：xqshare 实时拉（RSI/MA/MACD/量比 一次拿全）
    try:
        from llm_scorer import _fetch_tech_data_via_xqshare
        tech = _fetch_tech_data_via_xqshare(stock)
        if tech and _is_valid_tech_data(tech):
            tech["_source"] = "xqshare"
            return tech
    except Exception as e:
        logger.warning(f"xqshare 技术面获取失败 {stock}: {e}")

    # 备路径：mx-data（保留原兜底）
    prices = _get_stock_hist_prices_mx(stock, days=days)
    if not prices or len(prices) < 5:
        return None
    try:
        latest = prices[-1]
        ma5 = sum(prices[-5:]) / 5
        ma10 = sum(prices[-10:]) / 10 if len(prices) >= 10 else ma5
        ma20 = sum(prices[-20:]) / 20 if len(prices) >= 20 else ma10
        vol_ratio = 1.0  # 腾讯备用时无法获取成交量，比率设为默认值
        # 均线多头
        ma_trend = "多头" if (ma5 > ma10 > ma20) else ("空头" if (ma5 < ma10 < ma20) else "震荡")
        # MACD 判断（标准算法）
        macd_bullish = False
        if len(prices) >= 35:
            # 计算完整EMA序列
            ema12_seq = _ema_seq(prices, 12)
            ema26_seq = _ema_seq(prices, 26)
            # DIF = EMA12 - EMA26
            dif_seq = [e12 - e26 for e12, e26 in zip(ema12_seq, ema26_seq)]
            # DEA = DIF的9日EMA
            dea_seq = _ema_seq(dif_seq, 9)
            macd_bullish = dif_seq[-1] > dea_seq[-1]
        return {
            "latest": latest,
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "ma_trend": ma_trend,
            "vol_ratio": vol_ratio,
            "macd_bullish": macd_bullish,
        }
    except Exception as e:
        logger.warning(f"技术数据获取失败 {stock}: {e}")
        return None


def _ema(prices: List[float], period: int) -> float:
    """计算指数移动平均"""
    if not prices:
        return 0.0
    k = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = p * k + ema * (1 - k)
    return ema


def _ema_seq(prices: List[float], period: int) -> List[float]:
    """计算完整EMA序列（每个位置的EMA值）"""
    if not prices:
        return []
    k = 2 / (period + 1)
    result = [prices[0]]
    for p in prices[1:]:
        result.append(p * k + result[-1] * (1 - k))
    return result


def _apply_rule_score(candidate: Dict, news_output: Dict, tech_cache: Dict) -> int:
    """
    Route A 综合规则打分（0-100），不调 LLM
    规则维度：基本面(ROE/营收/利润/负债) + 技术面(RSI/MA/成交量) + 情绪面(利好/利空) + 资金面(北向)
    """
    score = 50  # 基础分
    reasons = []

    # ── 1. 基本面分 ──────────────────────────────────
    # 优先从 _financial 子字段读取（prewarm_cache 注入在这里），兼容顶层直接字段
    fin = candidate.get("_financial", {})
    
    # ROE（净资产收益率）
    roe = fin.get("roe_annual_latest") or candidate.get("roe_annual_latest")
    if roe is not None:
        if roe > 15:
            score += 10
            reasons.append(f"高ROE({roe:.1f}%)")
        elif roe > 10:
            score += 5
            reasons.append(f"良好ROE({roe:.1f}%)")
        elif roe < 0:
            score -= 12
            reasons.append(f"负ROE({roe:.1f}%)")

    # 营收增速
    rev_growth = fin.get("营收增速") or candidate.get("营收增速")
    if rev_growth is not None:
        if rev_growth > 20:
            score += 6
            reasons.append(f"高营收增速({rev_growth:.1f}%)")
        elif rev_growth > 10:
            score += 3
            reasons.append(f"营收增长({rev_growth:.1f}%)")
        elif rev_growth < 0:
            score -= 6
            reasons.append(f"营收下滑({rev_growth:.1f}%)")

    # 净利润增速
    profit_growth = fin.get("净利润增长率") or candidate.get("净利润增长率")
    if profit_growth is not None:
        if profit_growth > 20:
            score += 6
            reasons.append(f"高净利润增速({profit_growth:.1f}%)")
        elif profit_growth > 10:
            score += 3
            reasons.append(f"净利润增长({profit_growth:.1f}%)")
        elif profit_growth < 0:
            score -= 6
            reasons.append(f"净利润下滑({profit_growth:.1f}%)")

    # 负债率（越低越好）
    debt = fin.get("负债率") or candidate.get("负债率")
    if debt is not None:
        if debt < 40:
            score += 3
            reasons.append(f"低负债({debt:.1f}%)")
        elif debt > 70:
            score -= 5
            reasons.append(f"高负债({debt:.1f}%)")

    # ── 2. 技术面规则 ────────────────────────────────────
    tech_info = tech_cache.get(candidate.get("stock", ""))
    if tech_info:
        # RSI
        rsi = tech_info.get("rsi")
        if rsi is not None:
            if rsi < 30:
                score += 8
                reasons.append(f"RSI超卖({rsi:.0f})")
            elif rsi < 40:
                score += 4
                reasons.append(f"RSI偏低({rsi:.0f})")
            elif rsi > 70:
                score -= 5
                reasons.append(f"RSI超买({rsi:.0f})")

        # 均线多头
        ma_trend = tech_info.get("ma_trend")
        if ma_trend == "多头":
            score += 6
            reasons.append("均线多头")
        elif ma_trend == "空头":
            score -= 6
            reasons.append("均线空头")

        # 成交量放大
        vr = tech_info.get("vol_ratio", 1.0)
        if vr > 2.0:
            score += 5
            reasons.append(f"放量({vr:.1f}x)")
        elif vr > 1.5:
            score += 3
            reasons.append(f"温和放量({vr:.1f}x)")
        elif vr < 0.5:
            score -= 4
            reasons.append(f"缩量({vr:.1f}x)")
    else:
        # 无技术数据时，信任候选股本身的信号
        reason_text = candidate.get("reason", "")
        if "MACD" in reason_text or "金叉" in reason_text:
            score += 5
            reasons.append("MACD信号")
        if "MA" in reason_text or "均线" in reason_text:
            score += 3

    # ── 3. 情绪面规则 ────────────────────────────────────
    sentiment = candidate.get("sentiment", "中性")
    if sentiment == "利好":
        score += 8
        reasons.append("利好情绪")
    elif sentiment == "利空":
        score -= 8
        reasons.append("利空情绪")

    # 涨停基因
    if "涨停" in candidate.get("reason", ""):
        score += 5

    # ── 4. 资金面规则（北向资金）─────────────────────────
    try:
        fund_file = OUTPUT_DIR / "mx_search" / "mx_search_今日北向资金动向.json"
        if fund_file.exists():
            with open(fund_file) as f:
                fund_data = json.load(f)
            articles = fund_data.get("data", {}).get("data", {}).get("llmSearchResponse", {}).get("data", [])
            stock = candidate.get("stock", "")
            for art in articles:
                if stock in art.get("title", "") + art.get("content", ""):
                    if "净流入" in art.get("title", "") or "增持" in art.get("title", ""):
                        score += 5
                        reasons.append("北向资金净流入")
                    break
    except Exception:
        pass

    # ★ 回写技术面到 candidate，避免 gen_report 渲染缺字段
    if tech_info:
        candidate["_technical"] = tech_info

    return max(0, min(100, score))


def route_a_phase2(analyst_results: List[Dict[str, Any]], gen=None) -> Dict[str, Any]:
    """
    Route A: 纯规则引擎，不调LLM，秒级出结果
    包含批量预拉取个股技术数据，避免逐个拉取延迟
    """
    from llm_scorer import CandidateGenerator
    from concurrent.futures import ThreadPoolExecutor, as_completed

    successful = [r for r in analyst_results if r.get("status") == "success"]
    logger.info(f"Route A 启动: {len(successful)} 个分析师成功")

    news_output = next((r for r in successful if r["name"] == "新闻分析师"), {})
    tech_output = next((r for r in successful if r["name"] == "技术分析师"), {})

    # 复用 Phase 1 预热结果
    if gen is not None and hasattr(gen, 'candidates') and gen.candidates:
        xuangu_candidates = gen.candidates
        logger.info(f"Route A 复用 Phase 1 候选股: {len(xuangu_candidates)} 只")
        # 直接使用预热后的候选股（已有 _financial 和 tech_data）
        candidates = xuangu_candidates
    else:
        gen = CandidateGenerator(SKILLS_DIR, OUTPUT_DIR)
        candidates = gen.generate(news_analyst_output=news_output, tech_analyst_output=tech_output)

    if not candidates:
        logger.warning("候选股票池为空")
        return {"candidates": [], "scored": [], "top_picks": [], "phase": "route_a"}

    logger.info(f"Route A 候选股池: {len(candidates)} 只，预拉取技术数据...")

    # 批量预拉取个股技术数据（并发，timeout 15s/candidate）
    tech_cache: Dict[str, Dict] = {}
    stocks_to_fetch = [c["stock"] for c in candidates]

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_fetch_stock_tech, s): s for s in stocks_to_fetch}
        for future in as_completed(futures):
            stock = futures[future]
            try:
                result = future.result(timeout=15)
                if result:
                    tech_cache[stock] = result
            except Exception:
                pass

    logger.info(f"技术数据获取完成: {len(tech_cache)}/{len(stocks_to_fetch)} 只")

    # 规则打分
    scored = []
    for c in candidates:
        s = _apply_rule_score(c, news_output, tech_cache)
        action = "BUY" if s >= 65 else "WATCH" if s >= 40 else "AVOID"
        scored.append({
            **c,
            "news_score": s,
            "tech_score": s,
            "fundamental_score": s,
            "sentiment_score": s,
            "total_score": s,
            "action": action,
            "scoring_method": "route_a",
        })

    scored.sort(key=lambda x: x.get("adjusted_score", x.get("total_score", 0)), reverse=True)
    top_picks = scored[:5]

    logger.info(f"Route A 完成: Top {len(top_picks)} -> {[p['stock'] for p in top_picks]}")
    # _push_phase2_feishu(scored, top_picks)  # 已在 run_daily_workflow 末尾发送完整卡片，此处省略

    return {
        "candidates": candidates,
        "scored": scored,
        "top_picks": top_picks,
        "all_analysts": successful,
        "phase": "route_a",
        "timestamp": datetime.now().isoformat(),
    }


# ── 主流程 ──────────────────────────────────────────────

def run_daily_workflow(dry_run: bool = False, model: str = "volcengine-plan/ark-code-latest", resume: bool = False):
    lock_fh = None
    external_lock_held = os.getenv("DAILY_STOCK_WORKFLOW_LOCK_HELD") == "1"
    if not dry_run and not external_lock_held:
        lock_fh = _try_acquire_workflow_lock()
        if lock_fh is None:
            logger.warning("已有今日选股工作流实例正在运行，本次启动退出，避免重复推送/重复写入")
            return None
    elif external_lock_held:
        logger.info("检测到外层启动器已持有工作流锁，本进程跳过二次加锁")
        marker = _read_daily_push_marker()
        if (
            not resume
            and os.getenv("DAILY_STOCK_WORKFLOW_FORCE_RUN", "0") != "1"
            and marker.get("date") == date.today().isoformat()
            and marker.get("status") == "success"
        ):
            logger.warning("今日选股早报已成功推送过，本次启动退出；如需强制重跑请设置 DAILY_STOCK_WORKFLOW_FORCE_RUN=1")
            return None
    try:
        return _run_daily_workflow_locked(dry_run=dry_run, model=model, resume=resume)
    finally:
        _release_workflow_lock(lock_fh)


def _run_daily_workflow_locked(dry_run: bool = False, model: str = "volcengine-plan/ark-code-latest", resume: bool = False):
    """
    每日工作流主入口
    """
    logger.info("=" * 50)
    logger.info(f"每日选股工作流启动 | PID: {os.getpid()} | 日期: {date.today()} | Dry-run: {dry_run}")
    logger.info("=" * 50)

    # ── 交易日检查：休市日直接跳过（走共享模块） ──────────────────────
    import os as _os
    FORCE_RUN = _os.environ.get("FORCE_RUN", "") == "1"

    is_holiday = False
    try:
        # 共享模块路径：~/.openclaw/agents/shared/trading_calendar.py
        _SHARED_PATH = _os.path.expanduser("~/.openclaw/agents/shared")
        if _SHARED_PATH not in sys.path:
            sys.path.insert(0, _SHARED_PATH)
        from trading_calendar import is_a_share_trading_day
        is_holiday = not is_a_share_trading_day()
        logger.info(f"交易日检查: {'休市日，跳过工作流' if is_holiday else '交易日，继续运行'}")
    except Exception as e2:
        logger.warning(f"无法确认交易日，历次检查失败，默认继续运行: {e2}")
        is_holiday = False
    if FORCE_RUN:
        logger.info("FORCE_RUN=1，强制运行，跳过交易日检查")
        is_holiday = False

    if is_holiday:
        logger.info("今日为非交易日，工作流跳过")
        return None

    check_env()

    # 加载 Tavily API Key（从 openclaw.json env 注入，或直接环境变量）
    if "TAVILY_API_KEY" not in os.environ:
        # 尝试从 openclaw.json 读取
        try:
            import json as _json
            with open(Path.home() / ".openclaw/openclaw.json") as f:
                _cfg = _json.load(f)
            _env = _cfg.get("env", {})
            if "TAVILY_API_KEY" in _env:
                os.environ["TAVILY_API_KEY"] = _env["TAVILY_API_KEY"]
                logger.info("Tavily API Key loaded from openclaw.json")
        except Exception:
            pass

    # ── Phase 1 + Phase 2（resume时跳过Phase 1，直接用checkpoint辩论）─────
    if resume:
        logger.info("Resume 模式：从 checkpoint 恢复辩论，并恢复 Phase 1 市场上下文")
        phase1_results = _ensure_resume_market_context(_load_phase1_context())
        gen = None
        phase2_result = run_phase2_debate(phase1_results, gen=gen, dry_run=dry_run, model=model, resume=resume)
    else:
        # Phase 1: 并行分析师
        logger.info("Phase 1: 启动5大分析师...")
        analysts = [
            NewsAnalyst(),               # 市场新闻/公告
            TechnicalAnalyst(),          # 大盘指数均线
            MarketSentimentAnalyst(),    # 涨跌停/炸板/连板
            LLMWebSearchAnalyst(),      # 媒体舆情（MiniMax联网搜索替代RSS）
            ZtGeneAnalyst(),              # 涨停基因（强势股/涨停股）
        ]

        phase1_results = []
        with ThreadPoolExecutor(max_workers=8) as pool:
            # 并行：7大分析师 + ROE缓存预热
            from llm_scorer import CandidateGenerator
            gen = CandidateGenerator(SKILLS_DIR, OUTPUT_DIR)
            futures = {pool.submit(a.run): a for a in analysts}
            futures[pool.submit(gen.prewarm_cache)] = ("财务预热", "cache_prewarm")

            for future in as_completed(futures):
                item = futures[future]
                if isinstance(item, tuple):
                    name, _ = item
                    try:
                        result = future.result()
                        # prewarm_cache 返回候选股列表，prewarm_tech_cache 返回缓存数量(int)
                        if isinstance(result, int):
                            logger.info(f"  📊 {name} 完成: {result} 只技术数据缓存")
                        else:
                            logger.info(f"  📊 {name} 完成: {len(result)} 只候选股预热")
                    except Exception as e:
                        logger.warning(f"  ⚠️ {name} 异常: {e}")
                else:
                    analyst = item
                    try:
                        result = future.result()
                        phase1_results.append(result)
                        color = result.get("color", "⚪")
                        name = result.get("name", analyst.name)
                        status = result.get("status", "unknown")
                        elapsed = result.get("elapsed", 0)
                        logger.info(f"  {color} {name} [{status}] ({elapsed:.1f}s)")
                    except Exception as e:
                        logger.error(f"  ❌ {analyst.name} 崩溃: {e}")
                        phase1_results.append({"status": "error", "name": analyst.name, "error": str(e)})

        _save_phase1_context(phase1_results)

        # 预获取辩论数据包（K线+资金流+新闻），Phase 2 直接读缓存
        if gen is not None and hasattr(gen, 'candidates') and gen.candidates:
            try:
                from stock_selection_debate.data_fetcher import _prefetch_debate_data
                _prefetch_debate_data(gen.candidates)
            except Exception as e:
                logger.warning(f"辩论数据预获取失败: {e}")

        # ── Phase 2: 完全版辩论 ────────────────────────────────
        logger.info("Phase 2: 选股辩论（替代LLM打分）...")
        phase2_result = run_phase2_debate(phase1_results, gen=gen, dry_run=dry_run, model=model, resume=resume)
        sector_rotation = _extract_sector_rotation(
            phase1_results,
            phase2_result,
            phase2_result.get('ranked_candidates', []),
        )
        if sector_rotation.get("强势板块") or sector_rotation.get("弱势板块"):
            phase2_result["sector_rotation"] = sector_rotation

        # 打印分析师报告摘要
        for r in phase2_result.get('all_analysts', phase1_results):
            if r.get("status") == "success":
                findings = r.get("findings", "（无内容）")
                logger.info(f"\n{r.get('color')} {r.get('name')} 报告:\n{findings}\n")

    # ── QMT HTTP 预热等待（最多30秒）────────────
    import urllib.request
    qmt_ready = False
    for _ in range(30):
        try:
            with urllib.request.urlopen("http://127.0.0.1:8080/health", timeout=3) as r:
                if json.loads(r.read())["status"] == "ok":
                    qmt_ready = True
                    break
        except:
            pass
        time.sleep(1)
    if not qmt_ready:
        logger.warning("QMT HTTP 未就绪，回测可能失败")

    # ── Phase 3a: 选股回测（候选股本身好不好）───────────
    logger.info("Phase 3a: 选股回测...")
    top_picks = phase2_result.get('top_picks', [])

    backtest_selection = run_backtest_selection(top_picks) if top_picks else {"type": "selection", "status": "no_candidates"}

    # ── Phase 3b: 策略回测（完整交易策略）───────────────
    logger.info("Phase 3b: 策略回测...")
    backtest_strategy = run_backtest_strategy(top_picks) if top_picks else {"type": "strategy", "status": "no_candidates"}
    _attach_top_pick_backtests(phase2_result, backtest_selection, backtest_strategy)

    # ── 生成今日报告 ───────────────────────────────────
    report = {
        "date": date.today().isoformat(),
        "phase1": phase1_results,
        "phase2": phase2_result,
        "phase3_selection": backtest_selection,
        "phase3_strategy": backtest_strategy,
        "timestamp": datetime.now().isoformat(),
    }
    top5_failure_reason = ""
    if not top_picks:
        top5_failure_reason = "Top5为空：基金经理裁决未产生任何 BUY/WATCH 可买候选，已阻止早报卡片推送"
        report["status"] = "failed"
        report["failure_reason"] = top5_failure_reason
        logger.error(f"今日选股早报生成失败: {top5_failure_reason}")
    else:
        report["status"] = "success"

    report_file = OUTPUT_DIR / f"daily_report_{date.today().strftime('%Y%m%d')}.json"
    report_tmp = report_file.with_name(report_file.name + ".tmp")
    with open(report_tmp, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
        f.flush()
    report_tmp.replace(report_file)
    logger.info(f"报告已保存: {report_file}")
    try:
        from selection_memory import append_daily_selection_memory
        memory_count = append_daily_selection_memory(report, backtest_selection, backtest_strategy)
        report["selection_memory"] = {
            "path": str(OUTPUT_DIR / "selection_memory.jsonl"),
            "records_written": memory_count,
        }
        with open(report_tmp, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
            f.flush()
        report_tmp.replace(report_file)
        logger.info(f"选股决策记忆已写入: {memory_count} 条")
    except Exception as e:
        logger.warning(f"选股决策记忆写入失败: {e}")

    # ── 飞书推送早报卡片 ───────────────────────────────
    webhook = os.getenv("FEISHU_WEBHOOK_URL")
    if webhook and os.getenv("DAILY_STOCK_WORKFLOW_SEND_FEISHU", "1") == "1":
        if top5_failure_reason:
            if _daily_report_failure_already_notified(top5_failure_reason):
                logger.warning("今日选股早报生成失败已通知过，本次跳过重复失败通知")
            else:
                msg = (
                    f"❌ 今日选股早报生成失败\n"
                    f"日期: {date.today().isoformat()}\n"
                    f"原因: {top5_failure_reason}\n"
                    f"候选股: {len(phase2_result.get('ranked_candidates', []) or [])} 只\n"
                    f"处理: 已禁止推送空 Top5 早报卡片，请修复后使用 --resume 断点续跑。"
                )
                notified = feishu_push_text(msg, webhook)
                _mark_daily_report_push_status(
                    report,
                    "failed",
                    reason=top5_failure_reason,
                    notified=bool(notified),
                    notified_at=datetime.now().isoformat() if notified else "",
                )
            logger.error("已阻止空 Top5 早报卡片推送")
            return report

        # 收集辩论/数据质量/执行统计
        candidates = phase2_result.get('candidates', [])
        ranked = phase2_result.get('ranked_candidates', [])
        
        # 辩论胜负统计：从 final_decision 文书中解析多空裁决结果
        def _parse_winner(s):
            dec = s.get('final_decision', '')
            # 多方胜：明确"多方论据更具说服力"
            if re.search(r'多方论据.*更具说服力', dec):
                return 'bull'
            # 空方胜：明确"空方论据更具说服力"
            if re.search(r'空方论据.*更具说服力', dec):
                return 'bear'
            # 其余均为平局（多空均有支撑，无压倒性优势）
            return 'tie'
        debate_stats = {'bull': 0, 'bear': 0, 'tie': 0}
        for s in ranked:
            w = _parse_winner(s)
            debate_stats[w] += 1
        
        # 数据质量：优先使用 Phase2 的真实缺口标记；PE/RSI 只有确实取到时才展示覆盖率
        pe_ok = sum(1 for s in ranked if s.get('pe') is not None and s.get('pe') != '')
        rsi_ok = sum(1 for s in ranked if s.get('rsi') is not None and s.get('rsi') != '')
        data_quality = {'summary': phase2_result.get('data_quality_summary') or {}}
        if pe_ok:
            data_quality['pe'] = f"{pe_ok}/{len(ranked)}"
        if rsi_ok:
            data_quality['rsi'] = f"{rsi_ok}/{len(ranked)}"
        
        # 执行统计
        source_counts = {}
        for s in ranked:
            source = s.get('decision_source') or ('Structured' if str(s.get('final_decision', '')).startswith('[Structured]') else '')
            if source:
                source_counts[source] = source_counts.get(source, 0) + 1
        # ★ 6-11 修复：实际跑的模型汇总（从每个 ranked 里的 decision_models 字段统计）
        # 早报卡片不再误显"火山引擎"：拉取每只股票各节点实际使用的模型
        actual_models_summary: Dict[str, int] = {}
        actual_models_examples: Dict[str, str] = {}
        for s in ranked:
            for node, m in (s.get('decision_models') or {}).items():
                # 抹平别名：Structured:GPT-5.5 -> GPT-5.5；Structured:MiniMax-M3 -> MiniMax-M3
                short = str(m).split(':')[-1] if m else 'unknown'
                key = f'{node}={short}'
                actual_models_summary[key] = actual_models_summary.get(key, 0) + 1
                actual_models_examples.setdefault(key, short)
        # 生成卡片展示串：按节点聚合，避免同一模型在多个节点上重复显示成
        # "model×N/N · model×N/N ..."。
        node_order = ['bull', 'bear', 'judge', 'aggressive', 'conservative', 'neutral', 'pm']
        node_model_counts: Dict[str, Dict[str, int]] = {}
        for s in ranked:
            for node, m in (s.get('decision_models') or {}).items():
                short = str(m).split(':')[-1] if m else 'unknown'
                node_model_counts.setdefault(node, {})
                node_model_counts[node][short] = node_model_counts[node].get(short, 0) + 1
        display_parts = []
        debate_nodes = node_order[:-1]
        debate_model_counts: Dict[str, int] = {}
        debate_total = 0
        for node in debate_nodes:
            for model_name, count in (node_model_counts.get(node) or {}).items():
                debate_model_counts[model_name] = debate_model_counts.get(model_name, 0) + count
                debate_total += count
        if debate_model_counts:
            debate_desc = ','.join(f"{m}×{c}" for m, c in sorted(debate_model_counts.items(), key=lambda x: -x[1]))
            display_parts.append(f"辩论节点:{debate_desc}")
        pm_counts = node_model_counts.get('pm') or {}
        if pm_counts:
            pm_desc = ','.join(f"{m}×{c}/{len(ranked)}" for m, c in sorted(pm_counts.items(), key=lambda x: -x[1]))
            display_parts.append(f"基金经理:{pm_desc}")
        if display_parts:
            actual_models_display = ' · '.join(display_parts)
        elif source_counts:
            actual_models_display = '结构化裁决（模型明细未记录）'
        else:
            actual_models_display = f'{model}（模型明细未记录）'
        exec_stats = {
            'total': len(ranked),
            'model': actual_models_display,
            'decision_sources': source_counts,
        }
        
        if _daily_report_already_pushed(report):
            logger.warning("今日选股早报已成功推送过，本次跳过飞书推送，避免重复消息")
        else:
            pushed = _send_daily_report_card(
                phase1_results, phase2_result, backtest_selection, backtest_strategy, webhook,
                debate_stats, data_quality, exec_stats,
            )
            if pushed:
                _mark_daily_report_pushed(report)
    elif webhook:
        logger.info("DAILY_STOCK_WORKFLOW_SEND_FEISHU=0，跳过脚本内飞书推送，交由 cron delivery 投递")

    logger.info("✅ 每日工作流完成")
    return report


def _parse_signal(s):
    # 优先用 structured 输出字段（signal），避免与文本解析不一致
    sig_field = s.get('signal', '')
    if sig_field in ('MODEL_FAILED', 'PENDING_RETRY'):
        return sig_field
    if sig_field in ('BUY', 'WATCH', 'AVOID'):
        return sig_field
    # Fallback：解析文本 final_decision
    dec = s.get('final_decision', '')
    dec_upper = dec.upper()
    # 解析失败 / 系统异常标记：排除在 Top5 之外
    if '异常:' in dec or '辩论系统异常' in dec or dec.startswith('异常:'):
        return 'AVOID'
    if any(neg in dec_upper for neg in ['不给BUY', '不支撑BUY', '不推荐BUY', '不建议BUY', '不足以BUY']):
        return 'WATCH'
    import re as _re_module
    m_signal = _re_module.search(r'\*\*最终信号\*\*:\s*(BUY|WATCH|AVOID)', dec)
    if m_signal:
        return m_signal.group(1)
    if '仓位建议' in dec and '0%' in dec:
        return 'AVOID'
    return 'WATCH'


def _parse_pos(s):
    """Return position ratio as a 0-100 percentage for card display."""
    raw = s.get('position_ratio')
    has_percent = False
    if raw in (None, ''):
        dec = s.get('final_decision', '') or ''
        m = re.search(r'position_ratio\s*[=：:]\s*([0-9.]+)\s*(%)?', dec, re.IGNORECASE)
        if not m:
            return 0
        raw = m.group(1)
        has_percent = bool(m.group(2))
    elif isinstance(raw, str):
        has_percent = '%' in raw
        raw = raw.strip().rstrip('%')
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return 0
    pct = val if has_percent or val > 1 else val * 100
    return max(0, min(100, round(pct)))


def _confidence_value(s):
    for key in ('confidence', 'total_score', 'final_score', 'score'):
        try:
            value = s.get(key)
            if value not in (None, ''):
                return float(value)
        except (TypeError, ValueError):
            continue
    return 0


def _buy_score_value(s):
    for key in ('buy_score', 'long_score', 'ranking_score', 'final_score', 'quant_base_score'):
        try:
            value = s.get(key)
            if value not in (None, ''):
                return max(0, min(100, float(value)))
        except (TypeError, ValueError):
            continue
    # 兼容旧日报：旧数据没有 buy_score 时，用 signal+confidence 临时折算。
    sig = _parse_signal(s)
    conf = _confidence_value(s)
    if sig == 'BUY':
        return max(70, conf)
    if sig == 'WATCH':
        return min(max(55, conf), 69)
    return min(conf, 54)


def _pool_score_value(s):
    try:
        value = s.get('pool_score')
        if value not in (None, ''):
            return float(value)
    except (TypeError, ValueError):
        pass
    return 0


def _pool_rank_value(s):
    try:
        rank = s.get('pool_rank')
        if rank not in (None, ''):
            return int(rank)
    except (TypeError, ValueError):
        pass
    return 999999


def _strategy_return_map(backtest_strategy):
    returns = {}
    for trade in (backtest_strategy or {}).get('trades', []) or []:
        stock = trade.get('stock')
        if not stock:
            continue
        ret = trade.get('return_pct')
        try:
            returns[str(stock)] = float(ret) if ret not in (None, '') else 0.0
        except (TypeError, ValueError):
            returns[str(stock)] = 0.0
    return returns


def _top5_sort_key(s, strategy_returns=None):
    strategy_returns = strategy_returns or {}
    stock = str(s.get('stock') or '')
    return (
        -_buy_score_value(s),
        -float(s.get('ranking_score') or s.get('final_score') or 0.0),
        -_pool_score_value(s),
        -float(strategy_returns.get(stock, 0.0) or 0.0),
        _pool_rank_value(s),
    )


def _select_display_top5(ranked, target=5, strategy_returns=None):
    """BUY first, then WATCH candidates; sort each group by buy_score."""
    def _has_buyable_kline(s):
        flags = set(s.get('data_quality_flags') or [])
        return not flags.intersection({'KLINE_MISSING', 'KLINE_SHORT'})

    buy_stocks = sorted(
        [s for s in ranked if _parse_signal(s) == 'BUY' and _has_buyable_kline(s)],
        key=lambda x: _top5_sort_key(x, strategy_returns),
    )
    watch_stocks = sorted(
        [
            s for s in ranked
            if _parse_signal(s) == 'WATCH'
            and _has_buyable_kline(s)
            and _parse_pos(s) > 0
        ],
        key=lambda x: _top5_sort_key(x, strategy_returns),
    )
    seen, deduped = set(), []
    for s in buy_stocks + watch_stocks:
        key = s.get('stock') or f"{s.get('name', '')}:{id(s)}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(s)
        if len(deduped) >= target:
            break
    return deduped[:target]


def _tradingview_exchange_symbol(stock_code: str) -> str:
    code = str(stock_code or "").strip().upper().split(".")[0]
    if not code:
        return ""
    if code.startswith(("000", "001", "002", "003", "300", "301")):
        exchange = "SZSE"
    elif code.startswith(("920", "8", "4")):
        exchange = "BSE"
    else:
        exchange = "SSE"
    return f"{exchange}:{code}"


def _tradingview_chart_url(stock_code: str) -> str:
    symbol = _tradingview_exchange_symbol(stock_code)
    if not symbol:
        return ""
    return f"https://cn.tradingview.com/chart/?symbol={symbol.replace(':', '%3A')}"


def _resolve_report_top5_for_display(top_picks, ranked, target=5, strategy_returns=None):
    """Use persisted report Top5 order; enrich rows from ranked_candidates for card details."""
    if not top_picks:
        return _select_display_top5(ranked, target, strategy_returns)

    ranked_by_stock = {}
    for row in ranked or []:
        stock = str(row.get("stock") or "")
        if stock and stock not in ranked_by_stock:
            ranked_by_stock[stock] = row

    display_rows = []
    seen = set()
    for pick in top_picks or []:
        stock = str(pick.get("stock") or "")
        key = stock or f"{pick.get('name', '')}:{id(pick)}"
        if key in seen:
            continue
        seen.add(key)
        merged = dict(ranked_by_stock.get(stock, {}))
        merged.update(pick)
        display_rows.append(merged)
        if len(display_rows) >= target:
            break
    return display_rows


def _attach_top_pick_backtests(phase2_result, backtest_selection, backtest_strategy):
    top_picks = phase2_result.get('top_picks') or []
    if not top_picks:
        return
    selection_by_stock = {
        str(item.get('stock')): item
        for item in (backtest_selection or {}).get('stocks', []) or []
        if item.get('stock')
    }
    strategy_by_stock = {
        str(item.get('stock')): item
        for item in (backtest_strategy or {}).get('trades', []) or []
        if item.get('stock')
    }
    for pick in top_picks:
        stock = str(pick.get('stock') or '')
        if stock in selection_by_stock:
            pick['selection_backtest'] = selection_by_stock[stock]
        if stock in strategy_by_stock:
            pick['strategy_backtest'] = strategy_by_stock[stock]


def _extract_sector_rotation(phase1_results, phase2_result, ranked):
    """Extract hot/cold sectors from structured fields, news text, then candidate sector frequency."""
    hot, cold = [], []
    stop_words = {
        "A股", "三大指数", "上证指数", "深证成指", "创业板指", "科创50指数",
        "两市", "个股", "市场", "公司", "今日", "昨日", "本周", "资金", "股价",
    }

    def _add_many(target, values):
        if not values:
            return
        if isinstance(values, str):
            values = re.split(r"[、,，/| ]+", values)
        for value in values:
            text = str(value).strip()
            if not text:
                continue
            if "个股" in text:
                continue
            text = re.sub(r"^[\d.、\-\s]+", "", text)
            text = re.sub(r"(?:等.*)?(?:板块|行业|概念).*$", "", text).strip()
            text = re.sub(r"等.*$", "", text).strip()
            if not text or text in stop_words or len(text) > 12:
                continue
            if any(word in text for word in ("指数", "个股", "市场", "涨幅", "跌幅", "收跌", "收涨")):
                continue
            if text not in target:
                target.append(text)

    def _extract_from_text(text, keywords):
        found = []
        for sentence in re.split(r"[。；;\n]", text or ""):
            for keyword in keywords:
                if keyword not in sentence:
                    continue
                prefix = sentence.split(keyword, 1)[0]
                prefix = re.split(r"[，,：:]", prefix)[-1]
                _add_many(found, prefix)
        return found

    sector_rot = phase2_result.get("sector_rotation") or {}
    _add_many(hot, sector_rot.get("强势板块") or sector_rot.get("hot") or phase2_result.get("hot_sectors"))
    _add_many(cold, sector_rot.get("弱势板块") or sector_rot.get("cold") or phase2_result.get("cold_sectors"))

    if len(hot) < 3 or len(cold) < 3:
        news_text = "\n".join(
            str(r.get("findings", ""))
            for r in phase1_results or []
            if r.get("name") in ("新闻分析师", "媒体舆情分析师", "情绪分析师")
        )
        _add_many(hot, _extract_from_text(news_text, ("逆势走强", "涨幅居前", "领涨", "走强", "活跃")))
        _add_many(cold, _extract_from_text(news_text, ("领跌", "跌幅居前", "走弱", "下挫")))

    if len(hot) < 3:
        sector_scores = {}
        for s in ranked or []:
            sector = str(s.get("sector", "") or "").strip()
            if not sector or sector in ("未知", "暂无数据"):
                continue
            weight = 1.0
            if _parse_signal(s) == "BUY":
                weight += 2.0
            elif _parse_signal(s) == "WATCH" and _parse_pos(s) > 0:
                weight += 1.0
            weight += _confidence_value(s) / 100.0
            sector_scores[sector] = sector_scores.get(sector, 0.0) + weight
        ranked_sectors = [
            sector for sector, _ in sorted(sector_scores.items(), key=lambda item: -item[1])
        ]
        _add_many(hot, ranked_sectors)

    return {
        "强势板块": hot[:3],
        "弱势板块": cold[:3],
    }


def _format_market_news(findings: str, max_items: int = 4) -> str:
    """Keep complete numbered news items instead of cutting the feed mid-item."""
    text = (findings or "").strip()
    if not text:
        return ""
    starts = list(re.finditer(r"(?m)^\s*\d+\.\s+", text))
    if not starts:
        return text

    items = []
    for idx, match in enumerate(starts[:max_items]):
        end = starts[idx + 1].start() if idx + 1 < len(starts) else len(text)
        item = text[match.start():end].strip()
        item = re.sub(r"\n\s*📍来源:", " | 来源:", item)
        item = re.sub(r"\s+", " ", item).strip()
        if item:
            items.append(item)
    return "\n".join(items)


def _summarize_news_one_line(findings: str, max_len: int = 300) -> str:
    """早报 v2：LLM 总结为 300 字内的一句话舆情研判（不再列标题）"""
    text = (findings or "").strip()
    if not text:
        return "今日无重要舆情"

    # 提取纯标题列表
    titles = []
    for line in text.split("\n"):
        line2 = re.sub(r"^\s*\d+\.\s*", "", line.strip())
        line2 = re.sub(r"📍来源:.*$", "", line2).strip()
        line2 = re.sub(r"来源:.*$", "", line2).strip()
        if line2 and len(line2) > 6 and not line2.startswith("【导读】"):
            title = line2.split("📍")[0].strip()
            if title:
                titles.append(title[:80])

    if not titles:
        return "今日无重要舆情"

    # 优先用统一 provider 总结：GPT-5.5 主用，MiniMax M3 兜底。
    import logging as _logging
    _log = _logging.getLogger("daily_stock_workflow")
    try:
        from stock_selection_debate.providers import call_llm_with_fallback
        used_model = [""]
        prompt = (
            "你是A股短线舆情研判助手。请根据以下今日重要新闻标题，写 150-280 字的一段话，判断未来1-3个交易日的市场风险偏好和题材方向。"
            "可多句，不要列表/换行分段。结构：先说整体偏向（偏多/偏空/中性），再点出2~3个核心主题、可能走强/走弱板块，"
            "最后给出短线交易启示（关注哪些方向、回避哪些方向）。不要写泛泛宏观评论。\n\n"
            "新闻标题：\n"
            + "\n".join([f"{i+1}. {t}" for i, t in enumerate(titles[:15])])
            + "\n\n输出（150-280 字，不要列表/分段）："
        )
        summary = call_llm_with_fallback(
            prompt=prompt,
            model=os.getenv("NEWS_SENTIMENT_SUMMARY_MODEL", "volcengine-plan/ark-code-latest"),
            fallback_model=os.getenv("NEWS_SENTIMENT_SUMMARY_FALLBACK_MODEL", "openai/gpt-5.5"),
            timeout=int(os.getenv("NEWS_SENTIMENT_SUMMARY_TIMEOUT", "120")),
            retries=int(os.getenv("NEWS_SENTIMENT_SUMMARY_RETRIES", "3")),
            max_tokens=int(os.getenv("NEWS_SENTIMENT_SUMMARY_MAX_TOKENS", "5000")),
            thinking_budget=int(os.getenv("NEWS_SENTIMENT_SUMMARY_THINKING_BUDGET", "4000")),
            fallback_thinking_budget=int(os.getenv("NEWS_SENTIMENT_SUMMARY_FALLBACK_THINKING_BUDGET", "4000")),
            actual_model_out=used_model,
        ).strip()
        summary = re.sub(r"\s+", " ", summary).strip()
        if summary:
            if len(summary) > max_len:
                summary = summary[:max_len-2] + ".."
            _log.info(f"[新闻舆情] LLM 总结成功 model={used_model[0] or 'unknown'}, {len(summary)} chars")
            return summary
        _log.error("[新闻舆情] LLM 总结返回空，降级到关键词拼接")
    except Exception as e:
        _log.error(f"[新闻舆情] LLM 总结失败: {type(e).__name__}: {e}")

    # 降级：关键词拼接
    short_titles = [t.split("：")[0].split(":")[0].strip() for t in titles[:3] if t]
    keywords = " / ".join(short_titles[:3])
    s = f"今日 {len(titles)} 条舆情，关注：{keywords}"
    if len(s) > max_len:
        s = s[:max_len-2] + ".."
    return s


def _send_daily_report_card(phase1_results, phase2_result, backtest_selection, backtest_strategy, webhook,
                           debate_stats=None, data_quality=None, exec_stats=None):
    """每日选股早报飞书卡片 - 辩论版（短线增强版）"""
    import datetime, re as re_module
    today = datetime.date.today().strftime("%Y-%m-%d")
    _run_start_time = datetime.datetime.now()  # #17 早报质量自评起算点

    top_picks = phase2_result.get('top_picks', [])
    phase = phase2_result.get('phase', 'unknown')
    route_tag = 'Route A' if phase == 'route_a' else '辩论模式'

    # ── 收集所有候选股代码（从 phase2_result 提取）───
    # 从 ranked_candidates 统一取 top5（用 combined_top5 逻辑，与卡片展示保持一致）
    ranked = phase2_result.get('ranked_candidates', [])


    buy_stocks  = sorted([s for s in ranked if _parse_signal(s) == 'BUY'], key=lambda x: -_buy_score_value(x))
    watch_stocks = sorted([s for s in ranked if _parse_signal(s) == 'WATCH'], key=lambda x: -_buy_score_value(x))
    avoid_stocks = sorted([s for s in ranked if _parse_signal(s) == 'AVOID'], key=lambda x: -_buy_score_value(x))[:5]
    avoid_count  = len([s for s in ranked if _parse_signal(s) == 'AVOID'])

    TARGET = 5
    combined_top5 = _resolve_report_top5_for_display(top_picks, ranked, TARGET)

    # 统一使用 ranked_candidates 的字段，不再区分 scored / top_picks / candidates
    all_codes = set()
    for s in ranked:  # 所有候选股都纳入价格获取范围
        if s.get('stock'):
            all_codes.add(s.get('stock'))

    # ── 价格获取：QXShare 单股查询(加后缀) → mx-data 兜底 → 无价格也行 ──
    price_cache = {}
    if all_codes:
        codes = sorted(all_codes)
        
        def _add_suffix(code):
            """自动加后缀"""
            if code.startswith(("920", "8", "4")):
                return f"{code}.BJ"
            if code.startswith(('000','001','002','003','300','301')):
                return f"{code}.SZ"
            return f"{code}.SH"
        
        # ① QXShare HTTP API（单股查询，5s超时/只）
        try:
            import urllib.request, json as _json
            def _qmt_get(code, timeout=5):
                full = _add_suffix(code)
                url = f"http://127.0.0.1:8080/market_data3?stock={full}&period=1d&count=2"
                try:
                    req = urllib.request.Request(url)
                    with urllib.request.urlopen(req, timeout=timeout) as r:
                        result = _json.loads(r.read().decode())
                except Exception:
                    return None
                if not result or not result.get("success"):
                    return None
                close_data = result.get("data", {}).get("close", {})
                dates = sorted(close_data.keys(), reverse=True)
                if len(dates) < 2:
                    return None
                curr = close_data[dates[0]].get(full)
                prev = close_data[dates[1]].get(full)
                if curr and prev and float(prev) > 0:
                    pct = (float(curr) - float(prev)) / float(prev) * 100
                    return {"close": float(curr), "prev_close": float(prev), "pct": round(pct, 2)}
                return None
            
            for code in codes[:50]:  # 最多50只
                p = _qmt_get(code, timeout=5)
                if p:
                    price_cache[code] = p
            logger.info(f"QXShare 价格获取: {len(price_cache)}/{min(len(codes),50)} 只")
        except Exception as e:
            logger.warning(f"QXShare 价格获取失败: {e}")
        
        # ② mx-data 兜底（仅补充 QXShare 未获取到的）
        missing = [c for c in codes if c not in price_cache]
        if missing:
            try:
                import os as _os
                MXData = _load_mx_data_class()
                api_key = _os.environ.get("MX_APIKEY") or _os.environ.get("MINIMAX_API_KEY", "")
                tool = MXData(api_key=api_key)
                for i in range(0, min(len(missing), 30), 10):
                    batch = missing[i:i+10]
                    query_str = " ".join([f"{c} 昨收价 涨跌幅" for c in batch])
                    try:
                        result = tool.query(query_str)
                        tables, _, _, _ = tool.parse_result(result)
                        for t in tables or []:
                            for row in t.get("rows", []):
                                code = row.get("代码", row.get("code", ""))
                                if not code:
                                    for k in row:
                                        if any(x in str(k) for x in ["000","002","300","688","601","603"]):
                                            code = k
                                            break
                                if code and code in missing and code not in price_cache:
                                    close_str = row.get("昨收") or row.get("最新价") or ""
                                    pct_str = row.get("涨跌幅") or "0"
                                    try:
                                        close = float(str(close_str).replace(",", "")) if close_str else None
                                    except:
                                        close = None
                                    try:
                                        pct = float(str(pct_str).replace("%", "").replace(",", "")) if pct_str else 0
                                    except:
                                        pct = 0
                                    if close is not None:
                                        price_cache[code] = {"close": close, "prev_close": close, "pct": pct}
                    except Exception as e:
                        logger.warning(f"mx-data 批量价格查询失败: {e}")
                logger.info(f"mx-data 兜底完成: {len(missing)} 只中补充 {len([c for c in missing if c in price_cache])} 只")
            except Exception as e:
                logger.warning(f"mx-data 价格兜底失败: {e}")
        
        logger.info(f"价格获取完成: {len(price_cache)}/{min(len(codes),50)} 只")

    # ── 解析7大分析师数据 ──
    sentiment_label = '未知'
    sentiment_color = 'grey'
    pe_info = ''
    news_analyst_news = ''
    limit_up = hot_sector = bulls_bears = None

    for r in phase1_results:
        name = r.get('name', '')
        findings = r.get('findings', '') or ''
        if name == '情绪分析师':
            sentiment_label = findings.split('\n')[0] if findings else '未知'
        elif name == '市场情绪分析师':
            # 提取情绪阶段标签（分歧/亢奋/冰点/中性）
            m_phase = re_module.search(r'市场情绪[:：]?\s*\*?\*?([^*\n]+?)(?:\*|\|)', findings)
            if m_phase:
                sentiment_label = m_phase.group(1).strip()
            m = re_module.search(r'涨停(\d+)家', findings)
            if m:
                limit_up = m.group(1)
            m2 = re_module.search(r'炸板率([\d.]+)%', findings)
            if m2:
                hot_sector = m2.group(1)
            m3 = re_module.search(r'连板高度(\d+)板', findings)
            if m3:
                bulls_bears = f"连板{m3.group(1)}板"
        elif name == '基本面分析师':
            pe_info = findings.strip()
        elif name == '涨停基因分析师' and findings:
            # 解析 ZtGeneAnalyst findings，提取统计数字供市场情绪参考
            m_zt = re_module.search(r'今日涨停: (\d+) 只', findings)
            if m_zt and limit_up is None:
                limit_up = m_zt.group(1)
        elif name == '新闻分析师' and findings:
            # 早报 v2：新闻舆情压成一句话，不再单列 15 条
            news_analyst_news = _summarize_news_one_line(findings, max_len=300)

    if '乐观' in sentiment_label or '偏多' in sentiment_label:
        sentiment_color = 'green'
    elif '谨慎' in sentiment_label or '偏空' in sentiment_label or '防御' in sentiment_label:
        sentiment_color = 'red'
    else:
        sentiment_color = 'blue'

    # ── 高级 header 颜色（#9）：purple/orange/yellow 三个额外信号 ──
    # purple: 系统性机会（BUY 数量 >= 3）
    # orange: 强分歧（BUY + AVOID 都多，意见不集中）
    # yellow: 数据质量差（缺口 > 30%）
    n_buy = len([s for s in ranked if _parse_signal(s) == 'BUY'])
    n_avoid = len([s for s in ranked if _parse_signal(s) == 'AVOID'])
    # 数据完整度（计算 affected 比例）
    dq_flags = []
    for s in ranked:
        dq_flags.extend(s.get('data_quality_flags') or [])
    total_dq = len(dq_flags) / max(len(ranked), 1)
    if n_buy >= 3 and sentiment_color in ('blue', 'green'):
        sentiment_color = 'purple'  # 系统性买入机会
    elif n_buy >= 1 and n_avoid >= max(10, n_buy * 2):
        sentiment_color = 'orange'  # 强分歧
    elif total_dq > 0.3:
        sentiment_color = 'yellow'  # 数据质量差

    # ── 信号分组（从 final_decision 解析真实信号，而非 signal 字段）───
    ranked = phase2_result.get('ranked_candidates', [])
    strategy_returns = _strategy_return_map(backtest_strategy)

    buy_stocks  = sorted([s for s in ranked if _parse_signal(s) == 'BUY'],  key=lambda x: _top5_sort_key(x, strategy_returns))
    watch_stocks = sorted([s for s in ranked if _parse_signal(s) == 'WATCH'], key=lambda x: _top5_sort_key(x, strategy_returns))
    avoid_stocks = sorted([s for s in ranked if _parse_signal(s) == 'AVOID'], key=lambda x: _top5_sort_key(x, strategy_returns))[:5]
    avoid_count  = len([s for s in ranked if _parse_signal(s) == 'AVOID'])

    # ── Top5 补全逻辑：BUY 不足5只时从 WATCH 补；同信号内按 buy_score 排序 ──
    TARGET = 5
    combined_top5 = _resolve_report_top5_for_display(top_picks, ranked, TARGET, strategy_returns)

    if buy_stocks:
        action_emoji, action_text = '🟢', f'买入信号 ({len(buy_stocks)}只)'
    elif watch_stocks:
        action_emoji, action_text = '🟡', f'观望信号 ({len(watch_stocks)}只)'
    else:
        action_emoji, action_text = '🔴', '今日空仓'

    # ── 回测数据 ──
    sel = backtest_selection or {}
    sel_stocks = sel.get('stocks', [])
    strat = backtest_strategy or {}
    strat_trades = strat.get('trades', [])
    strat_valid = [t for t in strat_trades if t.get('return_pct', 0) != 0]

    def _fmt_price(value):
        try:
            return f"{float(value):.2f}"
        except (TypeError, ValueError):
            return ""

    def _get_backtest_text(stock_code):
        sel_item = next((x for x in sel_stocks if x.get('stock') == stock_code), None)
        strat_item = next((x for x in strat_trades if x.get('stock') == stock_code), None)
        parts = []

        if sel_item and sel_item.get('return_pct') is not None:
            parts.append(f"候选{float(sel_item.get('return_pct')):+.2f}%")

        if strat_item:
            status = strat_item.get('status')
            entry_date = strat_item.get('entry_date') or ""
            entry_price = _fmt_price(strat_item.get('entry_price'))
            exit_date = strat_item.get('exit_date') or ""
            exit_price = _fmt_price(strat_item.get('exit_price'))
            ret = strat_item.get('return_pct')

            if ret is not None:
                left = f"{entry_date} ¥{entry_price}" if entry_date and entry_price else (entry_date or f"¥{entry_price}")
                right = f"{exit_date} ¥{exit_price}" if exit_date and exit_price else (exit_date or f"¥{exit_price}")
                parts.append(f"策略{left}→{right} {float(ret):+.2f}%")
            elif status == 'holding':
                left = f"{entry_date} ¥{entry_price}" if entry_date and entry_price else (entry_date or f"¥{entry_price}")
                parts.append(f"策略{left}→持有中")
            elif status == 'no_trade':
                reason = strat_item.get('error') or strat_item.get('reason_type') or "未成交"
                parts.append(f"策略{reason}")

        return "📊" + " | ".join(parts) if parts else ""

    # ── 大盘数据解析（方向①）——直接查QMT HTTP（绕过mx-data次数限制）──
    sh_chg = sz_chg = cy_chg = None
    hs300_ret = None
    total_turnover_str = None  # 两市总成交金额（万Y）
    live_market_enabled = os.getenv("OPENCLAW_DISABLE_LIVE_MARKET") != "1"
    try:
        if live_market_enabled:
            def _get_index_pct(code):
                import urllib.request as _ur
                url = f'http://127.0.0.1:8080/market_data3?stock={code}&period=1d&count=2'
                req = _ur.Request(url)
                with _ur.urlopen(req, timeout=5) as r:
                    data = json.loads(r.read())
                    close_data = data.get('data', {}).get('close', {})
                    dates = sorted(close_data.keys(), reverse=True)
                    if len(dates) >= 2:
                        curr = close_data[dates[0]].get(code)
                        prev = close_data[dates[1]].get(code)
                        if curr and prev and float(prev) > 0:
                            return (float(curr) - float(prev)) / float(prev) * 100
                return None
            sh_chg_raw  = _get_index_pct('000001.SH')
            sz_chg_raw  = _get_index_pct('399001.SZ')
            cy_chg_raw  = _get_index_pct('399006.SZ')
            hs300_raw   = _get_index_pct('000300.SH')
            if sh_chg_raw  is not None: sh_chg  = sh_chg_raw
            if sz_chg_raw  is not None: sz_chg  = sz_chg_raw
            if cy_chg_raw  is not None: cy_chg  = cy_chg_raw
            if hs300_raw   is not None: hs300_ret = hs300_raw
    except Exception:
        pass

    # ── 两市总成交金额（查 mx-data）──
    try:
        if live_market_enabled:
            import subprocess as _sp
            mx_script = Path(SKILLS_DIR) / "mx-data" / "mx_data.py"
            if not mx_script.exists():
                raise FileNotFoundError(f"mx_data.py not found: {mx_script}")
            _mx_out = _sp.run(
                [sys.executable, str(mx_script), '沪深A股今日总成交金额'],
                capture_output=True, text=True, timeout=120,
                env={**os.environ, 'MX_APIKEY': os.environ.get('MX_APIKEY', '')},
            )
            if _mx_out.returncode == 0:
                import re as _re
                m = _re.search(r'([\d.]+)\s*万亿', _mx_out.stdout)
                if m:
                    total_turnover_str = f"{m.group(1)}万亿"
                else:
                    import logging as _lg
                    _lg.getLogger("daily_stock_workflow").warning(f"[大盘环境] mx-data 返回但正则不匹配: stdout tail={_mx_out.stdout[-200:]}")
    except Exception as e:
        import logging as _lg
        _lg.getLogger("daily_stock_workflow").warning(f"[大盘环境] mx-data 子进程异常: {e}")
        pass

    # ── 板块强弱解析（方向①）──
    sector_source = phase1_results or phase2_result.get('all_analysts', [])
    sector_rot = _extract_sector_rotation(sector_source, phase2_result, ranked)
    if sector_rot.get("强势板块") or sector_rot.get("弱势板块"):
        phase2_result["sector_rotation"] = sector_rot
    hot_sectors_str = '、'.join(sector_rot.get("强势板块", [])[:3])
    cold_sectors_str = '、'.join(sector_rot.get("弱势板块", [])[:3])

    # ── 时间戳（方向⑥）──
    report_timestamp = phase2_result.get('generated_at', '')
    if report_timestamp:
        try:
            ts_dt = datetime.datetime.fromisoformat(report_timestamp.replace('Z', '+00:00'))
            time_str = ts_dt.strftime('%H:%M')
        except Exception:
            time_str = ''
    else:
        time_str = ''
    time_label = f' | {time_str}生成' if time_str else ''

    # ── 单只股票短线卡片行（方向②③⑤）───
    def _short_reason(reason_str: str, max_len: int = 60) -> str:
        """#6 理由短摘要：限 60 字，不在数字/量词中间截断。"""
        if not reason_str:
            return ''
        s = re_module.sub(r'\s+', ' ', str(reason_str)).strip()
        if len(s) <= max_len:
            return s
        # 优先在逗号/句号/分号处截断
        for sep in ['，', '；', '。', ',', ';', '.']:
            idx = s.find(sep, 0, max_len)
            if 30 <= idx <= max_len:
                return s[:idx+1] + '..'
        # 兑底在 max_len-2 截断
        return s[:max_len-2] + '..'

    def _fmt_stock_row(s, label_emoji, position_pct=None):
        """从 final_decision 解析信号/理由，构建完整飞书行"""
        stock  = s.get('stock', '')
        name   = s.get('name', '')
        conf   = _confidence_value(s)
        buy_score = _buy_score_value(s)
        dec    = s.get('final_decision', '') or ''
        sig    = _parse_signal(s)

        # ── 核心理由（方向③）：优先从结构化 reason= 字段读，再用正则解析文本──
        reason = s.get('reason', '')
        if not reason:
            m = re_module.search(r'reason=([^\n]+)', dec)
            if m:
                reason = m.group(1).strip()
        if not reason:
            m = re_module.search(r'\*\*核心理由\*\*[:：]?\s*(.+)', dec, re.DOTALL)
            if m:
                reason = re_module.sub(r'^[\*\s]+', '', m.group(1).strip())
        if not reason:
            in_reason = False
            for line in dec.split('\n'):
                if '核心理由' in line:
                    in_reason = True
                    continue
                if in_reason and line.strip() and not line.strip().startswith('**') \
                   and not line.strip().startswith('仓位') and '信号' not in line[:6]:
                    reason = re_module.sub(r'^[\*\s]+', '', line.strip())
                    break
        if not reason and dec:
            reason = re_module.sub(r'^\[(Structured|Repaired|TextOnly)\]\s*', '', dec.strip())
        reason_str = re_module.sub(r'\s+', ' ', str(reason)).strip().strip('"“”').strip(",，}")

        # ── 技术标签（方向②：昨涨幅|PE|RSI）──
        tech_parts = []
        # 尝试多个后缀形式（price_cache 用 .SH/.SZ/.BJ）
        pdata = price_cache.get(stock, {})
        if not pdata:
            for suffix in ['.SH', '.SZ', '.BJ']:
                if stock + suffix in price_cache:
                    pdata = price_cache[stock + suffix]
                    break
        ychg = pdata.get('pct')
        if ychg is not None:
            try:
                ychg_f = float(ychg)
                arrow = '📈' if ychg_f > 0 else '📉' if ychg_f < 0 else '➡️'
                tech_parts.append(f"昨{ychg_f:+.1f}%{arrow}")
            except (ValueError, TypeError):
                pass
        # PE（来自 phase2_result 的 pe 字段，如果辩论结果有的话）
        pe_val = s.get('pe') or phase2_result.get('stock_pe', {}).get(stock)
        if pe_val:
            try:
                pe_f = float(pe_val)
                if pe_f > 0:
                    tech_parts.append(f"PE={pe_f:.1f}")
            except (ValueError, TypeError):
                pass
        # RSI（如果有的话）
        rsi_val = s.get('rsi') or phase2_result.get('stock_rsi', {}).get(stock)
        if rsi_val:
            try:
                rsi_f = float(rsi_val)
                rsi_tag = '📈' if rsi_f > 60 else '📉' if rsi_f < 40 else '➡️'
                tech_parts.append(f"RSI={rsi_f:.0f}{rsi_tag}")
            except (ValueError, TypeError):
                pass
        tech_str = ' | '.join(tech_parts) if tech_parts else ''

        # ── 价格信息（#14：标“昨日收盘”）──
        prev_close = pdata.get('prev_close') or pdata.get('close')
        try:
            price_str = f"参考¥{float(prev_close):.2f}·昨收" if prev_close else ''
        except (TypeError, ValueError):
            price_str = ''
        pt_m = re_module.search(r'目标[价位]*\s*[¥￥]?\s*(\d+\.?\d*)', dec)
        pt_str = f" | 目标¥{pt_m.group(1)}" if pt_m else ''
        sl_m = re_module.search(r'止损[价位]*\s*[¥￥]?\s*(\d+\.?\d*)', dec)
        sl_str = f" | 止损¥{sl_m.group(1)}" if sl_m else ''

        # ── 回测收益（方向④）──
        ret_str = _get_backtest_text(stock)

        data_contract = s.get('data_contract') or {}
        dq_flags = s.get('data_quality_flags') or []
        if data_contract:
            contract_parts = []
            for label, key in [('K线', 'kline'), ('资金', 'money_flow'), ('财务', 'financial'), ('板块', 'sector')]:
                item = data_contract.get(key) or {}
                status = item.get('status') or 'unknown'
                source = item.get('source') or 'none'
                mark = 'OK' if status == 'ok' else '部分' if status == 'partial' else '缺失'
                contract_parts.append(f"{label}{mark}:{source}")
            data_quality_str = '数据: ' + ' '.join(contract_parts)
        elif dq_flags:
            data_quality_str = '数据缺口: ' + '、'.join(str(x) for x in dq_flags[:4])
        else:
            data_quality_str = '数据: 完整'

        money_source = (s.get('money_flow') or {}).get('source')
        money_source_str = f"资金流源:{money_source}" if money_source else ""
        pm_model = (s.get('decision_models') or {}).get('pm') or s.get('decision_source')
        pm_model_str = f"PM:{str(pm_model).split(':')[-1]}" if pm_model else ""
        evidence_validation = s.get('evidence_validation') or {}
        evidence_status = str(evidence_validation.get('status') or '').lower()
        if evidence_status == 'pass':
            evidence_str = '幻觉校验:通过'
        elif evidence_status == 'warn':
            warn_count = len(evidence_validation.get('warnings') or [])
            evidence_str = f'幻觉校验:警告{warn_count}'
        elif evidence_status == 'fail':
            err_count = len(evidence_validation.get('errors') or [])
            evidence_str = f'幻觉校验:失败降级{err_count}'
        else:
            evidence_str = ''

        # ── 风险标记──
        risk_str = '⚠️回避' if sig == 'AVOID' and not s.get('risk_flags') else ''
        if not risk_str:
            risk_str = ' '.join([f"⚠️{r}" for r in s.get('risk_flags', [])])

        # ── 板块标签（方向⑤）──
        sector_label = s.get('sector', '')
        if sector_label:
            sector_str = f"[{sector_label}]"
        else:
            sector_str = ''

        source_pools = s.get('source_pools') or ([s.get('pool')] if s.get('pool') else [])
        source_pools = [str(x) for x in source_pools if x]
        source_str = f"源:{'+'.join(source_pools[:2])}" if source_pools else ''
        pool_score = _pool_score_value(s)
        pool_rank = _pool_rank_value(s)
        pool_total = s.get('pool_scored_candidates') or s.get('pool_total_candidates')
        if pool_score > 0:
            pool_str = f"池内{pool_score:.1f}分"
            if pool_rank < 999999:
                pool_str += f"#{pool_rank}"
                try:
                    if pool_total:
                        pool_str += f"/{int(pool_total)}"
                except (TypeError, ValueError):
                    pass
        else:
            pool_str = ''

        edge_score = 0.0
        try:
            edge_score = float(s.get('historical_edge_score') or 0)
        except (TypeError, ValueError):
            edge_score = 0.0
        edge_matches = s.get('historical_edge_matches') or []
        chase_penalty = 0.0
        try:
            chase_penalty = float(s.get('chase_risk_penalty') or 0)
        except (TypeError, ValueError):
            chase_penalty = 0.0
        edge_str = f"历史优势+{edge_score:.1f}" if edge_score > 0 else ''

        # ── 置信度emoji（方向④）──
        conf_num = int(conf) if isinstance(conf, (int, float)) else 0
        conf_label = str(int(conf)) if isinstance(conf, (int, float)) and float(conf).is_integer() else f"{conf:.1f}"
        buy_score_label = str(int(buy_score)) if float(buy_score).is_integer() else f"{buy_score:.1f}"
        conf_emoji_str = '🔵' if conf_num >= 70 else '🟡' if conf_num >= 50 else '🟠'

        # ── #5 两段式：概览行（必看） + 详情行 ──
        # 概览只放短字段，完整理由放详情，避免 Top5 每段被截断。
        tv_url = _tradingview_chart_url(stock)
        tv_link = f" [TV图表]({tv_url})" if tv_url else ""
        overview_parts = [
            f"{label_emoji}{sector_str}**{stock}** {name}{tv_link}",
            f"信号{sig}",
            f"做多{buy_score_label}",
            f"{conf_emoji_str}置信{conf_label}分",
        ]
        if position_pct is not None:
            overview_parts.append(f"仓位{position_pct}%")
        if pool_str:
            overview_parts.append(pool_str)
        if edge_str:
            overview_parts.append(edge_str)
        # 只在概览行超出 5 项时是"过长"，才不展示冗余字段
        if source_str:
            overview_parts.append(source_str)
        overview = ' | '.join(overview_parts)

        # 详情行（折叠）
        detail_parts = []
        if tech_str:
            detail_parts.append(f"技术: {tech_str}")
        if price_str:
            detail_parts.append(price_str)
        if pt_str:
            detail_parts.append(pt_str.strip(' |'))
        if sl_str:
            detail_parts.append(sl_str.strip(' |'))
        if ret_str:
            detail_parts.append(ret_str)
        if data_quality_str:
            detail_parts.append(data_quality_str)
        if money_source_str:
            detail_parts.append(money_source_str)
        if pm_model_str:
            detail_parts.append(pm_model_str)
        if evidence_str:
            detail_parts.append(evidence_str)
        if edge_matches:
            rule_texts = []
            for item in edge_matches[:2]:
                desc = item.get('description') if isinstance(item, dict) else ''
                bonus = item.get('weighted_bonus') if isinstance(item, dict) else None
                if desc:
                    try:
                        rule_texts.append(f"{desc}(+{float(bonus):.1f})" if bonus is not None else str(desc))
                    except (TypeError, ValueError):
                        rule_texts.append(str(desc))
            if rule_texts:
                detail_parts.append('历史优势: ' + '；'.join(rule_texts))
        quant_base = s.get('quant_base_score')
        risk_adj = s.get('llm_risk_adjustment')
        pool_dyn = s.get('pool_dynamic_adjustment')
        if quant_base not in (None, ''):
            try:
                quant_text = f"量化基分{float(quant_base):.1f}"
                if pool_dyn not in (None, ''):
                    quant_text += f" + 池子复盘{float(pool_dyn):+.1f}"
                if risk_adj not in (None, ''):
                    adj = float(risk_adj)
                    quant_text += f" + LLM修正{adj:+.1f}"
                if edge_score > 0:
                    quant_text += f" + 历史优势{edge_score:+.1f}"
                if chase_penalty > 0:
                    quant_text += f" - 追高风险{chase_penalty:.1f}"
                detail_parts.append(quant_text)
            except (TypeError, ValueError):
                pass
        if risk_str:
            detail_parts.append(risk_str)
        if reason_str:
            detail_parts.append(f"💡完整理由: {reason_str}")
        detail_text = ' | '.join(detail_parts) if detail_parts else '（无详情）'

        # 返回 (概览, 详情) 元组，供调用者拼成“概览行 + 折叠详情”结构
        return {'overview': overview, 'detail': detail_text, 'sig': sig}

    # ── 构建精简飞书卡片（Top5 只股票）──────────────
    elements = []

    # ── ① 操作信号总览 ──
    n_top5 = len(combined_top5)
    if buy_stocks:
        action_text = f"BUY {len(buy_stocks)}只，展示Top{n_top5}"
        action_emoji = '🟢'
    elif watch_stocks:
        action_text = f"今日无BUY，展示观察Top{n_top5}"
        action_emoji = '🟡'
    else:
        action_text = "今日空仓"
        action_emoji = '🔴'
    rule_line = "盘中买入以Top5为池；Top5按做多分排序并叠加历史优势组合，WATCH需盘中技术确认"
    elements.append({
        "tag": "div",
        "text": {"tag": "lark_md",
               "content": f"**{action_emoji} {action_text}**\n{rule_line}{time_label}"}
    })

    # ── ① 大盘实际涨跌幅 + 板块强弱 ──
    index_parts = []
    if sh_chg is not None:
        index_parts.append(f"上证 {sh_chg:+.2f}%")
    if cy_chg is not None:
        index_parts.append(f"创业板 {cy_chg:+.2f}%")
    if hs300_ret is not None:
        index_parts.append(f"沪深300 {hs300_ret:+.2f}%")
    index_line = ' | '.join(index_parts) if index_parts else '指数涨跌: 暂无数据'
    turnover_line = f" | 两市总成交 **{total_turnover_str}**" if total_turnover_str else ''
    elements.append({"tag": "hr"})
    elements.append({
        "tag": "div",
        "text": {"tag": "lark_md",
               "content": (
                   f"📊 **大盘环境**\n"
                   f"{index_line}{turnover_line}"
               )}
    })

    # ── #8 板块热力（早报 v2：webhook 机器人不支持 table，改成 div 文本）───
    hot_list = sector_rot.get("强势板块", [])[:5]
    cold_list = sector_rot.get("弱势板块", [])[:5]
    if hot_list or cold_list:
        elements.append({"tag": "hr"})
        hot_str = '、'.join([f"🔥{n}" for n in hot_list]) if hot_list else ''
        cold_str = '、'.join([f"❄️{n}" for n in cold_list]) if cold_list else ''
        content_parts = ['**🔥❄️ 板块热力**']
        if hot_str:
            content_parts.append(f"强势板块: {'、'.join(hot_list)}")
        if cold_str:
            content_parts.append(f"弱势板块: {'、'.join(cold_list)}")
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": '\n'.join(content_parts)}
        })

    # ── ② 市场情绪（保留原结构，但加入大盘数字）───
    detail_parts = [sentiment_label]
    if limit_up:
        detail_parts.append(f"涨停{limit_up}家")
    if hot_sector:
        detail_parts.append(f"炸板率{hot_sector}%")
    if bulls_bears:
        detail_parts.append(bulls_bears)
    sentiment_detail = ' | '.join(detail_parts)
    elements.append({"tag": "hr"})
    elements.append({
        "tag": "div",
        "text": {"tag": "lark_md",
               "content": f"🌡️ 市场情绪: **{sentiment_detail}**"}
    })

    # ── ③ 4 个市场分析（早报 v2：技术/情绪/涨停基因/新闻舆情，压成 4 段）───
    # 收集 4 分析师首行作为一句话
    market4_lines = []
    for r in phase1_results:
        nm = r.get('name', '')
        f = (r.get('findings') or '').strip()
        if not f:
            continue
        if nm == '技术分析师':
            # 早报 v2 修复：合并 3 大指数（上证/深证/创业板）研判，不只 000001
            tech_lines = []
            for line in f.split('\n'):
                line = line.strip().lstrip('*').strip()
                if '趋势=' in line or '现价=' in line:
                    m_code = re_module.match(r'(\d{6}):\s*现价=([\d.]+).*?趋势=([\u4e00-\u9fa5/]+)', line)
                    if m_code:
                        code = m_code.group(1)
                        trend = m_code.group(3)
                        label_map = {'000001': '上证', '399001': '深证', '399006': '创业板'}
                        tech_lines.append(f"{label_map.get(code, code)}{trend}")
            if tech_lines:
                market4_lines.append(f"📈 **技术面**: {' / '.join(tech_lines)}")
            else:
                first = f.split('\n')[0].strip().lstrip('*').strip()
                if len(first) > 80:
                    first = first[:78] + '..'
                market4_lines.append(f"📈 **技术面**: {first}")
        elif nm == '市场情绪分析师':
            market4_lines.append(f"🌡️ **市场情绪**: {sentiment_detail}")
        elif nm == '涨停基因分析师':
            first = f.split('\n')[0].strip().lstrip('*').strip()
            if len(first) > 80:
                first = first[:78] + '..'
            market4_lines.append(f"🔥 **涨停基因**: {first}")
        elif nm == '新闻分析师':
            market4_lines.append(f"🌐 **新闻舆情**: {news_analyst_news}")
    if market4_lines:
        elements.append({"tag": "hr"})
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md",
                   "content": '\n'.join(market4_lines)}
        })

    # ── 旧 7 分析师折叠面板（早报 v2 删：和上面 4 个市场分析重复）───
    _ = phase2_result.get('all_analysts') or []  # 保留以备回退


    # ── ④ 选股回测 + Alpha基准（方向④）───
    sel = backtest_selection or {}
    sel_stocks = sel.get('stocks', [])
    sel_valid = [x for x in sel_stocks if 'return_pct' in x and 'error' not in x]
    sel_no_trade = [x for x in sel_stocks if x.get('status') == 'no_trade']
    sel_days = sel.get('lookback_days', 5)
    if sel_stocks:
        elements.append({"tag": "hr"})
        sel_avg = sel.get('avg_return_pct', 0)
        sel_wr  = sel.get('win_rate', 0)
        sel_valid_n = len(sel_valid)
        hs300_sel = sel.get('hs300_return_pct')
        alpha_sel = sel.get('alpha_pct')
        alpha_sel_str = f" | Alpha{alpha_sel:+.2f}%" if alpha_sel is not None else ''
        hs300_sel_str = f" | 沪深300{hs300_sel:+.2f}%" if hs300_sel is not None else ''
        if sel_valid_n > 0:
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md",
                    "content": (
                        f"📊 **候选质量**（信号日→{sel_days}日持仓）\n"
                        f"均收益{sel_avg:+.2f}%{alpha_sel_str}{hs300_sel_str} | "
                        f"胜率{sel_wr:.0f}%({sel_valid_n}/{len(sel_stocks)})"
                        + (f" | 数据不足{len(sel_no_trade)}只" if sel_no_trade else "")
                    )}
            })
        elif sel_no_trade:
            elements.append({"tag": "div", "text": {"tag": "lark_md",
                "content": f"📊 **候选质量**: ⚠️ 数据不足（{len(sel_no_trade)}只无法回测）"}})
        else:
            err = sel_stocks[0].get('error', '数据不可用') if sel_stocks else '数据不可用'
            elements.append({"tag": "div", "text": {"tag": "lark_md",
                "content": f"📊 **候选质量**: ⚠️ {err}"}})

    # ── ④ 策略模拟 + Alpha基准（方向④）───
    strat = backtest_strategy or {}
    strat_trades = strat.get('trades', [])
    strat_valid = [x for x in strat_trades if x.get('return_pct') is not None and 'error' not in x]
    strat_holding = [x for x in strat_trades if x.get('status') == 'holding']
    strat_no_trade = [x for x in strat_trades if x.get('status') == 'no_trade']
    strat_data_missing = [x for x in strat_no_trade if x.get('reason_type') == 'data_missing']
    strat_not_filled = [x for x in strat_no_trade if x.get('reason_type') == 'not_filled']
    if strat_trades:
        elements.append({"tag": "hr"})
        strat_avg = strat.get('avg_return_pct', 0)
        strat_window = ""
        if strat.get("fromdate") and strat.get("todate"):
            strat_window = f"{strat.get('fromdate')}→{strat.get('todate')}，"
        elif strat.get("lookback_days"):
            strat_window = f"近{strat.get('lookback_days')}个交易数据点，"
        hs300_st_ret = strat.get('hs300_return_pct')
        alpha_st     = strat.get('alpha_pct')
        strat_wr  = len([x for x in strat_valid if x.get('return_pct', 0) > 0]) / len(strat_valid) * 100 if strat_valid else 0
        alpha_st_str = f" | Alpha{alpha_st:+.2f}%" if alpha_st is not None else ""
        hs300_st_str = f" | 沪深300{hs300_st_ret:+.2f}%" if hs300_st_ret is not None else ""
        strat_valid_n = len(strat_valid)
        total_ret = strat.get('total_return_pct')
        if total_ret is None:
            total_ret = strat.get('portfolio_return_pct')
        total_ret_str = f"组合当前收益{float(total_ret):+.2f}%" if total_ret is not None else ""
        holding_str = f" | 持有中{len(strat_holding)}/{len(strat_trades)}" if strat_holding else ""
        no_trade_str = (
            (f" | 数据不足{len(strat_data_missing)}只" if strat_data_missing else "")
            + (f" | 未成交{len(strat_not_filled)}只" if strat_not_filled else "")
        )
        if strat_valid_n > 0:
            closed_line = (
                f"已平仓样本{strat_valid_n}/{len(strat_trades)}："
                f"均收益{strat_avg:+.2f}% | 胜率{strat_wr:.0f}%"
            )
            if total_ret_str:
                strategy_content = (
                    f"📈 **策略模拟**（{strat_window}持仓{strat.get('hold_days',20)}日）\n"
                    f"{total_ret_str}{alpha_st_str}{hs300_st_str}{holding_str}{no_trade_str}\n"
                    f"{closed_line}"
                )
            else:
                strategy_content = (
                    f"📈 **策略模拟**（{strat_window}持仓{strat.get('hold_days',20)}日）\n"
                    f"{closed_line}{holding_str}{no_trade_str}{alpha_st_str}{hs300_st_str}"
                )
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md",
                    "content": strategy_content}
            })
        elif strat_holding:
            total_ret_str = total_ret_str or "组合收益待收盘确认"
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md",
                    "content": (
                        f"📈 **策略模拟**（{strat_window}持仓{strat.get('hold_days',20)}日）\n"
                        f"{total_ret_str}{alpha_st_str}{hs300_st_str} | "
                        f"持有中{len(strat_holding)}/{len(strat_trades)}"
                        + no_trade_str
                    )}
            })
        elif strat_no_trade:
            if strat_data_missing and not strat_not_filled:
                no_trade_msg = f"数据不足（{len(strat_data_missing)}只无法回测）"
            elif strat_not_filled and not strat_data_missing:
                no_trade_msg = f"Top5模拟未形成成交（{len(strat_not_filled)}只）"
            else:
                no_trade_msg = f"数据不足{len(strat_data_missing)}只，未成交{len(strat_not_filled)}只"
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md",
                    "content": f"📈 策略模拟: ⚠️ {no_trade_msg}"}
            })
        else:
            err = (strat_trades[0].get('error', '数据不可用') or '数据不可用') if strat_trades else '数据不可用'
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md",
                    "content": f"📈 策略模拟: ⚠️ {err}"}
            })



    # ── ⑥ Top5 股票（#5 两段式：概览行 + 详情，全部以 div 呈现避免飞书 webhook 拒绝 details）───
    if combined_top5:
        elements.append({"tag": "hr"})
        overview_lines = []
        detail_lines = []
        for s in combined_top5:
            sig = _parse_signal(s)
            emoji = '🟢' if sig == 'BUY' else '🟡'
            row = _fmt_stock_row(s, emoji)
            overview_lines.append(row['overview'])
            detail_lines.append(f"- **{s.get('stock', '')} {s.get('name', '')}**: {row['detail']}")
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md",
                   "content": '🎯 **Top 5**\n' + '\n'.join(overview_lines)}
        })
        if detail_lines:
            # 早报 v2：webhook 机器人不支持 details，改用普通 div 展示
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md",
                       "content": '🔍 **Top5 详情**\n' + '\n'.join(detail_lines)}
            })

    # ── ⑦ 数据质量 + 执行信息（方向⑥：时间戳）───
    qual_parts = []
    if pe_info:
        qual_parts.append(pe_info)
    total_count = len(ranked)  # 早报 v2 修复：提到 if data_quality 之外
    # 早报 v2 修复：dq 相关变量在 if 块外默认初始化，避免 data_quality 为空时挂
    qpe = qrsi = ''
    dq_summary = {}
    flag_counts = core_counts = aux_counts = {}
    affected_count = 0
    flag_names = {}
    if data_quality:
        qpe = data_quality.get('pe', '')
        qrsi = data_quality.get('rsi', '')
        dq_summary = data_quality.get('summary') or phase2_result.get('data_quality_summary') or {}
        flag_counts = dq_summary.get('flag_counts') or {}
        core_counts = dq_summary.get('core_flag_counts') or {}
        aux_counts = dq_summary.get('aux_flag_counts') or {}
        affected_count = dq_summary.get('affected_count', 0)
        total_count = len(ranked)
        if flag_counts:
            flag_names = {
                'KLINE_MISSING': 'K线缺失',
                'KLINE_SHORT': 'K线偏短',
                'FINANCIAL_MISSING': '财务缺失',
                'SECTOR_MISSING': '板块缺失',
                'MONEY_FLOW_MISSING': '资金流缺失',
                'MONEY_FLOW_FETCH_FAILED': '资金流抓取失败',
                'MONEY_FLOW_PARTIAL': '资金流部分缺失',
                'TECH_ANALYSIS_MISSING': '技术形态分析缺失',
                'MODEL_FAILED': '模型裁决失败',
                'MODEL_EVIDENCE_FAILED': '模型证据校验失败',
            }
            if not core_counts and not aux_counts:
                core_keys = {
                    'KLINE_MISSING', 'KLINE_SHORT', 'FINANCIAL_MISSING',
                    'MONEY_FLOW_MISSING', 'MONEY_FLOW_FETCH_FAILED', 'TECH_ANALYSIS_MISSING', 'MODEL_FAILED',
                }
                aux_keys = {'MONEY_FLOW_PARTIAL', 'SECTOR_MISSING'}
                core_counts = {k: v for k, v in flag_counts.items() if k in core_keys}
                aux_counts = {k: v for k, v in flag_counts.items() if k in aux_keys}
        source_counts = dq_summary.get('money_flow_source_counts') or {}
        if source_counts:
            src_line = '/'.join(f"{k}:{v}" for k, v in sorted(source_counts.items()))
            qual_parts.append(f"资金流来源 {src_line}")
        elif total_count:
            qual_parts.append(f"数据质量: 未发现关键缺口({total_count}只)")
        if qpe:
            qual_parts.append(f"PE覆盖{qpe}")
        if qrsi:
            qual_parts.append(f"RSI有效{qrsi}")
    exec_parts = []
    if exec_stats:
        exec_parts.append(f"共{exec_stats.get('total', 0)}只辩论")
        if exec_stats.get('model'):
            exec_parts.append(f"🤖 {exec_stats.get('model', '')}")
        source_counts = exec_stats.get('decision_sources') or {}
        if source_counts:
            exec_parts.append('裁决来源 ' + '/'.join(f"{k}:{v}" for k, v in sorted(source_counts.items())))
    checkpoint_summary = phase2_result.get('checkpoint_summary') or {}
    if checkpoint_summary:
        exec_parts.append(
            f"断点 已完成{checkpoint_summary.get('completed', 0)} "
            f"失败{checkpoint_summary.get('failed', 0)} "
            f"节点{checkpoint_summary.get('node_status', 0)}"
        )
    contract_counts = dq_summary.get('data_contract_counts') or {}
    if contract_counts:
        contract_line = []
        for key, label in [('kline', 'K线'), ('money_flow', '资金'), ('financial', '财务'), ('sector', '板块')]:
            counts = contract_counts.get(key) or {}
            ok = counts.get('ok', 0)
            partial = counts.get('partial', 0)
            missing = counts.get('missing', 0)
            if ok or partial or missing:
                contract_line.append(f"{label}OK{ok}/部分{partial}/缺{missing}")
        if contract_line:
            qual_parts.append('数据合同 ' + ' '.join(contract_line))
    exec_parts.append(route_tag)
    qual_line = ' | '.join(qual_parts) if qual_parts else ''
    exec_line = ' | '.join(exec_parts)

    # ── #11 数据质量综合分（0-100，分越高越完整）───
    dq_score = None
    dq_detail = []
    if total_count and (core_counts or aux_counts):
        # 权重调为更温和：关键缺口 -1/只, 辅助 -0.3/只, 模型失败 -2/只
        # 这样 73 只，50 只资金流缺失只扣 50 分，最后 50/100，能反映问题但不归零
        core_penalty = sum(core_counts.values()) * 1.0
        aux_penalty = sum(aux_counts.values()) * 0.3
        model_penalty = core_counts.get('MODEL_FAILED', 0) * 2.0
        total_penalty = core_penalty + aux_penalty + model_penalty
        dq_score = max(0, int(100 - total_penalty))
        if dq_score >= 90:
            dq_grade, dq_color = '优秀', '🟢'
        elif dq_score >= 75:
            dq_grade, dq_color = '良好', '🟡'
        elif dq_score >= 60:
            dq_grade, dq_color = '一般', '🟠'
        else:
            dq_grade, dq_color = '较差', '🔴'
        dq_detail.append(f"{dq_color} 数据完整度 **{dq_score}/100** ({dq_grade})")
        if core_counts:
            core_line = '、'.join(f"{flag_names.get(k, k)}{v}" for k, v in sorted(core_counts.items()))
            dq_detail.append(f"关键缺口{affected_count}/{total_count}: {core_line}")
        if aux_counts:
            aux_line = '、'.join(f"{flag_names.get(k, k)}{v}" for k, v in sorted(aux_counts.items()))
            dq_detail.append(f"辅助缺口: {aux_line}")

    # ── #17 早报质量自评（耗时 + 模型 + 数据完整度）───
    report_elapsed = (datetime.datetime.now() - _run_start_time).total_seconds() if _run_start_time else None
    quality_parts = dq_detail if dq_detail else (['数据质量: 未发现关键缺口'] if total_count else [])
    if report_elapsed is not None:
        elapsed_str = f"{int(report_elapsed//60)}分{int(report_elapsed%60)}秒"
        quality_parts.append(f"⏱ 耗时 {elapsed_str}")
    quality_line = ' | '.join(quality_parts) if quality_parts else ''
    exec_line_text = f"⚙️ {exec_line}" if exec_line else ''

    if quality_line:
        elements.append({"tag": "hr"})
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"📊 {quality_line}\n{exec_line_text}"}
        })

    card = {
        "config": {"enable_forward": True},
        "header": {
            "title": {"tag": "plain_text",
                      "content": f"📋 选股早报 {today}{time_label}"},
            "template": sentiment_color
        },
        "elements": elements
    }
    try:
        pushed = feishu_push_card(card, webhook)
        if pushed:
            logger.info("飞书早报卡片推送成功")
            return True
        logger.warning("飞书早报卡片推送失败，未写入成功标记")
        return False
    except Exception as e:
        logger.warning(f"飞书推送失败: {e}")
        return False

# ── 入口 ───────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="每日选股工作流")
    parser.add_argument("--model", default=None, help="LLM 模型（默认火山引擎 coding plan，GPT-5.5 / MiniMax M3 兜底）")
    parser.add_argument("--resume", action="store_true", help="从上次断点继续辩论（不重新跑 Step 1-3）")
    args = parser.parse_args()
    model = args.model or "volcengine-plan/ark-code-latest"
    run_daily_workflow(dry_run=False, model=model, resume=args.resume)
