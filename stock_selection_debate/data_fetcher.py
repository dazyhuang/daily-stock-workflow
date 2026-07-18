"""
数据获取模块
===============
通过 xqshare 获取候选股的日K线数据
复用 Phase 1 的财务/技术缓存
"""

import sys
import json
import logging
import subprocess
import os
import re
import time
import pandas as pd
import threading
try:
    import fcntl
except Exception:  # pragma: no cover - non-posix fallback
    fcntl = None
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
import urllib.parse
import urllib.request
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, List, Any, Optional

# Use the workflow logger hierarchy so prefetch progress reaches the stable
# runner's stdout/file handlers and counts as watchdog activity.
logger = logging.getLogger("daily_stock_workflow.data_fetcher")
CACHE_DIR = Path(__file__).parent.parent / "output" / "data_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(Path(__file__).parent.parent))
from domestic_network import domestic_subprocess_env, request_direct, retry_call  # noqa: E402
from stock_selection_debate.technical_indicators import compute_macd, legacy_macd_signal  # noqa: E402

# 辩论信号套件（蜡烛图、量价背离、海龟、波浪、江恩）
DEBATE_SIGNAL_KITS_DIR = Path(__file__).parent.parent / "debate_signal_kits"
if DEBATE_SIGNAL_KITS_DIR.exists():
    sys.path.insert(0, str(DEBATE_SIGNAL_KITS_DIR.parent))
    try:
        from debate_signal_kits import (
            candlestick_signals,
            volume_price_divergence,
            turtle_signals,
            elliott_signals,
            gann_signals,
        )
    except ImportError:
        candlestick_signals = None
        volume_price_divergence = None
        turtle_signals = None
        elliott_signals = None
        gann_signals = None
else:
    candlestick_signals = None
    volume_price_divergence = None
    turtle_signals = None
    elliott_signals = None
    gann_signals = None

# xqshare 客户端路径
SKILLS_DIR = Path(__file__).parent.parent.parent / "skills"
QMT_HTTP_URL = os.environ.get("QMT_HTTP_URL", "http://127.0.0.1:8080").rstrip("/")
XQ_KLINE_MANIFEST_PATH = Path(__file__).resolve().parents[2] / "xq_kline_manifest.json"
KLINE_EXTERNAL_FETCH_MIN_BARS = 60
_XQ_SECTOR_MAP: Optional[Dict[str, str]] = None
_EASTMONEY_FAIL_STREAK = 0
_EASTMONEY_DISABLED_UNTIL = 0.0
_EASTMONEY_LOCK = threading.Lock()
_MX_MONEY_FLOW_FAIL_STREAK = 0
_MX_MONEY_FLOW_DISABLED_UNTIL = 0.0
_MX_MONEY_FLOW_LOCK = threading.Lock()
_MX_MONEY_FLOW_LAST_QUERY_AT = 0.0
_MX_MONEY_FLOW_GLOBAL_LOCK_PATH = CACHE_DIR / "mx_money_flow_global.lock"
_MX_MONEY_FLOW_STATE_PATH = CACHE_DIR / "mx_money_flow_rate_state.json"


class _MXMoneyFlowRateLimited(RuntimeError):
    pass


class _MXMoneyFlowTooFrequent(RuntimeError):
    pass


class _MXMoneyFlowAuthError(RuntimeError):
    pass


def _data_result(
    *,
    source: str,
    status: str,
    error: str = "",
    quality_flags: Optional[List[str]] = None,
    as_of: Optional[str] = None,
    age_minutes: Optional[int] = None,
    is_stale: Optional[bool] = None,
) -> Dict[str, Any]:
    """Small data-access contract used by reports without changing legacy fields."""
    resolved_as_of = as_of or ""
    as_of_key = re.sub(r"\D", "", str(resolved_as_of or ""))[:8]
    stale = bool(is_stale) if is_stale is not None else False
    flags = _uniq_keep_order(quality_flags or [])
    resolved_status = status
    if resolved_status == "ok" and not as_of_key:
        resolved_status = "partial"
        flags = _uniq_keep_order(flags + ["DATE_UNKNOWN"])
    return {
        "source": source or "none",
        "status": resolved_status,
        "error": str(error or "")[:300],
        "as_of": resolved_as_of,
        "age_minutes": age_minutes,
        "is_stale": stale,
        "quality_flags": flags,
    }


def _contract_status(ok: bool, partial: bool = False) -> str:
    if ok and not partial:
        return "ok"
    if ok and partial:
        return "partial"
    return "missing"


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: Optional[int] = None) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except Exception:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    return value


def _env_float(name: str, default: float, *, minimum: Optional[float] = None) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except Exception:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    return value


def _load_json_cache(path: Path, default):
    try:
        if path.exists():
            with open(path, encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"读取缓存失败 {path}: {e}")
    return default


def _save_json_cache(path: Path, data) -> None:
    try:
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(path)
    except Exception as e:
        logger.warning(f"保存缓存失败 {path}: {e}")


def _latest_cache(prefix: str) -> Dict:
    files = sorted(CACHE_DIR.glob(f"{prefix}*.json"), reverse=True)
    for path in files:
        data = _load_json_cache(path, {})
        if data:
            return data
    return {}


def _retry_call(label: str, fn, retries: int = 3, delay: float = 1.5):
    return retry_call(
        label,
        fn,
        retries=max(1, retries),
        base_delay=delay,
        throttle_key=label.split()[0],
        min_interval=1.0,
    )


def _sector_cache_path() -> Path:
    return CACHE_DIR / "sector_cache.json"


def _xq_sector_map_path() -> Path:
    return CACHE_DIR / "xq_sector_map.json"


def _get_cached_sector(stock_code: str) -> str:
    cache = _load_json_cache(_sector_cache_path(), {})
    item = cache.get(str(stock_code).zfill(6), {})
    return str(item.get("sector", "") or "")


def _get_cached_sector_as_of(stock_code: str) -> str:
    cache = _load_json_cache(_sector_cache_path(), {})
    item = cache.get(str(stock_code).zfill(6), {})
    return str(item.get("updated", "") or "")


def _get_fresh_cached_sector(stock_code: str, max_age_days: int = 30) -> tuple[str, str]:
    sector = _get_cached_sector(stock_code)
    as_of = _get_cached_sector_as_of(stock_code)
    if not sector or not as_of:
        return "", as_of
    try:
        observed = datetime.strptime(re.sub(r"\D", "", as_of)[:8], "%Y%m%d").date()
        if (date.today() - observed).days > max_age_days:
            return "", as_of
    except Exception:
        return "", as_of
    return sector, as_of


def _set_cached_sector(
    stock_code: str,
    sector: str,
    *,
    source: str = "verified",
    as_of: str = "",
) -> None:
    if not sector:
        return
    cache = _load_json_cache(_sector_cache_path(), {})
    cache[str(stock_code).zfill(6)] = {
        "sector": sector,
        "updated": _normalize_kline_date(as_of, dashed=False) or date.today().strftime("%Y%m%d"),
        "source": source,
    }
    _save_json_cache(_sector_cache_path(), cache)


def _qmt_get_json(path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 8) -> Optional[Dict[str, Any]]:
    try:
        url = f"{QMT_HTTP_URL}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.debug(f"QMT HTTP 请求失败 {path}: {e}")
        return None


def _normalize_xq_stock_code(code: str) -> str:
    raw = str(code or "").strip()
    if "." in raw:
        raw = raw.split(".", 1)[0]
    return raw.zfill(6) if raw.isdigit() else raw


def _clean_xq_sector_name(sector: str) -> str:
    return re.sub(r"^GICS\d+", "", str(sector or "")).strip() or str(sector or "").strip()


def _is_today_file(path: Path) -> bool:
    """检查文件是否今天创建（是则使用，否则删除重建）"""
    try:
        stat = path.stat()
        import datetime as _dt
        mtime = _dt.datetime.fromtimestamp(stat.st_mtime)
        today = _dt.datetime.now()
        return mtime.date() == today.date()
    except:
        return False


def _build_xq_sector_map() -> Dict[str, str]:
    """Build stock -> sector map from local XQShare/QMT sector constituents."""
    map_path = _xq_sector_map_path()
    if map_path.exists() and _is_today_file(map_path):
        cached = _load_json_cache(map_path, {})
        if cached:
            return cached
        # 文件存在但读取失败，删掉重建
        try:
            map_path.unlink(missing_ok=True)
        except Exception:
            pass

    raw = _qmt_get_json("/sector_list", timeout=10)
    if not raw or not raw.get("success"):
        return {}

    sectors = [str(s) for s in raw.get("data", []) if str(s).startswith("GICS")]
    # Prefer more specific categories first.
    sectors.sort(key=lambda s: (0 if s.startswith("GICS3") else 1 if s.startswith("GICS2") else 2, len(s)))

    mapping: Dict[str, str] = {}
    for sector in sectors:
        result = _qmt_get_json("/stock_list_in_sector", {"sector": sector}, timeout=6)
        if not result or not result.get("success"):
            continue
        clean_sector = _clean_xq_sector_name(sector)
        for item in result.get("data", []) or []:
            text = str(item)
            if not text.endswith((".SH", ".SZ", ".BJ")):
                continue
            code = _normalize_xq_stock_code(text)
            mapping.setdefault(code, clean_sector)

    if mapping:
        _save_json_cache(_xq_sector_map_path(), mapping)
        logger.info(f"XQShare 板块映射缓存完成: {len(mapping)} 只")
    return mapping


def _fetch_sector_via_xqshare(stock_code: str) -> str:
    global _XQ_SECTOR_MAP
    if _XQ_SECTOR_MAP is None:
        _XQ_SECTOR_MAP = _build_xq_sector_map()
    code = str(stock_code).zfill(6)
    return str((_XQ_SECTOR_MAP or {}).get(code, "") or "")


def _fetch_sector_via_mx(stock_code: str) -> str:
    """通过 mx-data 获取股票所属板块（行业）"""
    try:
        sys.path.insert(0, str(SKILLS_DIR / "mx-data"))
        from mx_data import MXData
        import os
        api_key = os.environ.get("MX_APIKEY") or os.environ.get("MINIMAX_API_KEY", "")
        tool = MXData(api_key=api_key)
        result = tool.query(f"{stock_code} 所属行业")
        tables, _, _, err = tool.parse_result(result)
        if err or not tables:
            return ""

        rows = tables[0].get("rows", [])
        if rows:
            # 优先取申万行业（最常用），其次中信证券
            row = rows[0]
            sector = (row.get("申万行业分类(2021)") or row.get("中信证券行业分类(2020)") or
                     row.get("证监会行业分类(2012)") or row.get("东财行业(2016)") or "").strip()
            return sector
    except Exception as e:
        logger.warning(f"mx-data 板块获取失败 {stock_code}: {e}")
    return ""


def _fetch_sector_via_akshare(stock_code: str) -> str:
    """通过 akshare 东方财富个股信息获取行业，作为 mx-data 超额时的兜底。"""
    if os.getenv("ENABLE_AKSHARE_SECTOR_FALLBACK", "0") != "1":
        return ""
    try:
        import akshare as ak

        df = retry_call(
            f"akshare 板块 {stock_code}",
            lambda: ak.stock_individual_info_em(symbol=stock_code),
            retries=1,
            base_delay=1.0,
            throttle_key="akshare-sector",
            min_interval=1.0,
        )
        if df is None or df.empty:
            return ""

        # stock_individual_info_em 通常返回 item/value 或 项目/值 两列。
        key_col = None
        value_col = None
        for col in df.columns:
            low = str(col).lower()
            if low in ("item", "项目") or "项目" in str(col):
                key_col = col
            if low in ("value", "值") or "值" in str(col):
                value_col = col
        if key_col is None or value_col is None:
            if len(df.columns) >= 2:
                key_col, value_col = df.columns[:2]
            else:
                return ""

        for _, row in df.iterrows():
            key = str(row.get(key_col, "")).strip()
            val = str(row.get(value_col, "")).strip()
            if key in ("行业", "所属行业", "行业分类") and val and val.lower() != "nan":
                return val
    except Exception as e:
        logger.warning(f"akshare 板块获取失败 {stock_code}: {e}")
    return ""


def _fetch_news_via_mxsearch(stock_name: str, max_results: int = 5) -> List[Dict]:
    """通过 mx-search 获取个股近7天资讯，返回去重后的新闻列表"""
    import os
    api_key = os.environ.get("MX_APIKEY", "")
    if not api_key:
        return []
    import requests
    url = "https://mkapi2.dfcfs.com/finskillshub/api/claw/news-search"
    headers = {"Content-Type": "application/json", "apikey": api_key}
    today = date.today()
    query = (
        f"{stock_name} 最新公告 重大消息 近7天 "
        f"截至{today.strftime('%Y-%m-%d')}"
    )
    try:
        resp = retry_call(
            f"mx-search {stock_name}",
            lambda: request_direct("POST", url, headers=headers, json={"query": query}, timeout=20),
            retries=3,
            base_delay=2,
            throttle_key="mx-search",
            min_interval=1.2,
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        raw = data if isinstance(data, dict) else {}
        items = raw.get("data", {}).get("data", {}).get("llmSearchResponse", {}).get("data", [])
        if not items:
            return []
        news_items = []
        seen_titles = set()
        for item in items:
            title = str(item.get("title", "")[:80])
            if not title or len(title) < 5 or title in seen_titles:
                continue
            seen_titles.add(title)
            content = str(item.get("content", "")[:200])
            date_str = str(item.get("date", "")[:16])
            source = str(item.get("source", ""))
            news_items.append({"title": title, "content": content, "time": date_str, "source": source})
            if len(news_items) >= max_results:
                break
        return news_items
    except Exception:
        return []


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


_FINANCIAL_FIELD_ALIASES = {
    "roe_annual_latest": ("roe_annual_latest", "roe", "ROE"),
    "roe_quarter_latest": ("roe_quarter_latest", "roe_quarter"),
    "revenue_growth_yoy": ("revenue_growth_yoy", "revenue_growth", "营收增速", "营业收入同比增长"),
    "net_profit_growth_yoy": ("net_profit_growth_yoy", "net_profit_growth", "净利润增长率", "净利润同比增长"),
    "gross_margin": ("gross_margin", "毛利率", "销售毛利率"),
    "debt_asset_ratio": ("debt_asset_ratio", "debt_ratio", "负债率", "资产负债率"),
    "pe_ttm": ("pe_ttm", "pe", "市盈率"),
    "pb": ("pb", "市净率"),
    "total_market_cap": ("total_market_cap", "market_cap", "总市值"),
    "operating_cash_flow": ("operating_cash_flow", "cash_flow", "经营现金流"),
    "book_value_per_share": ("book_value_per_share", "每股净资产"),
    "sector": ("sector", "industry", "行业", "所属行业"),
    "_as_of": ("_as_of", "report_date", "end_date", "m_timetag"),
}


def normalize_financial_record(record: Optional[Dict]) -> Dict:
    """保留原字段，并补齐 Phase 2 使用的统一财务字段名。"""
    if not isinstance(record, dict):
        return {}
    normalized = dict(record)
    for canonical, aliases in _FINANCIAL_FIELD_ALIASES.items():
        for field in aliases:
            value = record.get(field)
            if value is not None and value != "":
                normalized[canonical] = value
                break
    return normalized


def _parse_ddx_ddy_tables(tables: Any) -> tuple[Optional[float], Optional[float], List[str]]:
    """Parse only explicit 5-day DDX and 10-day DDY fields."""
    ddx_value: Optional[float] = None
    ddy_value: Optional[float] = None
    ddx_priority = -1
    ddy_priority = -1
    observed_columns: List[str] = []

    def parse_index(raw: Any) -> Optional[float]:
        if raw in (None, ""):
            return None
        try:
            text = str(raw).strip().replace(",", "").replace("%", "")
            if text.lower() in {"nan", "none", "--", "-"}:
                return None
            return round(float(text), 4)
        except (TypeError, ValueError):
            return None

    for table in tables or []:
        if not isinstance(table, dict):
            continue
        for row in table.get("rows", []) or []:
            if not isinstance(row, dict):
                continue
            row_label = " ".join(
                str(value).strip()
                for value in row.values()
                if value not in (None, "") and parse_index(value) is None
            )
            row_numbers = [
                parse_index(value)
                for value in row.values()
                if parse_index(value) is not None
            ]
            for raw_key, raw_value in row.items():
                key = str(raw_key).strip()
                if key and key not in observed_columns:
                    observed_columns.append(key)
                normalized = re.sub(r"[\s_\-()（）\[\]：:]", "", key).upper()
                value = parse_index(raw_value)
                if value is None:
                    continue
                if "DDX" in normalized and any(x in normalized for x in ("5日", "DDX5", "5DDX")):
                    if 3 > ddx_priority:
                        ddx_value, ddx_priority = value, 3
                if "DDY" in normalized and any(x in normalized for x in ("10日", "DDY10", "10DDY")):
                    if 3 > ddy_priority:
                        ddy_value, ddy_priority = value, 3
            normalized_label = re.sub(r"[\s_\-()（）\[\]：:]", "", row_label).upper()
            if row_numbers and "DDX" in normalized_label and any(
                x in normalized_label for x in ("5日", "DDX5", "5DDX")
            ):
                if 3 > ddx_priority:
                    ddx_value, ddx_priority = row_numbers[-1], 3
            if row_numbers and "DDY" in normalized_label and any(
                x in normalized_label for x in ("10日", "DDY10", "10DDY")
            ):
                if 3 > ddy_priority:
                    ddy_value, ddy_priority = row_numbers[-1], 3
    return ddx_value, ddy_value, observed_columns


def _financial_record_has_any_value(record: Optional[Dict]) -> bool:
    normalized = normalize_financial_record(record)
    return any(
        normalized.get(field) is not None and normalized.get(field) != ""
        for field in (
            "roe_annual_latest",
            "roe_quarter_latest",
            "revenue_growth_yoy",
            "net_profit_growth_yoy",
            "gross_margin",
            "debt_asset_ratio",
            "pe_ttm",
            "pb",
            "total_market_cap",
            "operating_cash_flow",
            "book_value_per_share",
        )
    )


def _fetch_financial_via_xqshare(stock_code: str) -> Optional[Dict]:
    """
    通过 QMT HTTP API 获取单只股票财务数据
    字段映射：xtquant → Phase 2 辩论包字段
    """
    try:
        import urllib.request, json
        full_code = _ensure_suffix(stock_code)
        url = f"{QMT_HTTP_URL}/financial_data?stocks={full_code}&tables=PERSHAREINDEX"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
        if not raw.get("success"):
            logger.warning(f"QMT HTTP 财务返回失败: {stock_code}")
            return None
        data = raw.get("data", {})
        if not data or full_code not in data:
            logger.warning(f"QMT HTTP 财务数据返回空: {stock_code}")
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
            if v is None or (isinstance(v, float) and v != v):
                return None
            try:
                return round(float(v), 2)
            except (ValueError, TypeError):
                return None
        annual_rows = sorted([r for r in rows if str(r[col_idx["m_timetag"]]).endswith("1231")],
                          key=lambda r: str(r[col_idx["m_timetag"]]), reverse=True)
        quarter_rows = sorted([r for r in rows if not str(r[col_idx["m_timetag"]]).endswith("1231")],
                              key=lambda r: str(r[col_idx["m_timetag"]]), reverse=True)
        annual_row = annual_rows[0] if annual_rows else None
        quarter_row = quarter_rows[0] if quarter_rows else None
        roe_annual = get_val(annual_row, "equity_roe") if annual_row else None
        if roe_annual is None:
            return None
        rev_growth = get_val(quarter_row, "inc_revenue_rate") if quarter_row else None
        if rev_growth is None and annual_row:
            rev_growth = get_val(annual_row, "inc_revenue_rate")
        profit_growth = get_val(quarter_row, "inc_net_profit_rate") if quarter_row else None
        if profit_growth is None and annual_row:
            profit_growth = get_val(annual_row, "inc_net_profit_rate")
        # PE/PB from HTTP K-line
        pe = None
        pb = None
        try:
            eps = get_val(annual_row, "s_fa_eps_basic")
            bps = get_val(quarter_row, "s_fa_bps") if quarter_row else None
            if eps and eps > 0:
                price_url = f"{QMT_HTTP_URL}/market_data3?stock={full_code}&period=1d&count=1"
                preq = urllib.request.Request(price_url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(preq, timeout=10) as pr:
                    pd_data = json.loads(pr.read().decode("utf-8"))
                close_data = pd_data.get("data", {}).get("close", {})
                dates = sorted(close_data.keys(), reverse=True)
                if dates:
                    inner = close_data[dates[0]]
                    price = inner.get(full_code) if isinstance(inner, dict) else inner
                    if price and float(price) > 0:
                        pe = round(float(price) / eps, 2)
                        if bps and bps > 0:
                            pb = round(float(price) / bps, 2)
        except Exception:
            pass
        return {
            "roe_annual_latest": roe_annual,
            "roe_quarter_latest": get_val(quarter_row, "equity_roe") if quarter_row else None,
            "revenue_growth_yoy": rev_growth,
            "net_profit_growth_yoy": profit_growth,
            "gross_margin": get_val(annual_row, "sales_gross_profit") or get_val(annual_row, "gross_profit"),
            "debt_asset_ratio": get_val(annual_row, "gear_ratio") if annual_row else None,
            "pe_ttm": pe,
            "pb": pb,
            "_as_of": _normalize_kline_date(
                quarter_row[col_idx["m_timetag"]] if quarter_row and "m_timetag" in col_idx else "",
                dashed=False,
            ),
        }
    except Exception as e:
        logger.warning(f"QMT HTTP 财务获取失败 {stock_code}: {e}")
        return None


def _fetch_money_flow_via_mx(stock_code: str) -> Optional[Dict]:
    """
    通过 mx-data 获取资金流数据（主力净流入、DDX/DDY、超大单净流入等）
    返回格式供辩论包使用
    """
    global _MX_MONEY_FLOW_FAIL_STREAK, _MX_MONEY_FLOW_DISABLED_UNTIL, _MX_MONEY_FLOW_LAST_QUERY_AT
    now = time.monotonic()
    with _MX_MONEY_FLOW_LOCK:
        if _MX_MONEY_FLOW_DISABLED_UNTIL > now:
            return {}

    try:
        sys.path.insert(0, str(SKILLS_DIR / "mx-data"))
        from mx_data import MXData
        import os
        api_key = os.environ.get("MX_APIKEY") or os.environ.get("MINIMAX_API_KEY", "")
        tool = MXData(api_key=api_key)
        code6 = str(stock_code).strip().split(".", 1)[0].zfill(6)
        full_code = _ensure_suffix(code6)

        def mx_query(query: str):
            return _mx_money_flow_query_with_global_gate(tool, query)

        def parse_tables(result, label: str) -> List[Dict]:
            if result is None:
                raise RuntimeError(f"{label} returned empty response")
            parsed = tool.parse_result(result)
            if not isinstance(parsed, (list, tuple)) or not parsed:
                return []
            err = parsed[3] if len(parsed) >= 4 else None
            if err:
                if "状态码 112" in str(err) or "请求频率" in str(err) or "频率过高" in str(err):
                    raise _MXMoneyFlowTooFrequent(str(err))
                if "状态码 113" in str(err) or "调用次数已达" in str(err):
                    raise _MXMoneyFlowRateLimited(str(err))
                if (
                    "状态码 114" in str(err)
                    or "API密钥不存在" in str(err)
                    or "API密钥失效" in str(err)
                    or "api key" in str(err).lower()
                ):
                    raise _MXMoneyFlowAuthError(str(err))
                logger.debug(f"{label} parse empty: {err}")
                return []
            tables = parsed[0]
            return tables if isinstance(tables, list) else []

        def parse_yi(val_str: str) -> Optional[float]:
            """解析 '1.64亿元' -> 1.64, '-9901万元' -> -0.9901"""
            if not val_str:
                return None
            val_str = str(val_str).strip().replace(',', '')
            try:
                if '亿' in val_str:
                    cleaned = re.sub(r"(亿元|亿港元|亿)", "", val_str)
                    return round(float(cleaned), 2)
                elif '万' in val_str:
                    cleaned = re.sub(r"(万元|万港元|万)", "", val_str)
                    return round(float(cleaned) / 10000, 4)
                else:
                    return round(float(val_str), 2)
            except (ValueError, AttributeError):
                return None

        def pick_money_value(
            row: Dict[str, Any],
            preferred_keys: List[str],
            *,
            allow_symbol_fallback: bool = True,
        ) -> Optional[float]:
            for key in preferred_keys:
                if key in row and row.get(key) not in (None, ""):
                    value = parse_yi(str(row.get(key)))
                    if value is not None:
                        return value
            if not allow_symbol_fallback:
                return None
            # mx-data 对 A/H 同名股会返回 “公司名(601916.SH)” 这种列名。
            for key, raw in row.items():
                key_text = str(key)
                if key_text.lower() == "date":
                    continue
                if code6 in key_text or full_code in key_text:
                    value = parse_yi(str(raw))
                    if value is not None:
                        return value
            # 最后兜底：单股票标准查询通常只有 date + 一个金额列。
            for key, raw in row.items():
                if str(key).lower() == "date":
                    continue
                value = parse_yi(str(raw))
                if value is not None:
                    return value
            return None

        def fetch_once() -> Dict[str, Optional[float]]:
            # 初始化默认值，避免作用域问题
            main_net_flow = None
            super_net_flow = None
            ddx_5 = None
            ddy_10 = None
            ddx_ddy_columns: List[str] = []
            as_of = ""

            # ── 查询1：主力净流入 + 超大单 ────────────────────────
            result = mx_query(f"{code6} 主力净流入资金 主力净额")
            tables = parse_tables(result, "mx main-flow query")
            for t in (tables or []):
                if not isinstance(t, dict):
                    continue
                for row in (t.get("rows", []) or []):
                    if not isinstance(row, dict):
                        continue
                    if not as_of and row.get("date"):
                        as_of = _normalize_kline_date(row.get("date"), dashed=False)
                    if main_net_flow is None:
                        main_net_flow = pick_money_value(row, ["主力净流入资金", "主力净流入", "主力净额", "净额"])
                    if super_net_flow is None:
                        super_net_flow = pick_money_value(
                            row,
                            ["超大单净流入资金", "超大单净额", "超大单流入"],
                            allow_symbol_fallback=False,
                        )

            # ── 查询2：DDX/DDY 指标（用DDX DDX查询，返回所有DDX/DDY字段）──
            result2 = mx_query(f"{code6} DDX DDY")
            tables2 = parse_tables(result2, "mx ddx query")
            ddx_5, ddy_10, ddx_ddy_columns = _parse_ddx_ddy_tables(tables2)
            if ddx_5 is None or ddy_10 is None:
                logger.info(
                    "mx-data DDX/DDY辅助字段不完整 %s: ddx_5=%s ddy_10=%s columns=%s",
                    code6,
                    ddx_5,
                    ddy_10,
                    ddx_ddy_columns[:20],
                )

            # ── 查询3：补充超大单（如缺失）─────────────────────────
            if super_net_flow is None:
                result3 = mx_query(f"{code6} 超大单净额 超大单净流入资金")
                tables3 = parse_tables(result3, "mx super-flow query")
                for t in (tables3 or []):
                    if not isinstance(t, dict):
                        continue
                    for row in (t.get("rows", []) or []):
                        if not isinstance(row, dict):
                            continue
                        super_net_flow = pick_money_value(row, ["超大单净额", "超大单净流入资金"])
                        if super_net_flow is not None:
                            break

            return {
                "main_net_flow": main_net_flow,    # 亿元（正=净流入，负=净流出）
                "super_net_flow": super_net_flow,  # 亿元
                "ddx_5": ddx_5,                    # 5日DDX（主力关注度）
                "ddy_10": ddy_10,                  # 10日DDY（趋势强度）
                "source": "mx-data",
                "as_of": as_of,
                "aux_checked": True,
                "diagnostics": {
                    "ddx_ddy_columns": ddx_ddy_columns[:30],
                    "ddx_missing_after_parse": ddx_5 is None,
                    "ddy_missing_after_parse": ddy_10 is None,
                },
            }

        retries = _env_int("MX_MONEY_FLOW_112_RETRIES", 3, minimum=0)
        retry_delays_raw = os.environ.get("MX_MONEY_FLOW_112_RETRY_DELAYS_SEC", "15,30,60")
        retry_delays: List[float] = []
        for part in retry_delays_raw.split(","):
            try:
                retry_delays.append(max(0.0, float(part.strip())))
            except Exception:
                continue
        if not retry_delays:
            retry_delays = [5.0, 10.0, 20.0]

        last_112: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                out = fetch_once()
                break
            except _MXMoneyFlowTooFrequent as e:
                last_112 = e
                if attempt >= retries:
                    logger.warning(f"mx-data 资金流请求频率过高，重试耗尽后跳过本股: {stock_code}: {e}")
                    return {
                        "main_net_flow": None,
                        "super_net_flow": None,
                        "ddx_5": None,
                        "ddy_10": None,
                    }
                delay = retry_delays[min(attempt, len(retry_delays) - 1)]
                if delay > 0:
                    time.sleep(delay)
                logger.warning(
                    f"mx-data 资金流请求频率过高，等待{delay:g}s后重试 "
                    f"({attempt + 1}/{retries}) {stock_code}: {e}"
                )
        else:
            raise last_112 or RuntimeError("mx-data 资金流未知重试失败")

        with _MX_MONEY_FLOW_LOCK:
            if _money_flow_has_any_value(out):
                _MX_MONEY_FLOW_FAIL_STREAK = 0
                _MX_MONEY_FLOW_DISABLED_UNTIL = 0.0
            else:
                _MX_MONEY_FLOW_FAIL_STREAK += 1
        return out
    except _MXMoneyFlowRateLimited as e:
        cooldown_sec = _env_int("MX_MONEY_FLOW_RATE_LIMIT_COOLDOWN_SEC", 1800, minimum=60)
        with _MX_MONEY_FLOW_LOCK:
            _MX_MONEY_FLOW_FAIL_STREAK += 1
            _MX_MONEY_FLOW_DISABLED_UNTIL = time.monotonic() + cooldown_sec
        logger.warning(f"mx-data 资金流触发限流熔断: 冷却{cooldown_sec}s, {stock_code}: {e}")
        return {}
    except _MXMoneyFlowAuthError as e:
        cooldown_sec = _env_int("MX_MONEY_FLOW_AUTH_COOLDOWN_SEC", 1800, minimum=60)
        with _MX_MONEY_FLOW_LOCK:
            _MX_MONEY_FLOW_FAIL_STREAK += 1
            _MX_MONEY_FLOW_DISABLED_UNTIL = time.monotonic() + cooldown_sec
        logger.error(f"mx-data 资金流认证失败: 冷却{cooldown_sec}s, {stock_code}: {e}")
        return {
            "main_net_flow": None,
            "super_net_flow": None,
            "ddx_5": None,
            "ddy_10": None,
            "source": "mx-data",
            "status": "auth_error",
            "error": str(e),
            "aux_checked": False,
            "diagnostics": {"auth_error": True},
        }
    except Exception as e:
        cooldown_sec = _env_int("MX_MONEY_FLOW_COOLDOWN_SEC", 900, minimum=30)
        threshold = _env_int("MX_MONEY_FLOW_FAIL_THRESHOLD", 3, minimum=1)
        with _MX_MONEY_FLOW_LOCK:
            _MX_MONEY_FLOW_FAIL_STREAK += 1
            if _MX_MONEY_FLOW_FAIL_STREAK >= threshold:
                _MX_MONEY_FLOW_DISABLED_UNTIL = time.monotonic() + cooldown_sec
                logger.warning(
                    f"mx-data 资金流临时熔断: 连续失败={_MX_MONEY_FLOW_FAIL_STREAK}, 冷却{cooldown_sec}s"
                )
        logger.warning(f"mx-data 资金流获取失败 {stock_code}: {e}")
        return {}


def _parse_money_amount_to_yi(val) -> Optional[float]:
    """把元/万元/亿元形式统一转成亿元。"""
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            if pd.isna(val):
                return None
            v = float(val)
            return round(v / 100000000, 4) if abs(v) > 10000 else round(v, 4)
        s = str(val).strip().replace(",", "")
        if not s or s.lower() in ("nan", "none", "--", "-"):
            return None
        if "亿元" in s or s.endswith("亿"):
            return round(float(s.replace("亿元", "").replace("亿", "")), 4)
        if "万元" in s or s.endswith("万"):
            return round(float(s.replace("万元", "").replace("万", "")) / 10000, 4)
        v = float(s)
        return round(v / 100000000, 4) if abs(v) > 10000 else round(v, 4)
    except Exception:
        return None


def _first_present(row, names: List[str]):
    for name in names:
        if name in row and row.get(name) is not None:
            return row.get(name)
    return None


def _fetch_money_flow_via_akshare(stock_code: str) -> Dict:
    """
    通过 akshare 获取资金流数据（主力净流入、超大单、DDX/DDY近似值）
    作为 mx-data 失败的兜底方案
    """
    try:
        import akshare as ak
        import pandas as pd

        # 判断市场
        market = "sz" if stock_code.startswith(("000", "001", "002", "003", "300")) else "sh"


        df = _retry_call(
            f"akshare 个股资金流 {stock_code}",
            lambda: ak.stock_individual_fund_flow(stock=stock_code, market=market),
        )
        if df is None or len(df) < 5:
            return {}


        date_col = next((c for c in df.columns if str(c) in {"日期", "date", "交易日期"}), None)
        ordered = df.copy()
        if date_col:
            ordered["__date"] = pd.to_datetime(ordered[date_col], errors="coerce")
            ordered = ordered.sort_values("__date")

        # 使用真实最新交易日，并从同一有序序列计算累计主力净流入。
        latest = ordered.iloc[-1]
        main_net = latest.get("主力净流入-净额")
        super_net = latest.get("超大单净流入-净额")

        main_net_flow = _parse_money_amount_to_yi(main_net)
        super_net_flow = _parse_money_amount_to_yi(super_net)

        # 计算5日DDX近似值（5日主力净流入累计）
        ordered["主力净额_num"] = pd.to_numeric(ordered["主力净流入-净额"], errors="coerce").fillna(0)
        flow_5_raw = ordered["主力净额_num"].tail(5).sum() if len(ordered) >= 5 else None
        flow_5 = float(round(flow_5_raw / 100000000, 3)) if flow_5_raw is not None else None

        # 计算10日DDY近似值（10日主力净流入累计）
        flow_10_raw = ordered["主力净额_num"].tail(10).sum() if len(ordered) >= 10 else None
        flow_10 = float(round(flow_10_raw / 100000000, 3)) if flow_10_raw is not None else None

        return {
            "main_net_flow": float(main_net_flow) if main_net_flow is not None else None,
            "super_net_flow": float(super_net_flow) if super_net_flow is not None else None,
            "main_net_flow_5d": flow_5,
            "main_net_flow_10d": flow_10,
            "source": "akshare",
            "as_of": _normalize_kline_date(latest.get(date_col), dashed=False) if date_col else "",
        }
    except Exception as e:
        logger.warning(f"akshare 资金流获取失败 {stock_code}: {e}")
        return {}


def _eastmoney_secid(stock_code: str) -> str:
    code = str(stock_code).zfill(6)
    market = 1 if code.startswith(("5", "6", "9")) else 0
    return f"{market}.{code}"


def _fetch_money_flow_via_eastmoney_direct(stock_code: str) -> Dict:
    """
    通过东财 push2his 直连接口获取资金流（日线序列），
    不依赖 akshare 封装，降低封装层抽风导致的缺失。
    """
    global _EASTMONEY_FAIL_STREAK, _EASTMONEY_DISABLED_UNTIL
    now = time.monotonic()
    with _EASTMONEY_LOCK:
        if _EASTMONEY_DISABLED_UNTIL > now:
            return {}

    code = str(stock_code).zfill(6)
    secid = _eastmoney_secid(code)
    urls = [
        "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get",
        "http://80.push2.eastmoney.com/api/qt/stock/fflow/daykline/get",
        "http://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get",
    ]
    url_limit = _env_int("EASTMONEY_FLOW_URL_LIMIT", 1, minimum=1)
    urls = urls[:min(len(urls), url_limit)]
    request_timeout = _env_float("EASTMONEY_FLOW_TIMEOUT_SEC", 4.0, minimum=1.0)
    request_retries = _env_int("EASTMONEY_FLOW_RETRIES", 1, minimum=1)
    params = {
        "secid": secid,
        "klt": "101",
        "lmt": "20",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://quote.eastmoney.com/",
        "Origin": "https://quote.eastmoney.com",
    }

    last_err = None
    for url in urls:
        try:
            def _req():
                resp = request_direct("GET", url, params=params, headers=headers, timeout=request_timeout)
                resp.raise_for_status()
                return resp.json()

            payload = retry_call(
                f"eastmoney 资金流直连[{urllib.parse.urlparse(url).netloc}] {code}",
                _req,
                retries=request_retries,
                base_delay=0.8,
                throttle_key="eastmoney-fflow",
                min_interval=0.6,
            )
            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            klines = data.get("klines", []) if isinstance(data, dict) else []
            if not isinstance(klines, list) or not klines:
                continue

            rows: List[Dict[str, Any]] = []
            for line in klines:
                parts = str(line or "").split(",")
                if len(parts) < 6:
                    continue
                # 约定顺序: 日期,主力,小单,中单,大单,超大单,...
                main_net_flow = _parse_money_amount_to_yi(parts[1])
                super_net_flow = _parse_money_amount_to_yi(parts[5])
                rows.append({
                    "date": parts[0],
                    "main_net_flow": main_net_flow,
                    "super_net_flow": super_net_flow,
                })
            if not rows:
                continue

            latest = next(
                (r for r in reversed(rows) if r.get("main_net_flow") is not None or r.get("super_net_flow") is not None),
                rows[-1],
            )
            main_series = [float(r["main_net_flow"]) for r in rows if r.get("main_net_flow") is not None]
            flow_5 = round(sum(main_series[-5:]), 3) if main_series else None
            flow_10 = round(sum(main_series[-10:]), 3) if main_series else None
            out = {
                "main_net_flow": latest.get("main_net_flow"),
                "super_net_flow": latest.get("super_net_flow"),
                "main_net_flow_5d": flow_5,
                "main_net_flow_10d": flow_10,
                "source": "eastmoney",
                "as_of": _normalize_kline_date(latest.get("date"), dashed=False),
            }
            with _EASTMONEY_LOCK:
                _EASTMONEY_FAIL_STREAK = 0
                _EASTMONEY_DISABLED_UNTIL = 0.0
            return out
        except Exception as e:
            last_err = e
            continue

    if last_err is not None:
        cooldown_sec = _env_int("EASTMONEY_FLOW_COOLDOWN_SEC", 900, minimum=30)
        threshold = _env_int("EASTMONEY_FLOW_FAIL_THRESHOLD", 2, minimum=1)
        with _EASTMONEY_LOCK:
            _EASTMONEY_FAIL_STREAK += 1
            if _EASTMONEY_FAIL_STREAK >= max(1, threshold):
                _EASTMONEY_DISABLED_UNTIL = time.monotonic() + max(30, cooldown_sec)
                logger.warning(
                    f"eastmoney 资金流临时熔断: 连续失败={_EASTMONEY_FAIL_STREAK}, 冷却{max(30, cooldown_sec)}s"
                )
        logger.warning(f"eastmoney 资金流直连失败 {code}: {last_err}")
    return {}


def _fetch_money_flow_via_akshare_rank(stock_code: str) -> Dict:
    """
    通过 akshare 东方财富资金流排名补缺。

    这个接口一次返回全市场排名，适合在 mx-data 超额、个股资金流接口不稳定时，
    补充今日/5日主力净流入等核心字段。
    """
    code = str(stock_code).zfill(6)
    result: Dict[str, Optional[float]] = {}
    for indicator in ("今日", "5日", "10日"):
        rank_map = _get_money_flow_rank_map(indicator)
        row = rank_map.get(code, {})
        if not row:
            continue
        if indicator == "今日":
            if row.get("main_net_flow") is not None:
                result["main_net_flow"] = row.get("main_net_flow")
            if row.get("super_net_flow") is not None:
                result["super_net_flow"] = row.get("super_net_flow")
        elif indicator == "5日" and row.get("main_net_flow") is not None:
            result["main_net_flow_5d"] = row.get("main_net_flow")
        elif indicator == "10日" and row.get("main_net_flow") is not None:
            result["main_net_flow_10d"] = row.get("main_net_flow")
    if result:
        result["source"] = "ak_rank"
        result["as_of"] = ""
    return result


_MONEY_FLOW_RANK_CACHE: Dict[str, Dict] = {}


def _load_money_flow_rank_fallback(indicator: str) -> Dict:
    """读取最近一次可用的全市场资金流排名缓存。"""
    for old_path in sorted(CACHE_DIR.glob(f"money_flow_rank_*_{indicator}.json"), reverse=True):
        fallback = _load_json_cache(old_path, {})
        if fallback:
            return fallback
    return {}


def _get_money_flow_rank_map(indicator: str) -> Dict:
    """批量拉取并缓存全市场资金流排名，避免每只股票重复请求 akshare。"""
    today = date.today().strftime("%Y%m%d")
    cache_key = f"{today}_{indicator}"
    if cache_key in _MONEY_FLOW_RANK_CACHE:
        return _MONEY_FLOW_RANK_CACHE[cache_key]

    path = CACHE_DIR / f"money_flow_rank_{today}_{indicator}.json"
    cached = _load_json_cache(path, {})
    if cached:
        _MONEY_FLOW_RANK_CACHE[cache_key] = cached
        return cached

    if os.getenv("MONEY_FLOW_RANK_CACHE_ONLY") == "1" or not _env_bool("ENABLE_LIVE_MONEY_FLOW_RANK", "0"):
        fallback = _load_money_flow_rank_fallback(indicator)
        _MONEY_FLOW_RANK_CACHE[cache_key] = fallback
        return fallback

    try:
        import akshare as ak
        df = _retry_call(
            f"akshare 资金流排名[{indicator}]",
            lambda: ak.stock_individual_fund_flow_rank(indicator=indicator),
            retries=2,
        )
        if df is None or df.empty:
            raise RuntimeError("empty rank dataframe")

        code_col = "代码" if "代码" in df.columns else next((c for c in df.columns if "代码" in str(c)), None)
        if code_col is None:
            raise RuntimeError("missing code column")

        rank_map: Dict[str, Dict] = {}
        for _, row in df.iterrows():
            code = str(row.get(code_col, "")).zfill(6)
            if not code or code == "000000":
                continue
            main = _first_present(row, [
                "主力净流入-净额", "主力净流入净额", "主力净流入", "主力净额",
                f"{indicator}主力净流入-净额", f"{indicator}主力净额",
            ])
            super_flow = _first_present(row, [
                "超大单净流入-净额", "超大单净流入净额", "超大单净流入", "超大单净额",
            ])
            rank_map[code] = {
                "main_net_flow": _parse_money_amount_to_yi(main),
                "super_net_flow": _parse_money_amount_to_yi(super_flow),
            }

        _save_json_cache(path, rank_map)
        _MONEY_FLOW_RANK_CACHE[cache_key] = rank_map
        return rank_map
    except Exception as e:
        logger.warning(f"akshare 资金流排名[{indicator}]批量获取失败: {e}")
        fallback = _load_money_flow_rank_fallback(indicator)
        _MONEY_FLOW_RANK_CACHE[cache_key] = fallback
        return fallback


def _money_flow_coverage(money_flow: Dict) -> int:
    return sum(
        1
        for k in (
            "main_net_flow",
            "super_net_flow",
            "ddx_5",
            "ddy_10",
            "main_net_flow_5d",
            "main_net_flow_10d",
        )
        if money_flow.get(k) is not None
    )


def _sanitize_legacy_cached_money_flow(money_flow: Dict[str, Any]) -> Dict[str, Any]:
    """Do not reuse legacy DDX/DDY values whose source semantics were not recorded."""
    normalized = dict(money_flow or {})
    source = str(normalized.get("source") or "").lower()
    has_provenance = bool(normalized.get("field_sources")) and bool(normalized.get("units"))
    ambiguous_source = not source or source in {"none", "cache", "cache_hot", "cache_stale"}
    if has_provenance or not ambiguous_source:
        return normalized
    if normalized.get("ddx_5") is None and normalized.get("ddy_10") is None:
        return normalized
    normalized["ddx_5"] = None
    normalized["ddy_10"] = None
    normalized["legacy_semantics_ignored"] = True
    flags = list(normalized.get("quality_flags") or [])
    if "MONEY_FLOW_SEMANTICS_LEGACY" not in flags:
        flags.append("MONEY_FLOW_SEMANTICS_LEGACY")
    normalized["quality_flags"] = flags
    return normalized


def _get_cached_money_flow(stock_code: str, *, allow_partial: bool = True, allow_stale: bool = False) -> Dict:
    """读取 Phase2 当日预获取缓存中的资金流。

    旧独立资金流缓存已废弃；资金流以 debate_data_cache.json 为唯一主缓存。
    为避免跨日旧数据污染，updated 不是今天的记录一律不参与读取。
    """
    key = str(stock_code).zfill(6)
    today = date.today().strftime("%Y%m%d")
    debate_cache = _load_debate_data_cache()
    item = debate_cache.get(key, {}) if isinstance(debate_cache.get(key, {}), dict) else {}
    money_flow = item.get("money_flow", {}) if isinstance(item.get("money_flow", {}), dict) else {}
    money_flow = _sanitize_legacy_cached_money_flow(money_flow)
    if not money_flow or not _money_flow_has_any_value(money_flow):
        return {}
    if str(item.get("updated", "") or "") != today:
        return {}
    if not allow_partial and _money_flow_is_partial(money_flow):
        return {}
    return money_flow


def _set_debate_cached_money_flow(stock_code: str, money_flow: Dict) -> None:
    """把构包阶段现场补到的资金流写回当日预获取缓存。"""
    if not money_flow or not _money_flow_has_any_value(money_flow):
        return
    key = str(stock_code).zfill(6)
    cache = _load_debate_data_cache()
    item = cache.get(key, {}) if isinstance(cache.get(key, {}), dict) else {}
    old_money_flow = item.get("money_flow", {}) if isinstance(item.get("money_flow", {}), dict) else {}
    old_coverage = _money_flow_coverage(old_money_flow)
    new_coverage = _money_flow_coverage(money_flow)
    old_has_main = old_money_flow.get("main_net_flow") is not None
    new_has_main = money_flow.get("main_net_flow") is not None
    if old_money_flow and ((old_has_main and not new_has_main) or old_coverage > new_coverage):
        return
    item["money_flow"] = money_flow
    item["updated"] = date.today().strftime("%Y%m%d")
    cache[key] = item
    _save_debate_data_cache(cache)

def _merge_money_flow(*sources: Dict) -> Dict:
    """按字段合并资金流：前面的源优先，后面的源只补 None/缺失。"""
    fields = (
        "main_net_flow",
        "super_net_flow",
        "ddx_5",
        "ddy_10",
        "main_net_flow_5d",
        "main_net_flow_10d",
    )
    merged = {key: None for key in fields}
    field_sources: Dict[str, str] = {}
    field_as_of: Dict[str, str] = {}
    for source in sources:
        if not source:
            continue
        source_name = str(source.get("source") or "unknown")
        source_as_of = str(source.get("as_of") or "")
        source_field_sources = source.get("field_sources") or {}
        source_field_as_of = source.get("field_as_of") or {}
        for key in fields:
            value = source.get(key)
            if merged[key] is None and value is not None:
                merged[key] = value
                field_sources[key] = str(source_field_sources.get(key) or source_name)
                field_as_of[key] = str(source_field_as_of.get(key) or source_as_of)
    used_sources = _uniq_keep_order(list(field_sources.values()))
    merged["source"] = "+".join(used_sources) if used_sources else "none"
    merged["as_of"] = max((x for x in field_as_of.values() if x), default="")
    merged["field_sources"] = field_sources
    merged["field_as_of"] = field_as_of
    merged["units"] = {
        "main_net_flow": "CNY_100M",
        "super_net_flow": "CNY_100M",
        "ddx_5": "DDX_INDEX",
        "ddy_10": "DDY_INDEX",
        "main_net_flow_5d": "CNY_100M",
        "main_net_flow_10d": "CNY_100M",
    }
    merged["aux_checked"] = any(bool(source.get("aux_checked")) for source in sources if source)
    diagnostics: Dict[str, Any] = {}
    for source in sources:
        if isinstance(source, dict) and isinstance(source.get("diagnostics"), dict):
            diagnostics.update(source["diagnostics"])
    if diagnostics:
        merged["diagnostics"] = diagnostics
    return merged


def _empty_money_flow(source: str = "none") -> Dict:
    return _merge_money_flow({
        "main_net_flow": None,
        "super_net_flow": None,
        "ddx_5": None,
        "ddy_10": None,
        "main_net_flow_5d": None,
        "main_net_flow_10d": None,
        "source": source,
    })


def _money_flow_has_gap(money_flow: Dict) -> bool:
    """Core execution-data gap; DDX/DDY are tracked separately as auxiliaries."""
    return any(money_flow.get(k) is None for k in ("main_net_flow", "super_net_flow"))


def _money_flow_aux_has_gap(money_flow: Dict) -> bool:
    return any(money_flow.get(k) is None for k in ("ddx_5", "ddy_10"))


def _money_flow_is_partial(money_flow: Dict) -> bool:
    return _money_flow_has_gap(money_flow) or _money_flow_aux_has_gap(money_flow)


def _money_flow_aux_flags(money_flow: Dict) -> List[str]:
    flags: List[str] = []
    if money_flow.get("ddx_5") is None:
        flags.append("MONEY_FLOW_DDX_MISSING")
    if money_flow.get("ddy_10") is None:
        flags.append("MONEY_FLOW_DDY_MISSING")
    return flags


def _money_flow_has_any_value(money_flow: Dict) -> bool:
    return any(
        money_flow.get(k) is not None
        for k in (
            "main_net_flow",
            "super_net_flow",
            "ddx_5",
            "ddy_10",
            "main_net_flow_5d",
            "main_net_flow_10d",
        )
    )


def _mx_money_flow_query_with_global_gate(tool, query: str):
    """
    mx-data 资金流全局慢速通道。

    同一台机器上多个工作流/定时任务可能同时访问 mx-data。仅靠进程内
    ThreadLock 不能避免跨进程叠加请求，所以这里用文件锁 + 共享时间戳
    把资金流查询串行化。
    """
    global _MX_MONEY_FLOW_LAST_QUERY_AT
    min_interval = _env_float("MX_MONEY_FLOW_QUERY_INTERVAL_SEC", 5.0, minimum=0.0)

    def _query_inside_lock():
        global _MX_MONEY_FLOW_LAST_QUERY_AT
        now_wall = time.time()
        state = _load_json_cache(_MX_MONEY_FLOW_STATE_PATH, {})
        try:
            last_wall = float(state.get("last_query_wall_at", 0.0) or 0.0)
        except Exception:
            last_wall = 0.0
        wait_wall = (last_wall + min_interval) - now_wall

        now_mono = time.monotonic()
        wait_mono = (_MX_MONEY_FLOW_LAST_QUERY_AT + min_interval) - now_mono
        wait = max(wait_wall, wait_mono, 0.0)
        if wait > 0:
            time.sleep(wait)

        result = tool.query(query)
        queried_at_wall = time.time()
        _MX_MONEY_FLOW_LAST_QUERY_AT = time.monotonic()
        _save_json_cache(_MX_MONEY_FLOW_STATE_PATH, {"last_query_wall_at": queried_at_wall})
        return result

    if fcntl is None:
        with _MX_MONEY_FLOW_LOCK:
            return _query_inside_lock()

    with open(_MX_MONEY_FLOW_GLOBAL_LOCK_PATH, "a+", encoding="utf-8") as lock_f:
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
        try:
            return _query_inside_lock()
        finally:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)


def _pool_main_flow_to_yi(candidate: Dict[str, Any]) -> Optional[float]:
    detail = candidate.get("pool_score_detail", {}) if isinstance(candidate, dict) else {}
    if not isinstance(detail, dict):
        return None
    raw = detail.get("main_flow_value")
    if raw is None and isinstance(detail.get("primary_detail"), dict):
        raw = detail["primary_detail"].get("main_flow_value")
    if raw is None:
        return None
    try:
        return round(float(raw) / 100000000.0, 4)
    except Exception:
        return None


def _uniq_keep_order(items: List[str]) -> List[str]:
    return [k for k in dict.fromkeys([x for x in items if x])]


def _retry_money_flow_gap_once(stock_code: str, base_flow: Optional[Dict[str, Any]] = None) -> tuple[Dict[str, Any], List[str]]:
    """
    当资金流核心字段缺失时做一次轻量补齐。

    如果主力净流入已有值，只缺超大单/DDX/DDY，则跳过 mx-data，
    直接走东财/AK 补字段，避免为一个缺口继续冲击 mx-data 触发 112。
    """
    delay = 0.35
    try:
        delay = float(os.getenv("MONEY_FLOW_RETRY_DELAY", "0.35"))
    except Exception:
        pass
    if delay > 0:
        time.sleep(min(delay, 2.0))

    used_sources: List[str] = []
    base_flow = base_flow if isinstance(base_flow, dict) else {}
    mx = {}
    if not base_flow or base_flow.get("main_net_flow") is None:
        try:
            mx = _fetch_money_flow_via_mx(stock_code) or {}
        except Exception:
            mx = {}
    if _money_flow_has_any_value(mx):
        used_sources.append("mx_retry")

    em = {}
    merged = _merge_money_flow(base_flow, mx)
    if _money_flow_has_gap(merged):
        em = _fetch_money_flow_via_eastmoney_direct(stock_code) or {}
    if _money_flow_has_any_value(em):
        used_sources.append("eastmoney_retry")

    ak = {}
    merged = _merge_money_flow(base_flow, mx, em)
    if _money_flow_has_gap(merged):
        ak = _fetch_money_flow_via_akshare(stock_code) or {}
    if _money_flow_has_any_value(ak):
        used_sources.append("ak_retry")

    rank = {}
    merged = _merge_money_flow(base_flow, mx, em, ak)
    if _money_flow_has_gap(merged):
        rank = _fetch_money_flow_via_akshare_rank(stock_code) or {}
    if _money_flow_has_any_value(rank):
        used_sources.append("ak_rank_retry")

    retried = _merge_money_flow(mx, em, ak, rank)
    return retried, used_sources


def _retry_main_net_flow_once(stock_code: str) -> tuple[Dict[str, Any], List[str]]:
    """Backward-compatible main-flow retry used by older regression checks.

    Keep this narrow: only main money-flow sources are queried, so a missing main
    value does not trigger unrelated full-field network fetches.
    """
    used_sources: List[str] = []
    mx = {}
    try:
        mx = _fetch_money_flow_via_mx(stock_code) or {}
    except Exception:
        mx = {}
    if _money_flow_has_any_value(mx):
        used_sources.append("mx_retry")
    if mx.get("main_net_flow") is not None:
        return mx, used_sources

    em = {}
    try:
        em = _fetch_money_flow_via_eastmoney_direct(stock_code) or {}
    except Exception:
        em = {}
    if _money_flow_has_any_value(em):
        used_sources.append("eastmoney_retry")
    if em.get("main_net_flow") is not None:
        return em, used_sources

    rank = {}
    try:
        rank = _fetch_money_flow_via_akshare_rank(stock_code) or {}
    except Exception:
        rank = {}
    if _money_flow_has_any_value(rank):
        used_sources.append("ak_rank_retry")
    return rank, used_sources


def _money_flow_cache_needs_refresh(item: Dict) -> bool:
    if not isinstance(item, dict):
        return True
    money_flow = item.get("money_flow", {}) if isinstance(item.get("money_flow", {}), dict) else {}
    if not money_flow:
        return True
    today = date.today().strftime("%Y%m%d")
    updated = str(item.get("updated", "") or "")
    if updated != today:
        return True
    if money_flow.get("main_net_flow") is None:
        return True
    if _money_flow_has_gap(money_flow):
        return True
    # Legacy same-day entries without an auxiliary fetch marker get one refresh.
    # Once mx-data has explicitly checked DDX/DDY, a genuine provider omission is
    # recorded as partial instead of refetching on every resume.
    return _money_flow_aux_has_gap(money_flow) and not bool(money_flow.get("aux_checked"))


def _cached_klines_valid(item: Dict, today: Optional[str] = None) -> bool:
    if not isinstance(item, dict):
        return False
    today = today or date.today().strftime("%Y%m%d")
    if str(item.get("updated", "") or "") != today:
        return False
    klines = item.get("klines", [])
    if not _kline_has_min_bars(klines):
        return False
    latest = _normalize_kline_date((klines[-1] or {}).get("date"), dashed=False)
    return latest == _previous_a_share_trading_day_compact()


def _kline_has_min_bars(klines: Any, minimum: int = KLINE_EXTERNAL_FETCH_MIN_BARS) -> bool:
    return isinstance(klines, list) and len([k for k in klines if isinstance(k, dict) and k]) >= minimum


def _normalize_kline_date(value: Any, *, dashed: bool = True) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    s = s.split(" ")[0].replace("/", "-")
    compact = s.replace("-", "")[:8]
    if len(compact) != 8 or not compact.isdigit():
        return s[:10]
    if dashed:
        return f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"
    return compact


def _is_a_share_trading_day_compact(compact: str) -> bool:
    try:
        shared_dir = Path(
            os.environ.get(
                "OPENCLAW_SHARED_DIR",
                Path.home() / ".openclaw" / "agents" / "shared",
            )
        )
        if shared_dir.exists() and str(shared_dir) not in sys.path:
            sys.path.insert(0, str(shared_dir))
        from trading_calendar import get_a_share_trading_day_status
        return bool(get_a_share_trading_day_status(compact).get("is_trading_day"))
    except Exception:
        try:
            return pd.Timestamp(compact).weekday() < 5
        except Exception:
            return True


def _previous_a_share_trading_day_compact(today: Optional[date] = None) -> str:
    cur = today or date.today()
    for i in range(1, 15):
        candidate = cur - timedelta(days=i)
        compact = candidate.strftime("%Y%m%d")
        if _is_a_share_trading_day_compact(compact):
            return compact
    return (cur - timedelta(days=1)).strftime("%Y%m%d")


def _load_xq_kline_manifest() -> Dict[str, Any]:
    return _load_json_cache(XQ_KLINE_MANIFEST_PATH, {})


def _xq_kline_manifest_trusted(expected_trading_day: str) -> bool:
    manifest = _load_xq_kline_manifest()
    if not isinstance(manifest, dict):
        return False
    if str(manifest.get("status") or "") != "ok":
        return False
    if _normalize_kline_date(manifest.get("trading_day"), dashed=False) != expected_trading_day:
        return False
    verification = manifest.get("verification", {})
    if isinstance(verification, dict) and verification.get("stale_stock_codes"):
        return False
    return True


def _fetch_kline_via_http(stock_code: str, days: int = 60) -> Optional[List[Dict]]:
    """
    通过 QMT HTTP API (qmt_http_server.py) 获取日K线数据（主方案）
    HTTP 服务地址由 QMT_HTTP_URL 配置，服务端读取本地 xtquant 数据。
    今日K线如果本地DAT缺失，用 /full_tick 实时接口补上。
    """
    try:
        import urllib.request, json
        from datetime import date

        full_code = _ensure_suffix(stock_code)

        # Step 1: 获取历史K线（本地DAT缓存，可能缺今日数据）
        url = f"{QMT_HTTP_URL}/market_data?stock={full_code}&period=1d&count={days}&fields=open,close,high,low,volume,amount"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with _retry_call(f"QMT HTTP K线 {stock_code}", lambda: urllib.request.urlopen(req, timeout=15)) as resp:
            raw = resp.read().decode("utf-8")

        data = json.loads(raw)
        if not data.get("success") or not data.get("data"):
            return None

        # HTTP server返回格式: {date: {stock: value}}，需处理嵌套
        close_data = data["data"].get("close", {})
        open_data = data["data"].get("open", {})
        high_data = data["data"].get("high", {})
        low_data = data["data"].get("low", {})
        volume_data = data["data"].get("volume", {})
        amount_data = data["data"].get("amount", {})

        def get_v(field_dict, dt, stock=full_code):
            """从 {date: {stock: value}} 嵌套格式取值"""
            inner = field_dict.get(dt, {})
            v = inner.get(stock) if isinstance(inner, dict) else inner
            return v

        all_dates = sorted(set().union(*[
            d.keys() for d in [close_data, open_data, high_data, low_data, volume_data, amount_data] if d
        ]))
        if not all_dates:
            return None

        records = []
        for dt in all_dates:
            close = get_v(close_data, dt)
            open_p = get_v(open_data, dt)
            high = get_v(high_data, dt)
            low = get_v(low_data, dt)
            vol = get_v(volume_data, dt)
            amount = get_v(amount_data, dt)
            if close is not None:
                date_str = _normalize_kline_date(dt)
                records.append({
                    "date": date_str,
                    "open": float(open_p) if open_p else 0,
                    "close": float(close),
                    "high": float(high) if high else 0,
                    "low": float(low) if low else 0,
                    "volume": float(vol) if vol else 0,
                    "amount": float(amount) if amount else 0,
                })

        if not records:
            return None

        # Step 2: 17:30 增量下载已确认上一交易日时，信任本地 QMT 数据。
        # 只有 QMT 返回不足 60 条且缺上一交易日，才允许外部源补齐。
        prev_compact = _previous_a_share_trading_day_compact()
        prev_str = _normalize_kline_date(prev_compact)
        has_prev = any(_normalize_kline_date(r.get("date"), dashed=False) == prev_compact for r in records)
        should_external_fill = (
            len(records) < KLINE_EXTERNAL_FETCH_MIN_BARS
            and not has_prev
            and not _xq_kline_manifest_trusted(prev_compact)
        )
        if should_external_fill:
            # 优先用 mx-data 补全（不依赖外网行情）
            try:
                from .data_fetcher import get_kline_via_mx_data
                prev_kline = get_kline_via_mx_data(stock_code, days=10)
                if prev_kline:
                    for bar in prev_kline:
                        if _normalize_kline_date(bar.get("date"), dashed=False) == prev_compact and bar["close"] > 0:
                            bar["date"] = prev_str
                            records.append(bar)
                            logger.info(f"K线补全(mx-data) {stock_code}: {prev_str} close={bar['close']}")
                            break
            except Exception as e1:
                logger.debug(f"mx-data补全跳过 {stock_code}: {e1}")

            # mx-data 失败才用 akshare
            if not any(_normalize_kline_date(r.get("date"), dashed=False) == prev_compact for r in records):
                try:
                    from .data_fetcher import get_kline_via_akshare
                    prev_kline = get_kline_via_akshare(stock_code, days=10)
                    if prev_kline:
                        for bar in prev_kline:
                            if _normalize_kline_date(bar.get("date"), dashed=False) == prev_compact and bar["close"] > 0:
                                bar["date"] = prev_str
                                records.append(bar)
                                logger.info(f"K线补全(akshare) {stock_code}: {prev_str} close={bar['close']}")
                                break
                except Exception as e2:
                    logger.warning(f"akshare补全失败 {stock_code}: {e2}")

        return sorted(records, key=lambda x: _normalize_kline_date(x.get("date"), dashed=False))[-days:]

    except Exception as e:
        logger.warning(f"HTTP K线获取失败 {stock_code}: {e}")
        return None


def get_kline_via_xqshare(stock_code: str, days: int = 120) -> Optional[List[Dict]]:
    """
    通过 xqshare 获取日K线数据（已弃用，保留备用）。
    xqshare 标准版 get_market_data_ex 返回空字典，K线以 HTTP API 为主。
    """
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent.parent / "knowledge-base" / "xqshare"))
        from xqshare import XtQuantRemote

        client = XtQuantRemote(
            host=os.environ.get("XQSHARE_HOST", "127.0.0.1"),
            port=int(os.environ.get("XQSHARE_PORT", "18812")),
            client_id=os.environ.get("XQSHARE_CLIENT_ID", "client-standard"),
            client_secret=os.environ.get("XQSHARE_CLIENT_SECRET", ""),
            auto_reconnect=True,
        )

        full_code = _ensure_suffix(stock_code)

        end_date = date.today().strftime("%Y%m%d")
        start_date = (date.today() - timedelta(days=days * 2)).strftime("%Y%m%d")

        result = client.xtdata.get_market_data_ex(
            stock_list=[full_code],
            period="daily",
            start_time=start_date,
            end_time=end_date,
        )

        client.close()

        if result and isinstance(result, dict) and full_code in result:
            df = result[full_code]
            if df is not None and len(df) > 0:
                records = []
                for _, row in df.iterrows():
                    records.append({
                        "date": str(row.get("time", "")),
                        "open": float(row.get("open", 0)),
                        "high": float(row.get("high", 0)),
                        "low": float(row.get("low", 0)),
                        "close": float(row.get("close", 0)),
                        "volume": float(row.get("volume", 0)),
                        "amount": float(row.get("amount", 0) or 0),
                    })
                return records[-days:]
        return None

    except Exception as e:
        logger.warning(f"xqshare K线获取失败 {stock_code}: {e}")
        return None


def get_kline_via_mx_data(stock_code: str, days: int = 120) -> Optional[List[Dict]]:
    """
    通过 mx-data (mx_data.py CLI) 获取日K线数据
    """
    try:
        full_code = _ensure_suffix(stock_code)
        cmd = [
            sys.executable,
            str(SKILLS_DIR / "mx-data" / "mx_data.py"),
            f"{full_code} 近{days}日收盘价 日K线",
        ]
        r = _retry_call(
            f"mx-data K线 {stock_code}",
            lambda: subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=45,
                env=domestic_subprocess_env(os.environ),
            ),
            retries=2,
            delay=3,
        )
        output = r.stdout

        if r.returncode != 0 or not output or "错误" in output or "上限" in output:
            return None

        # 解析 K线表格输出（格式：| 日期 | 开盘 | 收盘 | ...）
        import re
        clean = re.sub(r'([\d.]+)\s*(港元|元|美元|人民币|新元|台币)', r'\1', output)
        # 匹配 | 2026-04-01 | 10.5 | 10.8 | ... | 成交量 | 格式
        rows = re.findall(
            r'\|\s*(\d{4}-\d{2}-\d{2})\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|',
            clean
        )
        if rows:
            records = []
            for date_str, open_p, close_p, high_p, low_p in rows:
                records.append({
                    "date": date_str.strip(),
                    "open": float(open_p),
                    "close": float(close_p),
                    "high": float(high_p),
                    "low": float(low_p),
                    "volume": 0.0,  # mx-data 收盘价输出不含成交量
                })
            return records[-days:] if len(records) > days else records
        return None

    except Exception as e:
        logger.warning(f"mx-data K线获取失败 {stock_code}: {e}")
        return None


def get_kline_via_tencent(stock_code: str, days: int = 120) -> Optional[List[Dict]]:
    """备用：腾讯行情API获取K线"""
    try:
        import urllib.request
        prefix = "sh" if stock_code.startswith(('6', '5', '9')) else "sz"
        url = (f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
               f"?_var=kline_dayhfq&param={prefix}{stock_code},day,,,{days},qfq")
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://gu.qq.com"
        })
        with _retry_call(f"腾讯K线 {stock_code}", lambda: urllib.request.urlopen(req, timeout=10)) as resp:
            raw = resp.read().decode("utf-8")
        raw = raw[raw.index("=") + 1:]
        import json
        obj = json.loads(raw)
        key = f"{prefix}{stock_code}"
        data = obj.get("data", {}).get(key, {}).get("qfqday", [])
        if data:
            records = []
            for item in data[-days:]:
                if isinstance(item, list) and len(item) >= 5:
                    records.append({
                        "date": str(item[0]),
                        "open": float(item[1]),
                        "close": float(item[2]),
                        "high": float(item[3]),
                        "low": float(item[4]),
                        "volume": float(item[5]) if len(item) > 5 else 0,
                    })
            return records
    except Exception as e:
        logger.warning(f"腾讯K线获取失败 {stock_code}: {e}")
    return None


def get_kline_via_akshare(stock_code: str, days: int = 120) -> Optional[List[Dict]]:
    """
    通过 akshare 获取日K线数据（最终备用）
    注意：eastmoney 连接可能不稳定，优先用 get_kline_via_mx_data
    """
    try:
        import akshare as ak
        import pandas as pd

        today = date.today().strftime("%Y%m%d")
        start_dt = pd.Timestamp.today() - pd.Timedelta(days=days * 2)
        start = start_dt.strftime("%Y%m%d")

        df = _retry_call(
            f"akshare K线 {stock_code}",
            lambda: ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=start,
                end_date=today,
                adjust="qfq",
            ),
        )

        if df is not None and len(df) >= 2:
            result = []
            for _, row in df.tail(days).iterrows():
                result.append({
                    "date": str(row["日期"])[:10],
                    "open": float(row["开盘"]),
                    "high": float(row["最高"]),
                    "low": float(row["最低"]),
                    "close": float(row["收盘"]),
                    "volume": float(row["成交量"]),
                    "amount": float(row.get("成交额", 0) or 0),
                })
            return result
        return None

    except Exception as e:
        logger.warning(f"akshare K线获取失败 {stock_code}: {e}")
        return None


def _fetch_best_kline(stock_code: str, days: int = 120) -> tuple[List[Dict[str, Any]], str]:
    """Fetch one canonical K-line packet while retaining the best partial result."""
    best: List[Dict[str, Any]] = []
    best_source = "none"
    expected_day = _previous_a_share_trading_day_compact()

    def keep(source: str, candidate: Optional[List[Dict[str, Any]]]) -> bool:
        nonlocal best, best_source
        by_day: Dict[str, Dict[str, Any]] = {}
        for row in candidate or []:
            if not isinstance(row, dict) or not row.get("close"):
                continue
            day_key = _normalize_kline_date(row.get("date"), dashed=False)
            if not day_key or day_key > expected_day:
                continue
            normalized = dict(row)
            normalized["date"] = day_key
            by_day[day_key] = normalized
        clean = [by_day[day_key] for day_key in sorted(by_day)]
        if len(clean) > len(best):
            best = clean
            best_source = source
        latest = _normalize_kline_date((clean[-1] or {}).get("date"), dashed=False) if clean else ""
        return _kline_has_min_bars(clean) and latest == expected_day

    routes = (
        ("xqshare", lambda: get_kline_via_xqshare(stock_code, days=days)),
        ("qmt_http", lambda: _fetch_kline_via_http(stock_code, days=days)),
        ("mx-data", lambda: get_kline_via_mx_data(stock_code, days=days)),
        ("akshare", lambda: get_kline_via_akshare(stock_code, days=days)),
        ("tencent", lambda: get_kline_via_tencent(stock_code, days=days)),
    )
    for source, fetcher in routes:
        try:
            if keep(source, fetcher()):
                break
        except Exception as exc:
            logger.debug(f"K线筛选路由失败 {stock_code} {source}: {exc}")
    return best[-days:], best_source


def _extract_qmt_sector(value: Any) -> str:
    """Pick a readable industry name from the varying xtdata.get_industry payloads."""
    preferred: List[str] = []
    fallback: List[str] = []

    def walk(node: Any, key_hint: str = "") -> None:
        if isinstance(node, dict):
            for key, child in node.items():
                walk(child, str(key))
            return
        if isinstance(node, (list, tuple, set)):
            for child in node:
                walk(child, key_hint)
            return
        text = str(node or "").strip()
        if not text or text.lower() in {"none", "null", "nan"} or re.fullmatch(r"\d+", text):
            return
        if any(token in key_hint.lower() for token in ("industry", "name", "sector", "hy")):
            preferred.append(text)
        else:
            fallback.append(text)

    walk(value)
    candidates = preferred + fallback
    for item in candidates:
        clean = _clean_xq_sector_name(item)
        if clean and clean not in {"沪深A股", "A股", "股票"}:
            return clean[:80]
    return ""


def _fetch_qmt_screening_metadata(stock_code: str) -> Dict[str, Any]:
    full_code = _ensure_suffix(stock_code)
    instrument_response = _qmt_get_json("/instrument_detail", {"stock": full_code}, timeout=6) or {}
    instrument = instrument_response.get("data") if instrument_response.get("success") else {}
    instrument = instrument if isinstance(instrument, dict) else {}
    sector, sector_as_of = _get_fresh_cached_sector(stock_code, max_age_days=30)
    sector_source = "sector_cache" if sector else ""
    if not sector:
        sector = _fetch_sector_via_xqshare(stock_code)
        if sector:
            sector_source = "xqshare_sector_map"
            _set_cached_sector(stock_code, sector, source=sector_source, as_of=date.today().strftime("%Y%m%d"))
            sector_as_of = date.today().strftime("%Y%m%d")
    return {
        "source": "qmt_http",
        "checked_at": date.today().strftime("%Y%m%d"),
        "instrument": instrument,
        "listing_date": _normalize_kline_date(instrument.get("OpenDate"), dashed=False),
        "expire_date": _normalize_kline_date(instrument.get("ExpireDate"), dashed=False),
        "instrument_status": instrument.get("InstrumentStatus"),
        "is_trading": instrument.get("IsTrading"),
        "sector": sector,
        "sector_source": sector_source or "none",
        "sector_as_of": sector_as_of,
    }


def build_debate_packet(
    stock_code: str,
    stock_name: str,
    phase1_cache: Dict,
    kline_data: List[Dict],
) -> Dict:
    """
    构建单只股票的辩论数据包

    Args:
        stock_code: 股票代码
        stock_name: 股票名称
        phase1_cache: Phase 1 缓存的财务数据 (from fundamental_cache/all_stocks_financial.json)
        kline_data: K线数据列表

    Returns:
        StockDebatePacket dict
    """
    # K线：优先读缓存 → 缓存无则用 workflow 传入的 kline_data → 都无则网络五级获取
    cache = _load_debate_data_cache()
    cache_key = str(stock_code).zfill(6)
    cached_item = cache.get(cache_key, {}) if isinstance(cache.get(cache_key, {}), dict) else {}
    cached_klines = cached_item.get("klines", []) if _cached_klines_valid(cached_item) else []
    kline_source = "none"
    if cached_klines:
        kline_data = cached_klines
        cached_source = str((((cached_item.get("data_contract") or {}).get("kline") or {}).get("source") or ""))
        kline_source = f"debate_data_cache:{cached_source}" if cached_source and "debate_data_cache" not in cached_source else "debate_data_cache"
        logger.info(f"  {stock_code} K线(缓存) {len(kline_data)} 条")
    elif _kline_has_min_bars(kline_data):
        # workflow 已通过 QMT HTTP 获取，直接用
        kline_source = "workflow"
        logger.info(f"  {stock_code} K线(workflow) {len(kline_data)} 条")
    else:
        # 缓存无且 workflow 无有效数据，五级串行网络获取
        best_kline = kline_data if isinstance(kline_data, list) else []
        best_source = "workflow" if best_kline else "none"

        def keep_candidate(source: str, candidate: Optional[List[Dict]]) -> bool:
            nonlocal best_kline, best_source
            if not candidate:
                return False
            if len(candidate) > len(best_kline):
                best_kline = candidate
                best_source = source
            return _kline_has_min_bars(candidate)

        if keep_candidate("xqshare", get_kline_via_xqshare(stock_code, days=120)):
            pass
        elif keep_candidate("qmt_http", _fetch_kline_via_http(stock_code, days=120)):
            pass
        elif keep_candidate("mx-data", get_kline_via_mx_data(stock_code, days=120)):
            pass
        elif keep_candidate("akshare", get_kline_via_akshare(stock_code, days=120)):
            pass
        elif keep_candidate("tencent", get_kline_via_tencent(stock_code, days=120)):
            pass

        kline_data = best_kline
        kline_source = best_source
        if kline_data:
            logger.info(f"  {stock_code} K线(网络) {len(kline_data)} 条")
        else:
            logger.warning(f"  {stock_code} K线五级获取均失败")

    data_quality_flags = []
    if not kline_data:
        data_quality_flags.append("KLINE_MISSING")
    elif len(kline_data) < KLINE_EXTERNAL_FETCH_MIN_BARS:
        data_quality_flags.append("KLINE_SHORT")
    else:
        try:
            key = str(stock_code).zfill(6)
            item = cache.get(key, {}) if isinstance(cache.get(key, {}), dict) else {}
            item["klines"] = kline_data[-120:]
            item["updated"] = date.today().strftime("%Y%m%d")
            cache[key] = item
            _save_debate_data_cache(cache)
        except Exception as e:
            logger.debug(f"  {stock_code} K线缓存写入失败: {e}")

    # 财务数据默认严格缓存优先；仅缓存无有效指标或显式要求刷新时才联网补取。
    cache_code = str(stock_code).strip().split(".", 1)[0].zfill(6)
    cache_fin = normalize_financial_record(
        (phase1_cache or {}).get(cache_code)
        or (phase1_cache or {}).get(stock_code)
        or {}
    )
    cache_fin_available = _financial_record_has_any_value(cache_fin)
    live_refresh = _env_bool("FINANCIAL_LIVE_REFRESH_IN_PACKET", "0")
    xq_fin = None
    if live_refresh or not cache_fin_available:
        xq_fin = normalize_financial_record(_fetch_financial_via_xqshare(stock_code))

    # 在线刷新只覆盖非空字段，缓存继续为其余字段补缺。
    fin = dict(cache_fin)
    if xq_fin:
        for k, v in xq_fin.items():
            if v is not None and v != "":
                fin[k] = v

    financial_source = "none"
    if xq_fin and cache_fin_available:
        financial_source = "xqshare+phase1_cache"
    elif xq_fin:
        financial_source = "xqshare"
    elif cache_fin_available:
        financial_source = str(fin.get("_source") or "phase1_cache")
    if not xq_fin and cache_fin_available:
        logger.info(f"  {stock_code} 使用 phase1_cache 财务数据")
    elif xq_fin:
        logger.info(f"  {stock_code} 使用 xqshare 财务补取/刷新数据")
    if not _financial_record_has_any_value(fin):
        data_quality_flags.append("FINANCIAL_MISSING")
    financial_as_of = _normalize_kline_date(
        fin.get("_as_of") or fin.get("report_date") or fin.get("end_date") or fin.get("m_timetag"),
        dashed=False,
    )

    # 板块数据：优先使用当天 XQShare 全市场映射；持久缓存仅在30天内有效。
    # 旧缓存不能遮住新鲜映射，也不能在未核验时被重写成今天日期。
    sector_source = "none"
    sector_as_of = ""
    sector = _fetch_sector_via_xqshare(stock_code)
    if sector:
        sector_source = "xqshare"
        sector_as_of = date.today().strftime("%Y%m%d")
        logger.info(f"  {stock_code} 板块(XQShare): {sector}")
    if not sector:
        sector = fin.get("sector", "")
        if sector:
            sector_source = financial_source
            sector_as_of = financial_as_of
    if not sector:
        sector, cached_sector_as_of = _get_fresh_cached_sector(stock_code)
        if sector:
            sector_source = "sector_cache"
            sector_as_of = cached_sector_as_of
        elif _get_cached_sector(stock_code):
            logger.info(
                f"  {stock_code} 忽略过期板块缓存 as_of={_get_cached_sector_as_of(stock_code)}"
            )
    if not sector:
        sector = _fetch_sector_via_mx(stock_code)
        if sector:
            sector_source = "mx-data"
            sector_as_of = date.today().strftime("%Y%m%d")
            logger.info(f"  {stock_code} 板块: {sector}")
    if not sector:
        sector = _fetch_sector_via_akshare(stock_code)
        if sector:
            sector_source = "akshare"
            sector_as_of = date.today().strftime("%Y%m%d")
            logger.info(f"  {stock_code} 板块(akshare): {sector}")
    if sector and sector_source != "sector_cache":
        _set_cached_sector(stock_code, sector, source=sector_source, as_of=sector_as_of)
    if not sector:
        data_quality_flags.append("SECTOR_MISSING")

    # 从K线计算技术摘要
    kline_summary = summarize_kline(kline_data)

    # 识别RSI/MACD（简单计算）
    indicators = compute_indicators(kline_data)

    # 资金流数据：QMT主源 + MX + AK + Rank 字段级补全；缓存仅作兜底
    cache = _load_debate_data_cache()
    cached = cache.get(stock_code, {})
    cached_money_flow = _get_cached_money_flow(stock_code, allow_partial=True, allow_stale=True)
    cached_money_flow_today = _get_cached_money_flow(stock_code, allow_partial=True, allow_stale=False)
    live_money_flow_in_packet = _env_bool("MONEY_FLOW_LIVE_FETCH_IN_PACKET", "0")
    # 构包阶段默认只消费预获取/缓存结果，避免资金流源抖动阻塞 Phase 2 排名和 top_picks。
    # 如需现场补齐所有资金流字段，可显式设置 MONEY_FLOW_LIVE_FETCH_IN_PACKET=1。
    if (
        cached_money_flow_today
        and (
            (
                live_money_flow_in_packet
                and cached_money_flow_today.get("main_net_flow") is not None
                and not _money_flow_has_gap(cached_money_flow_today)
            )
            or (
                not live_money_flow_in_packet
                and _money_flow_has_any_value(cached_money_flow_today)
            )
        )
    ):
        money_flow = dict(cached_money_flow_today)
        money_flow["source"] = str(money_flow.get("source") or "cache_hot")
        logger.info(
            f"  {stock_code} 资金流(当日缓存): 主力{money_flow.get('main_net_flow')}亿 "
            f"超大单{money_flow.get('super_net_flow')}亿 "
            f"DDX5={money_flow.get('ddx_5')} DDY10={money_flow.get('ddy_10')} "
            f"来源={money_flow.get('source')}"
        )
    elif not live_money_flow_in_packet:
        if _money_flow_has_any_value(cached_money_flow):
            money_flow = dict(cached_money_flow)
            money_flow["source"] = str(money_flow.get("source") or "cache_stale")
        else:
            money_flow = _empty_money_flow("none")
        money_flow_fetch_failed = False
    else:
        # ★ QMT HTTP 资金流已移除（6-04 实测不可用）—— 从 mx 开始
        money_flow_qmt = {}  # 占位仅供后续 base 变量使用
        used_sources: List[str] = []
        money_flow_mx = {}
        try:
            money_flow_mx = _fetch_money_flow_via_mx(stock_code) or {}
        except Exception:
            pass
        if _money_flow_has_any_value(money_flow_mx):
            used_sources.append("mx")
        money_flow_eastmoney = {}
        base = _merge_money_flow(money_flow_qmt, money_flow_mx)
        if not base or _money_flow_has_gap(base):
            try:
                money_flow_eastmoney = _fetch_money_flow_via_eastmoney_direct(stock_code) or {}
            except Exception:
                pass
        if _money_flow_has_any_value(money_flow_eastmoney):
            used_sources.append("eastmoney")

        money_flow_ak = {}
        base = _merge_money_flow(money_flow_qmt, money_flow_mx, money_flow_eastmoney)
        if not base or _money_flow_has_gap(base):
            try:
                money_flow_ak = _fetch_money_flow_via_akshare(stock_code) or {}
            except Exception:
                pass
        if _money_flow_has_any_value(money_flow_ak):
            used_sources.append("ak")
        money_flow_rank = {}
        merged = _merge_money_flow(money_flow_qmt, money_flow_mx, money_flow_eastmoney, money_flow_ak)
        if not merged or _money_flow_has_gap(merged):
            money_flow_rank = _fetch_money_flow_via_akshare_rank(stock_code) or {}
        if _money_flow_has_any_value(money_flow_rank):
            used_sources.append("ak_rank")
        if _money_flow_has_any_value(cached_money_flow):
            used_sources.append("cache")
        money_flow = _merge_money_flow(
            money_flow_qmt, money_flow_mx, money_flow_eastmoney, money_flow_ak, money_flow_rank, cached_money_flow
        )
        live_money_flow = _merge_money_flow(
            money_flow_qmt, money_flow_mx, money_flow_eastmoney, money_flow_ak, money_flow_rank
        )
        if _money_flow_has_gap(money_flow):
            retry_flow, retry_sources = _retry_money_flow_gap_once(stock_code, money_flow)
            if _money_flow_has_any_value(retry_flow):
                used_sources.extend(retry_sources)
                live_money_flow = _merge_money_flow(retry_flow, live_money_flow)
                money_flow = _merge_money_flow(retry_flow, money_flow)
        used_sources = _uniq_keep_order(used_sources)
        money_flow_source = "+".join(used_sources) if used_sources else "none"
        if money_flow:
            money_flow["source"] = money_flow_source
        if money_flow:
            logger.info(
                f"  {stock_code} 资金流: 主力{money_flow.get('main_net_flow')}亿 "
                f"超大单{money_flow.get('super_net_flow')}亿 "
                f"DDX5={money_flow.get('ddx_5')} DDY10={money_flow.get('ddy_10')} "
                f"来源={money_flow_source}"
            )
        money_flow_fetch_failed = (
            (not _money_flow_has_any_value(live_money_flow)) and (not _money_flow_has_any_value(cached_money_flow))
        )
    if "money_flow_fetch_failed" not in locals():
        money_flow_fetch_failed = False
    for flag in money_flow.get("quality_flags") or []:
        if flag not in data_quality_flags:
            data_quality_flags.append(flag)
    if not _money_flow_has_any_value(money_flow):
        data_quality_flags.append("MONEY_FLOW_MISSING")
        if money_flow_fetch_failed:
            data_quality_flags.append("MONEY_FLOW_FETCH_FAILED")
    elif money_flow.get("main_net_flow") is None:
        data_quality_flags.append("MONEY_FLOW_MISSING")
    elif money_flow.get("super_net_flow") is None:
        data_quality_flags.append("MONEY_FLOW_PARTIAL")
    for flag in _money_flow_aux_flags(money_flow):
        if flag not in data_quality_flags:
            data_quality_flags.append(flag)
    if live_money_flow_in_packet:
        _set_debate_cached_money_flow(stock_code, money_flow)

    # 技术信号检测（蜡烛图、量价背离、海龟、波浪、江恩）
    technical_signals = _detect_technical_signals(kline_data)

    # 个股新闻（mx-search + akshare 双源合并去重）
    # 新闻：优先读缓存，缓存无则 mx-search → akshare 串行获取
    cached = cache.get(stock_code, {})
    news_items = []
    news_fetch_attempted = False
    fetched_news_sources: List[str] = []
    if cached.get("news"):
        news_items = cached["news"]
        logger.info(f"  {stock_code} 新闻(缓存) {len(news_items)} 条")
    else:
        news_fetch_attempted = True
        seen_titles = set()
        mx_news = _fetch_news_via_mxsearch(stock_name, max_results=5)
        if os.environ.get("MX_APIKEY"):
            fetched_news_sources.append("mx-search")
        for n in mx_news:
            title = n.get("title", "")
            if title and title not in seen_titles:
                seen_titles.add(title)
                news_items.append(n)
        logger.info(f"  {stock_code} mx-search 新闻 {len(mx_news)} 条（合并前）")
        try:
            import akshare as ak
            fetched_news_sources.append("akshare")
            df = ak.stock_news_em(symbol=stock_code)
            if df is not None and len(df) > 0:
                for _, row in df.iterrows():
                    title = str(row.get("新闻标题", "")).strip()[:80]
                    content = str(row.get("新闻内容", "") or "").strip()
                    if len(content) > 20 and title and title not in seen_titles and len(news_items) < 10:
                        seen_titles.add(title)
                        news_items.append({"title": title, "content": content[:200],
                                     "source": str(row.get("文章来源", "") or "").strip(),
                                     "time": str(row.get("发布时间", "") or "").strip()[:16]})
            logger.info(f"  {stock_code} 双源新闻合计 {len(news_items)} 条（去重后）")
        except Exception as e:
            logger.warning(f"  {stock_code} akshare 新闻获取失败: {e}")

    cached_contract = cached.get("data_contract", {}) if isinstance(cached.get("data_contract", {}), dict) else {}
    cached_news_contract = cached_contract.get("news", {}) if isinstance(cached_contract.get("news", {}), dict) else {}
    news_content_as_of = max(
        (_normalize_kline_date(x.get("time"), dashed=False) for x in news_items if isinstance(x, dict)),
        default="",
    )
    news_checked_at = (
        date.today().strftime("%Y%m%d")
        if news_fetch_attempted
        else str(cached_news_contract.get("checked_at") or cached.get("updated") or "")
    )
    news_source = (
        ("+".join(_uniq_keep_order(fetched_news_sources)) or "none")
        if news_fetch_attempted
        else str(cached_news_contract.get("source") or "debate_data_cache")
    )

    data_contract = {
        "kline": _data_result(
            source=kline_source,
            status=_contract_status(
                bool(kline_data),
                bool(kline_data) and len(kline_data) < KLINE_EXTERNAL_FETCH_MIN_BARS,
            ),
            quality_flags=[f for f in data_quality_flags if f.startswith("KLINE_")],
            as_of=_normalize_kline_date((kline_data[-1] or {}).get("date"), dashed=False) if kline_data else "",
        ),
        "money_flow": _data_result(
            source=str(money_flow.get("source") or "none"),
            status=_contract_status(
                _money_flow_has_any_value(money_flow),
                _money_flow_has_any_value(money_flow) and _money_flow_is_partial(money_flow),
            ),
            error="fetch_failed" if money_flow_fetch_failed else "",
            quality_flags=[f for f in data_quality_flags if f.startswith("MONEY_FLOW_")],
            as_of=str(money_flow.get("as_of") or ""),
        ),
        "financial": _data_result(
            source=financial_source,
            status=_contract_status(_financial_record_has_any_value(fin)),
            quality_flags=[f for f in data_quality_flags if f == "FINANCIAL_MISSING"],
            as_of=financial_as_of,
        ),
        "sector": _data_result(
            source=sector_source,
            status=_contract_status(bool(sector)),
            quality_flags=[f for f in data_quality_flags if f == "SECTOR_MISSING"],
            as_of=sector_as_of,
        ),
        "news": _data_result(
            source=news_source,
            status=_contract_status(bool(news_items)),
            quality_flags=list(cached_news_contract.get("quality_flags") or []),
            as_of=news_checked_at or news_content_as_of,
        ),
    }
    data_contract["news"].update({
        "checked_at": news_checked_at,
        "content_as_of": news_content_as_of,
    })
    money_fields = (
        "main_net_flow", "super_net_flow", "ddx_5", "ddy_10",
        "main_net_flow_5d", "main_net_flow_10d",
    )
    data_contract["money_flow"]["field_status"] = {
        field: ("ok" if money_flow.get(field) is not None else "missing")
        for field in money_fields
    }
    data_contract["money_flow"]["field_sources"] = dict(money_flow.get("field_sources") or {})
    data_contract["money_flow"]["field_as_of"] = dict(money_flow.get("field_as_of") or {})
    data_contract["money_flow"]["units"] = dict(money_flow.get("units") or {})
    data_contract["money_flow"]["diagnostics"] = dict(money_flow.get("diagnostics") or {})

    packet = {
        "stock_code": stock_code,
        "name": stock_name,
        "sector": sector,
        "data_quality_flags": data_quality_flags,
        "data_contract": data_contract,
        "news": news_items[:10],  # 最新10条新闻（SentimentGrounding 用）
        # 财务数据（fin 由 xqshare + phase1_cache 合并而来，见 1498-1511）
        "financial": {
            # fin 实际有的 8 个字段
            "roe": fin.get("roe_annual_latest"),
            "roe_quarter": fin.get("roe_quarter_latest"),
            "revenue_growth": fin.get("revenue_growth_yoy"),
            "net_profit_growth": fin.get("net_profit_growth_yoy"),
            "gross_margin": fin.get("gross_margin"),
            "pe_ttm": fin.get("pe_ttm"),
            "pb": fin.get("pb"),
            "debt_ratio": fin.get("debt_asset_ratio"),
            # fin 暂未提供的字段（保留原 key 以兼容下游消费者）
            "market_cap": fin.get("total_market_cap"),
            "cash_flow": fin.get("operating_cash_flow"),
            "book_value_per_share": fin.get("book_value_per_share"),
            # ★ 新增：回传真实数据来源 + 原始 fin 字典，供 callers 回写到 candidates
            "_fin_raw": dict(fin),
            "_fin_source": financial_source,
        },
        # 资金流数据
        "money_flow": {
            "main_net_flow": money_flow.get("main_net_flow"),   # 亿元（正=净流入）
            "super_net_flow": money_flow.get("super_net_flow"),   # 亿元
            "ddx_5": money_flow.get("ddx_5"),                    # 5日DDX（主力关注度）
            "ddy_10": money_flow.get("ddy_10"),                 # 10日DDY（趋势强度）
            "main_net_flow_5d": money_flow.get("main_net_flow_5d"),
            "main_net_flow_10d": money_flow.get("main_net_flow_10d"),
            "source": money_flow.get("source", "none"),         # 数据来源轨迹
            "as_of": money_flow.get("as_of", ""),
            "field_sources": money_flow.get("field_sources", {}),
            "field_as_of": money_flow.get("field_as_of", {}),
            "units": money_flow.get("units", {}),
        },
        # K线摘要
        "kline_summary": kline_summary,
        # 技术指标
        "indicators": indicators,
        # 原始K线（供详细分析）
        "kline_raw": kline_data[-120:] if kline_data else [{}],  # 最近120天，空时用{}避免LLM拒绝
        # ── 新增技术信号（蜡烛图/量价背离/海龟/波浪/江恩） ──
        "candlestick_patterns": technical_signals.get("candlestick_patterns", {}),
        "volume_price_divergence": technical_signals.get("volume_price_divergence", {}),
        "turtle_signals": technical_signals.get("turtle_signals", {}),
        "elliott_wave": technical_signals.get("elliott_wave", {}),
        "gann_levels": technical_signals.get("gann_levels", {}),
    }
    try:
        from .data_router import attach_data_router_metadata
    except Exception:
        try:
            from stock_selection_debate.data_router import attach_data_router_metadata
        except Exception:
            attach_data_router_metadata = None
    if attach_data_router_metadata is not None:
        try:
            packet = attach_data_router_metadata(packet)
        except Exception as exc:
            logger.warning(f"  {stock_code} 数据路由合同归一化失败: {exc}")
    try:
        from .market_snapshot import attach_verified_market_snapshot
    except Exception:
        try:
            from stock_selection_debate.market_snapshot import attach_verified_market_snapshot
        except Exception:
            attach_verified_market_snapshot = None
    if attach_verified_market_snapshot is not None:
        try:
            packet = attach_verified_market_snapshot(packet)
        except Exception as exc:
            logger.warning(f"  {stock_code} 行情事实快照生成失败: {exc}")
    return packet



def summarize_kline(kline: List[Dict]) -> Dict:
    """从K线数据生成摘要"""
    if not kline or len(kline) < 5:
        return {}

    closes = [b["close"] for b in kline if "close" in b]
    highs = [b["high"] for b in kline if "high" in b]
    lows = [b["low"] for b in kline if "low" in b]
    volumes = [b["volume"] for b in kline if "volume" in b]

    latest = closes[-1] if closes else 0
    ma5 = sum(closes[-5:]) / min(5, len(closes)) if closes else 0
    ma20 = sum(closes[-20:]) / min(20, len(closes)) if closes else 0
    ma60 = sum(closes[-60:]) / min(60, len(closes)) if closes else 0

    # 均线系统判断
    if ma5 > ma20 > ma60:
        ma_system = "多头排列"
    elif ma5 < ma20 < ma60:
        ma_system = "空头排列"
    else:
        ma_system = "混乱"

    # 趋势判断（简单：最近20日涨幅）
    trend_pct = (closes[-1] / closes[-min(20, len(closes))] - 1) * 100 if len(closes) >= 20 else 0

    # 成交量判断（最近5日均量 vs 20日均量）
    vol_5avg = sum(volumes[-5:]) / min(5, len(volumes)) if volumes else 0
    vol_20avg = sum(volumes[-20:]) / min(20, len(volumes)) if volumes else 0
    vol_signal = "放量" if vol_5avg > vol_20avg * 1.2 else "缩量" if vol_5avg < vol_20avg * 0.8 else "正常"

    # ── 短期动量 ──────────────────────────────
    trend_pct_5d = (closes[-1] / closes[-min(5, len(closes))] - 1) * 100 if len(closes) >= 5 else 0
    trend_pct_10d = (closes[-1] / closes[-min(10, len(closes))] - 1) * 100 if len(closes) >= 10 else 0

    # ── 量能趋势（近5日逐日）─────────────────────
    vol_last5 = volumes[-5:] if len(volumes) >= 5 else volumes
    vol_prev5 = volumes[-10:-5] if len(volumes) >= 10 else volumes[:5]
    vol_now_avg = sum(vol_last5) / len(vol_last5) if vol_last5 else 0
    vol_prev_avg = sum(vol_prev5) / len(vol_prev5) if vol_prev5 else 0
    if vol_now_avg > vol_prev_avg * 1.15:
        vol_trend = "逐日递增"
    elif vol_now_avg < vol_prev_avg * 0.85:
        vol_trend = "逐日递减"
    else:
        vol_trend = "平稳"

    return {
        "latest_close": round(latest, 2),
        "ma5": round(ma5, 2),
        "ma20": round(ma20, 2),
        "ma60": round(ma60, 2),
        "ma_system": ma_system,
        "trend_pct_5d": round(trend_pct_5d, 2),    # 5日涨幅（短线动量）
        "trend_pct_10d": round(trend_pct_10d, 2),  # 10日涨幅（中期动量）
        "trend_pct_20d": round(trend_pct, 2),
        "vol_5avg_vs_20avg": round(vol_5avg / vol_20avg, 2) if vol_20avg else 0,  # 量比
        "vol_signal": vol_signal,
        "vol_trend": vol_trend,                     # 量能趋势（逐增/逐减/平稳）
        "high_20d": round(max(highs[-20:]) if highs else 0, 2),
        "low_20d": round(min(lows[-20:]) if lows else 0, 2),
        "close_position_20d": round((latest - min(lows[-20:])) / (max(highs[-20:]) - min(lows[-20:]) + 0.001) * 100, 1) if highs and lows else 50,
    }


def compute_indicators(kline: List[Dict]) -> Dict:
    """计算 RSI(14) 与统一口径 MACD。"""
    if not kline or len(kline) < 20:
        return {}

    closes = [b["close"] for b in kline if "close" in b]

    # RSI(14)
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0 for d in deltas[-14:]]
    losses = [-d if d < 0 else 0 for d in deltas[-14:]]
    avg_gain = sum(gains) / 14 if gains else 0
    avg_loss = sum(losses) / 14 if losses else 0
    rs = avg_gain / avg_loss if avg_loss > 0 else 999
    rsi_14 = round(100 - 100 / (1 + rs), 1) if avg_loss > 0 else 100

    macd_result = compute_macd(closes)

    return {
        "rsi_14": rsi_14,
        "rsi_position": _rsi_position(kline, 20),   # RSI在近20日的位置(0-100)
        "macd": macd_result.get("dif"),
        "macd_dea": macd_result.get("dea"),
        "macd_signal": legacy_macd_signal(macd_result),
        "macd_state": macd_result.get("state"),
        "macd_cross_event": macd_result.get("cross_event"),
        "macd_hist": macd_result.get("hist"),
        "macd_breadth": macd_result.get("hist_slope"),
        "volume_momentum": _volume_momentum(kline),      # 量能变化方向
    }


def _rsi_position(kline: List[Dict], period: int = 20) -> float:
    """RSI在近N日的位置（0=最低，100=最高）"""
    closes = [b["close"] for b in kline if "close" in b]
    if len(closes) < period:
        return 50.0
    recent = closes[-period:]
    current = closes[-1]
    minimum = min(recent)
    maximum = max(recent)
    if maximum == minimum:
        return 50.0
    return round((current - minimum) / (maximum - minimum) * 100, 1)


def _macd_breadth(kline: List[Dict]) -> str:
    """MACD柱是否在扩张（扩张=动能增强，收缩=动能减弱）"""
    closes = [b["close"] for b in kline if "close" in b]
    if len(closes) < 20:
        return "无法判断"
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    macd_now = ema12 - ema26
    ema12_prev = _ema(closes[:-5], 12)
    ema26_prev = _ema(closes[:-5], 26)
    macd_prev = ema12_prev - ema26_prev
    if abs(macd_now) > abs(macd_prev) * 1.1:
        return "扩张"
    elif abs(macd_now) < abs(macd_prev) * 0.9:
        return "收缩"
    return "平稳"


def _volume_momentum(kline: List[Dict]) -> str:
    """近5日量能相对前5日：放大/萎缩/平稳"""
    volumes = [b["volume"] for b in kline if "volume" in b]
    if len(volumes) < 10:
        return "无法判断"
    vol_last5 = volumes[-5:]
    vol_prev5 = volumes[-10:-5]
    avg_now = sum(vol_last5) / 5
    avg_prev = sum(vol_prev5) / 5
    if avg_now > avg_prev * 1.15:
        return "放大"
    elif avg_now < avg_prev * 0.85:
        return "萎缩"
    return "平稳"


def _ema(data: List[float], period: int) -> float:
    """计算指数移动平均"""
    if len(data) < period:
        return sum(data) / len(data) if data else 0
    k = 2 / (period + 1)
    ema = sum(data[:period]) / period
    for price in data[period:]:
        ema = price * k + ema * (1 - k)
    return ema


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
    amount_data = data.get("amount", {})
    all_dates = sorted(set().union(*[
        d.keys() for d in [close_data, open_data, high_data, low_data, volume_data, amount_data] if d
    ]))
    records = []
    for dt in all_dates:
        def _extract(d, date_str):
            inner = d.get(date_str, {})
            if inner and isinstance(inner, dict):
                vals = list(inner.values())
                return vals[0] if vals else 0.0
            return 0.0
        records.append({
            "date": dt,
            "open": _extract(open_data, dt),
            "high": _extract(high_data, dt),
            "low": _extract(low_data, dt),
            "close": _extract(close_data, dt),
            "volume": _extract(volume_data, dt),
            "amount": _extract(amount_data, dt),
        })
    return records


_DATA_CACHE_DIR = CACHE_DIR
DEBATE_CACHE_SCHEMA_VERSION = "2026-07-10.field-provenance-v2"



def _debate_data_cache_file():
    _DATA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return _DATA_CACHE_DIR / "debate_data_cache.json"


def _upgrade_legacy_debate_cache(data: Dict[str, Dict]) -> tuple[Dict[str, Dict], bool]:
    changed = False
    for item in data.values():
        if not isinstance(item, dict) or item.get("cache_schema_version") == DEBATE_CACHE_SCHEMA_VERSION:
            continue
        klines = item.get("klines") if isinstance(item.get("klines"), list) else []
        last_kline_day = _normalize_kline_date((klines[-1] or {}).get("date"), dashed=False) if klines else ""
        old_flow = item.get("money_flow") if isinstance(item.get("money_flow"), dict) else {}
        if _money_flow_has_any_value(old_flow) and not str(old_flow.get("source") or "").strip():
            item["money_flow"] = _empty_money_flow("legacy_cache_invalidated")
            item["money_flow"]["source"] = "legacy_cache_invalidated"
        flow = item.get("money_flow") if isinstance(item.get("money_flow"), dict) else _empty_money_flow()
        news = item.get("news") if isinstance(item.get("news"), list) else []
        news_as_of = max(
            (_normalize_kline_date(x.get("time"), dashed=False) for x in news if isinstance(x, dict)),
            default="",
        )
        money_ok = _money_flow_has_any_value(flow)
        item["data_contract"] = {
            "money_flow": _data_result(
                source=str(flow.get("source") or "none"),
                status="partial" if money_ok else "missing",
                error="legacy_cache_missing_provenance" if not money_ok else "",
                quality_flags=[] if money_ok else ["MONEY_FLOW_PROVENANCE_UNKNOWN"],
                as_of=str(flow.get("as_of") or ""),
            ),
            "kline": _data_result(
                source="debate_data_cache:legacy",
                status=_contract_status(bool(klines), bool(klines) and not _kline_has_min_bars(klines)),
                quality_flags=[] if _kline_has_min_bars(klines) else (["KLINE_SHORT"] if klines else ["KLINE_MISSING"]),
                as_of=last_kline_day,
            ),
            "news": _data_result(
                source="debate_data_cache:legacy" if news else "none",
                status=_contract_status(bool(news)),
                as_of=news_as_of,
            ),
        }
        item["data_contract"]["money_flow"].update({
            "field_status": {
                field: ("ok" if flow.get(field) is not None else "missing")
                for field in (
                    "main_net_flow", "super_net_flow", "ddx_5", "ddy_10",
                    "main_net_flow_5d", "main_net_flow_10d",
                )
            },
            "field_sources": dict(flow.get("field_sources") or {}),
            "field_as_of": dict(flow.get("field_as_of") or {}),
            "units": dict(flow.get("units") or {}),
        })
        item["cache_schema_version"] = DEBATE_CACHE_SCHEMA_VERSION
        changed = True
    return data, changed



def _load_debate_data_cache() -> Dict[str, Dict]:
    """加载辩论数据包预获取缓存（资金流+新闻+完整日K线）"""
    f = _debate_data_cache_file()
    if not f.exists():
        return {}
    try:
        with open(f) as fp:
            data = json.load(fp)
        if not isinstance(data, dict):
            return {}
        data, changed = _upgrade_legacy_debate_cache(data)
        if changed:
            _save_json_cache(f, data)
            logger.warning("已迁移旧辩论缓存：保留K线/新闻，失去来源的旧资金流已失效并等待重新预取")
        return data
    except Exception:
        return {}


def get_cached_debate_klines(stock_code: str) -> List[Dict[str, Any]]:
    """Return today's canonical Phase1 K-line packet for downstream phases."""
    key = str(stock_code).zfill(6)
    item = _load_debate_data_cache().get(key, {})
    if not _cached_klines_valid(item):
        return []
    return list(item.get("klines") or [])



def _save_debate_data_cache(cache: Dict) -> None:
    """保存辩论数据包缓存（原子写）"""
    f = _debate_data_cache_file()
    tmp = f.with_name(f.name + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as fp:
            json.dump(cache, fp, ensure_ascii=False)
        tmp.replace(f)
    except Exception as e:
        logger.warning(f"辩论数据缓存保存失败: {e}")


def _merge_news(ak_news: List[Dict], mx_news: List[Dict]) -> List[Dict]:
    """合并两个新闻源，去重"""
    seen = set()
    merged = []
    for n in mx_news:
        t = n.get("title", "")[:80]
        if t and t not in seen:
            seen.add(t)
            merged.append(n)
    for n in ak_news:
        t = n.get("title", "")[:80]
        if t and t not in seen and len(merged) < 10:
            seen.add(t)
            merged.append(n)
    return merged


_PREFETCH_CACHE_WRITE_LOCK = threading.Lock()


def _persist_prefetch_partial(
    stock_code: str,
    *,
    money_flow: Optional[Dict[str, Any]] = None,
    news: Optional[List[Dict[str, Any]]] = None,
    news_source: str = "",
    klines: Optional[List[Dict[str, Any]]] = None,
    kline_source: str = "",
    screening_metadata: Optional[Dict[str, Any]] = None,
    cache_override: Optional[Dict[str, Dict[str, Any]]] = None,
    persist: bool = True,
) -> None:
    """Persist each completed prefetch result so an interruption loses no work."""
    raw_key = str(stock_code or "").strip()
    if not raw_key:
        return
    key = raw_key.zfill(6)
    with _PREFETCH_CACHE_WRITE_LOCK:
        current = cache_override if cache_override is not None else _load_debate_data_cache()
        item = current.get(key, {}) if isinstance(current.get(key, {}), dict) else {}
        today = date.today().strftime("%Y%m%d")
        if item and str(item.get("updated") or "") != today:
            # A screening-only refresh must never make yesterday's flow/news look current.
            item = {}
        contract = item.get("data_contract", {}) if isinstance(item.get("data_contract", {}), dict) else {}

        if money_flow and _money_flow_has_any_value(money_flow):
            old_flow = item.get("money_flow", {}) if isinstance(item.get("money_flow", {}), dict) else {}
            merged_flow = _merge_money_flow(old_flow, money_flow)
            item["money_flow"] = merged_flow
            flow_flags = []
            if merged_flow.get("main_net_flow") is None:
                flow_flags.append("MONEY_FLOW_MISSING")
            elif _money_flow_has_gap(merged_flow):
                flow_flags.append("MONEY_FLOW_PARTIAL")
            flow_flags.extend(_money_flow_aux_flags(merged_flow))
            flow_contract = _data_result(
                source=str(merged_flow.get("source") or "none"),
                status=_contract_status(True, _money_flow_is_partial(merged_flow)),
                quality_flags=flow_flags,
                as_of=str(merged_flow.get("as_of") or ""),
            )
            flow_contract.update({
                "field_status": {
                    field: ("ok" if merged_flow.get(field) is not None else "missing")
                    for field in (
                        "main_net_flow", "super_net_flow", "ddx_5", "ddy_10",
                        "main_net_flow_5d", "main_net_flow_10d",
                    )
                },
                "field_sources": dict(merged_flow.get("field_sources") or {}),
                "field_as_of": dict(merged_flow.get("field_as_of") or {}),
                "units": dict(merged_flow.get("units") or {}),
                "diagnostics": dict(merged_flow.get("diagnostics") or {}),
            })
            contract["money_flow"] = flow_contract

        if news:
            old_news = item.get("news", []) if isinstance(item.get("news", []), list) else []
            merged_news = _merge_news(old_news, news)
            item["news"] = merged_news
            news_as_of = max(
                (
                    _normalize_kline_date(x.get("time"), dashed=False)
                    for x in merged_news
                    if isinstance(x, dict)
                ),
                default="",
            )
            old_news_contract = contract.get("news", {}) if isinstance(contract.get("news", {}), dict) else {}
            source_parts = []
            for source_text in (old_news_contract.get("source"), news_source):
                for part in re.split(r"[+,]", str(source_text or "")):
                    part = part.strip()
                    if part and part not in {"none", "prefetch_incremental", "debate_data_cache"} and part not in source_parts:
                        source_parts.append(part)
            checked_at = date.today().strftime("%Y%m%d")
            content_old = False
            if news_as_of:
                try:
                    content_old = (date.today() - datetime.strptime(news_as_of, "%Y%m%d").date()).days > 7
                except Exception:
                    content_old = False
            news_flags = ["NEWS_NO_RECENT_ITEMS"] if content_old else []
            contract["news"] = _data_result(
                source="+".join(source_parts) or "unknown",
                status="checked_fresh_no_recent_items" if content_old else _contract_status(bool(merged_news)),
                quality_flags=news_flags,
                as_of=checked_at,
            )
            contract["news"].update({
                "checked_at": checked_at,
                "content_as_of": news_as_of,
            })

        if klines:
            old_klines = item.get("klines", []) if isinstance(item.get("klines", []), list) else []
            selected = klines if len(klines) >= len(old_klines) else old_klines
            item["klines"] = selected
            contract["kline"] = _data_result(
                source=kline_source or "prefetch_incremental",
                status=_contract_status(bool(selected), bool(selected) and not _kline_has_min_bars(selected)),
                quality_flags=[] if _kline_has_min_bars(selected) else ["KLINE_SHORT"],
                as_of=_normalize_kline_date((selected[-1] or {}).get("date"), dashed=False) if selected else "",
            )

        if screening_metadata:
            old_meta = item.get("screening_metadata", {}) if isinstance(item.get("screening_metadata", {}), dict) else {}
            merged_meta = dict(old_meta)
            merged_meta.update(screening_metadata)
            item["screening_metadata"] = merged_meta
            instrument = merged_meta.get("instrument") if isinstance(merged_meta.get("instrument"), dict) else {}
            contract["instrument"] = _data_result(
                source=str(merged_meta.get("source") or "qmt_http"),
                status="ok" if instrument else "missing",
                quality_flags=[] if instrument else ["INSTRUMENT_METADATA_MISSING"],
                as_of=str(merged_meta.get("checked_at") or today),
            )
            sector = str(merged_meta.get("sector") or "").strip()
            contract["sector_screening"] = _data_result(
                source=str(merged_meta.get("sector_source") or "none"),
                status="ok" if sector else "missing",
                quality_flags=[] if sector else ["SECTOR_SCREENING_MISSING"],
                as_of=str(merged_meta.get("sector_as_of") or merged_meta.get("checked_at") or today),
            )

        item["data_contract"] = contract
        item["updated"] = today
        item["cache_schema_version"] = DEBATE_CACHE_SCHEMA_VERSION
        item["prefetch_partial"] = True
        current[key] = item
        if persist:
            _save_debate_data_cache(current)


def prefetch_screening_data(
    candidates: List[Dict[str, Any]],
    benchmark_code: str = "000300.SH",
) -> Dict[str, Any]:
    """Lightweight pre-screen fetch that is reused by the later debate prefetch."""
    today = date.today().strftime("%Y%m%d")
    expected_day = _previous_a_share_trading_day_compact()
    codes = _uniq_keep_order([
        str(candidate.get("stock") or candidate.get("stock_code") or "").strip().zfill(6)
        for candidate in candidates
        if str(candidate.get("stock") or candidate.get("stock_code") or "").strip()
    ])
    cache = _load_debate_data_cache()
    working_cache = dict(cache)
    batch_size = _env_int("SCREENING_PREFETCH_BATCH", 3, minimum=1)
    workers = _env_int("SCREENING_PREFETCH_WORKERS", 2, minimum=1)
    pause = _env_float("SCREENING_PREFETCH_BATCH_PAUSE_SEC", 0.3, minimum=0.0)

    def fetch_one(code: str) -> tuple[str, List[Dict[str, Any]], str, Dict[str, Any]]:
        old = cache.get(code, {}) if isinstance(cache.get(code, {}), dict) else {}
        if _cached_klines_valid(old, today):
            klines = list(old.get("klines") or [])
            source = str(((old.get("data_contract") or {}).get("kline") or {}).get("source") or "debate_data_cache")
        else:
            klines, source = _fetch_best_kline(code, days=120)
        old_meta = old.get("screening_metadata", {}) if isinstance(old.get("screening_metadata", {}), dict) else {}
        if str(old_meta.get("checked_at") or "") == today:
            metadata = old_meta
        else:
            metadata = _fetch_qmt_screening_metadata(code)
        return code, klines, source, metadata

    total_batches = (len(codes) + batch_size - 1) // batch_size if codes else 0
    for offset in range(0, len(codes), batch_size):
        batch = codes[offset:offset + batch_size]
        with ThreadPoolExecutor(max_workers=max(1, min(workers, len(batch)))) as executor:
            futures = {executor.submit(fetch_one, code): code for code in batch}
            for future in as_completed(futures):
                code, klines, source, metadata = future.result()
                _persist_prefetch_partial(
                    code,
                    klines=klines or None,
                    kline_source=source,
                    screening_metadata=metadata,
                    cache_override=working_cache,
                    persist=False,
                )
        with _PREFETCH_CACHE_WRITE_LOCK:
            _save_debate_data_cache(working_cache)
        done = offset // batch_size + 1
        logger.info(f"筛选数据预取进度: 批 {done}/{total_batches} 完成")
        if offset + batch_size < len(codes):
            time.sleep(pause)

    benchmark_file = _DATA_CACHE_DIR / f"screening_benchmark_{today}.json"
    benchmark_payload: Dict[str, Any] = {}
    try:
        if benchmark_file.exists():
            cached_benchmark = json.loads(benchmark_file.read_text(encoding="utf-8"))
            cached_rows = cached_benchmark.get("klines") or []
            cached_latest = _normalize_kline_date((cached_rows[-1] or {}).get("date"), dashed=False) if cached_rows else ""
            if cached_benchmark.get("updated") == today and cached_latest == expected_day:
                benchmark_payload = cached_benchmark
    except Exception:
        benchmark_payload = {}
    if not benchmark_payload:
        benchmark_rows, benchmark_source = _fetch_best_kline(benchmark_code, days=120)
        benchmark_payload = {
            "schema_version": "screening-benchmark-v1",
            "updated": today,
            "as_of": _normalize_kline_date((benchmark_rows[-1] or {}).get("date"), dashed=False) if benchmark_rows else "",
            "benchmark": benchmark_code,
            "source": benchmark_source,
            "klines": benchmark_rows,
        }
        _save_json_cache(benchmark_file, benchmark_payload)

    current = _load_debate_data_cache()
    stock_payload = {}
    fresh_count = 0
    for code in codes:
        item = current.get(code, {}) if isinstance(current.get(code, {}), dict) else {}
        rows = list(item.get("klines") or [])
        latest = _normalize_kline_date((rows[-1] or {}).get("date"), dashed=False) if rows else ""
        if _kline_has_min_bars(rows) and latest == expected_day:
            fresh_count += 1
        stock_payload[code] = {
            "klines": rows,
            "screening_metadata": dict(item.get("screening_metadata") or {}),
            "data_contract": dict(item.get("data_contract") or {}),
            "updated": item.get("updated"),
        }
    return {
        "schema_version": "screening-prefetch-v1",
        "updated": today,
        "as_of": expected_day,
        "stocks": stock_payload,
        "benchmark": benchmark_payload,
        "stats": {
            "requested": len(codes),
            "fresh_kline": fresh_count,
            "coverage": round(fresh_count / len(codes), 4) if codes else 0.0,
        },
    }


def _prefetch_debate_data(candidates: List[Dict]) -> None:
    """
    Phase 1 末尾调用：预获取所有候选股的资金流+新闻+完整日K线，并缓存到本地
    后续 build_debate_packet 直接读缓存，无需重新获取
    """
    cache = _load_debate_data_cache()
    today = date.today().strftime("%Y%m%d")
    stale_keys = [
        k for k, v in cache.items()
        if not isinstance(v, dict) or str(v.get("updated", "") or "") != today
    ]
    if stale_keys:
        for k in stale_keys:
            cache.pop(k, None)
        _save_debate_data_cache(cache)
        logger.info(f"辩论数据预获取: 已清理非当日缓存 {len(stale_keys)} 条，重新生成当日数据")
    raw_key_by_stock: Dict[str, str] = {}
    for c in candidates:
        raw_stock = str(c.get("stock") or "").strip()
        if not raw_stock:
            continue
        norm_stock = raw_stock.zfill(6)
        raw_key_by_stock.setdefault(norm_stock, raw_stock)
    stocks = _uniq_keep_order(list(raw_key_by_stock.keys()))
    candidate_map: Dict[str, Dict[str, Any]] = {
        str(c.get("stock")).zfill(6): c for c in candidates if c.get("stock")
    }
    money_stocks_to_fetch = []
    kline_stocks_to_fetch = []
    news_stocks_to_fetch = []
    for s in stocks:
        item = cache.get(s, {}) if isinstance(cache.get(s, {}), dict) else {}
        if s not in cache or _money_flow_cache_needs_refresh(item):
            money_stocks_to_fetch.append(s)
        if not _cached_klines_valid(item, today):
            kline_stocks_to_fetch.append(s)
        if not isinstance(item.get("news", []), list) or not item.get("news"):
            news_stocks_to_fetch.append(s)
    stocks_to_fetch = _uniq_keep_order(money_stocks_to_fetch + kline_stocks_to_fetch + news_stocks_to_fetch)
    if not stocks_to_fetch:
        logger.info(f"辩论数据预获取: 全部 {len(stocks)} 只资金流/新闻/K线已在当日缓存中")
        return
    logger.info(
        f"辩论数据预获取: 合计 {len(stocks_to_fetch)} 只需要获取 "
        f"(资金流{len(money_stocks_to_fetch)} 新闻{len(news_stocks_to_fetch)} K线{len(kline_stocks_to_fetch)})"
    )

    money_flow_deadline = time.monotonic() + _env_float("MONEY_FLOW_PREFETCH_BUDGET_SEC", 1800.0, minimum=10.0)  # ★ 6-05 老板拍板：1000s→1800s，对齐 watchdog 30 分钟（避免资金流预算先于 watchdog 掐断）

    def _money_flow_budget_left() -> float:
        return money_flow_deadline - time.monotonic()

    def _limit_source_stocks(source: str, items: List[str], default: int) -> List[str]:
        limit = _env_int(f"{source}_MONEY_FLOW_PREFETCH_MAX", default, minimum=0)
        return items if limit == 0 else items[:limit]

    def _run_parallel_money_source(
        label: str,
        items: List[str],
        fn,
        *,
        workers: int = 3,
        batch_size: Optional[int] = None,
        batch_pause: Optional[float] = None,
        on_result=None,
    ) -> Dict[str, Dict]:
        results: Dict[str, Dict] = {}
        if not items or _money_flow_budget_left() <= 0:
            return results
        batch_size = batch_size or _env_int("MONEY_FLOW_PREFETCH_BATCH", 3, minimum=1)
        batch_pause = _env_float("MONEY_FLOW_PREFETCH_BATCH_PAUSE_SEC", 1.0, minimum=0.0) if batch_pause is None else batch_pause
        total_batches = (len(items) + batch_size - 1) // batch_size
        logger.info(f"{label} 预获取开始: 共 {len(items)} 只 / {total_batches} 批 (batch_size={batch_size}, workers={workers})")
        for i in range(0, len(items), batch_size):
            if _money_flow_budget_left() <= 0:
                logger.warning(f"{label} 预获取达到资金流预算，跳过剩余 {len(items) - i} 只")
                break
            batch = items[i:i + batch_size]
            ex = ThreadPoolExecutor(max_workers=max(1, min(workers, len(batch))))
            futures = {ex.submit(fn, s): s for s in batch}
            try:
                for f in as_completed(futures, timeout=max(0.1, _money_flow_budget_left())):
                    sc, mf = f.result()
                    if mf:
                        results[sc] = mf
                        if on_result is not None:
                            try:
                                on_result(sc, mf)
                            except Exception as exc:
                                logger.warning(f"{label} 增量缓存失败 {sc}: {exc}")
            except TimeoutError:
                logger.warning(f"{label} 预获取超时，已返回已完成结果并放弃本批未完成任务")
            finally:
                for f in futures:
                    if not f.done():
                        f.cancel()
                ex.shutdown(wait=False, cancel_futures=True)
            # ★ 6-05 老板拍板：每批完成后打进度 log，防止 watchdog 误判卡住（之前 59 只全跑完才打 1 条，触发 600s 误杀）
            done_batches = i // batch_size + 1
            logger.info(f"{label} 进度: 批 {done_batches}/{total_batches} 完成，已获取 {len(results)}/{len(items)} 只，预算剩余 {_money_flow_budget_left():.0f}s")
            if i + batch_size < len(items):
                time.sleep(min(batch_pause, max(0.0, _money_flow_budget_left())))
        logger.info(f"{label} 预获取完成: 共获取 {len(results)}/{len(items)} 只")
        return results

    BATCH = _env_int("DEBATE_PREFETCH_BATCH", 3, minimum=1)

    # ★ QMT HTTP 资金流已移除（6-04 实测 QMT HTTP 资金流端点不可用）
    # 原：qmt_mf_results = _fetch_money_flow_batch_via_qmt_http(stocks_to_fetch)
    # 现在直接走 mx-data 资金流兜底小批量并行
    qmt_mf_results = {}  # 空占位，mx_stocks 判定会全部纳入

    # ── mx-data 资金流兜底小批量并行（主力净流入关键）──
    mx_mf_results = {}
    mx_stocks = [s for s in money_stocks_to_fetch if _money_flow_has_gap(qmt_mf_results.get(s, {}))]
    mx_stocks = _limit_source_stocks("MX", mx_stocks, 0)
    if mx_stocks and _env_bool("ENABLE_MX_MONEY_FLOW_PREFETCH", "1"):
        def _mx_money_flow(sc: str):
            return sc, _fetch_money_flow_via_mx(sc) or {}
        mx_mf_results = _run_parallel_money_source(
            "mx-data 资金流",
            mx_stocks,
            _mx_money_flow,
            workers=1,
            batch_size=_env_int("MX_MONEY_FLOW_PREFETCH_BATCH", 1, minimum=1),
            batch_pause=_env_float("MX_MONEY_FLOW_PREFETCH_BATCH_PAUSE_SEC", 3.0, minimum=0.0),
            on_result=lambda sc, mf: _persist_prefetch_partial(sc, money_flow=mf),
        )

    # ── Eastmoney 直连补齐：mx-data 缺字段/触发112后默认接力补齐，降低 mx 压力 ──
    em_mf_results = {}
    em_stocks = []
    for s in money_stocks_to_fetch:
        merged_now = _merge_money_flow(qmt_mf_results.get(s, {}), mx_mf_results.get(s, {}))
        if _money_flow_has_gap(merged_now):
            em_stocks.append(s)
    em_stocks = _limit_source_stocks("EASTMONEY", em_stocks, 0)

    def _eastmoney_money_flow(sc: str):
        try:
            mf = _fetch_money_flow_via_eastmoney_direct(sc)
            return sc, mf if mf else {}
        except Exception:
            return sc, {}

    if em_stocks and _env_bool("ENABLE_EASTMONEY_FLOW_PREFETCH", "1"):
        em_mf_results = _run_parallel_money_source(
            "eastmoney 资金流",
            em_stocks,
            _eastmoney_money_flow,
            workers=_env_int("EASTMONEY_FLOW_PREFETCH_WORKERS", 1, minimum=1),
            batch_size=_env_int("EASTMONEY_FLOW_PREFETCH_BATCH", 1, minimum=1),
            batch_pause=_env_float("EASTMONEY_FLOW_PREFETCH_BATCH_PAUSE_SEC", 0.6, minimum=0.0),
            on_result=lambda sc, mf: _persist_prefetch_partial(sc, money_flow=mf),
        )

    ak_mf_results = {}
    ak_stocks = []
    for s in money_stocks_to_fetch:
        merged_now = _merge_money_flow(qmt_mf_results.get(s, {}), mx_mf_results.get(s, {}), em_mf_results.get(s, {}))
        if _money_flow_has_gap(merged_now):
            ak_stocks.append(s)
    ak_stocks = _limit_source_stocks("AKSHARE", ak_stocks, 0)

    def _akshare_money_flow(sc: str):
        try:
            mf = _fetch_money_flow_via_akshare(sc)
            return sc, mf if mf else {}
        except Exception:
            return sc, {}

    if ak_stocks and _env_bool("ENABLE_AKSHARE_INDIVIDUAL_FLOW_PREFETCH", "0"):
        ak_mf_results = _run_parallel_money_source(
            "akshare 个股资金流",
            ak_stocks,
            _akshare_money_flow,
            on_result=lambda sc, mf: _persist_prefetch_partial(sc, money_flow=mf),
        )

    # ── akshare 新闻小批量并行（3线程+间隔1s）──
    def _akshare_news(sc: str):
        try:
            import akshare as ak
            items = []
            seen_t = set()
            df = ak.stock_news_em(symbol=sc)
            if df is not None and len(df) > 0:
                for _, row in df.iterrows():
                    title = str(row.get("新闻标题", "")).strip()[:80]
                    content = str(row.get("新闻内容", "") or "").strip()
                    if len(content) > 20 and title and title not in seen_t and len(items) < 5:
                        seen_t.add(title)
                        items.append({"title": title, "content": content[:200],
                                     "source": str(row.get("文章来源", "") or "").strip(),
                                     "time": str(row.get("发布时间", "") or "").strip()[:16]})
            return sc, items
        except Exception:
            return sc, []


    ak_news_results = {}
    for i in range(0, len(news_stocks_to_fetch), BATCH):
        batch = news_stocks_to_fetch[i:i+BATCH]
        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = {ex.submit(_akshare_news, s): s for s in batch}
            for f in as_completed(futures):
                sc, news = f.result()
                if news:
                    ak_news_results[sc] = news
                    _persist_prefetch_partial(sc, news=news, news_source="akshare")
        if i + BATCH < len(news_stocks_to_fetch):
            time.sleep(1)

    # ── mx-search 新闻小批量并行（3线程+间隔1s）──
    mx_name_map = {str(c["stock"]).zfill(6): c.get("name", c["stock"]) for c in candidates if c.get("stock")}
    mx_news_results = {}
    # ★ 6-05 老板拍板：mx-news 段加进度 log（之前 59 只全跑完才打 1 条，触发 watchdog 600s 误杀）
    mx_news_total_batches = (len(news_stocks_to_fetch) + BATCH - 1) // BATCH if news_stocks_to_fetch else 0
    logger.info(f"mx-search 新闻预获取开始: 共 {len(news_stocks_to_fetch)} 只 / {mx_news_total_batches} 批 (batch_size={BATCH}, workers=3)")
    for i in range(0, len(news_stocks_to_fetch), BATCH):
        batch = news_stocks_to_fetch[i:i+BATCH]
        def _mx_news_fn(sc: str):
            return sc, _fetch_news_via_mxsearch(mx_name_map.get(sc, sc), max_results=5)
        with ThreadPoolExecutor(max_workers=3) as ex:
            futures = {ex.submit(_mx_news_fn, s) for s in batch}
            for f in as_completed(futures):
                sc, news = f.result()
                if news:
                    mx_news_results[sc] = news
                    _persist_prefetch_partial(sc, news=news, news_source="mx-search")
        done_batches = i // BATCH + 1
        logger.info(f"mx-search 新闻进度: 批 {done_batches}/{mx_news_total_batches} 完成，已获取 {len(mx_news_results)}/{len(news_stocks_to_fetch)} 只")
        if i + BATCH < len(news_stocks_to_fetch):
            time.sleep(1)

    # ── 完整日K线预获取：写入同一个 debate_data_cache，后续 build_debate_packet 直接读缓存 ──
    kline_results: Dict[str, List[Dict[str, Any]]] = {}
    kline_sources: Dict[str, str] = {}

    def _prefetch_one_kline(sc: str):
        best_kline, selected_source = _fetch_best_kline(sc, days=120)
        return sc, best_kline, selected_source

    kline_batch = _env_int("KLINE_PREFETCH_BATCH", 3, minimum=1)
    kline_workers = _env_int("KLINE_PREFETCH_WORKERS", 2, minimum=1)
    kline_pause = _env_float("KLINE_PREFETCH_BATCH_PAUSE_SEC", 0.5, minimum=0.0)
    kline_total_batches = (len(kline_stocks_to_fetch) + kline_batch - 1) // kline_batch if kline_stocks_to_fetch else 0
    if kline_stocks_to_fetch:
        logger.info(
            f"K线预获取开始: 共 {len(kline_stocks_to_fetch)} 只 / {kline_total_batches} 批 "
            f"(batch_size={kline_batch}, workers={kline_workers})"
        )
    for i in range(0, len(kline_stocks_to_fetch), kline_batch):
        batch = kline_stocks_to_fetch[i:i + kline_batch]
        with ThreadPoolExecutor(max_workers=max(1, min(kline_workers, len(batch)))) as ex:
            futures = {ex.submit(_prefetch_one_kline, s): s for s in batch}
            for f in as_completed(futures):
                sc, klines, source = f.result()
                if klines:
                    kline_results[sc] = klines
                    kline_sources[sc] = source
                    _persist_prefetch_partial(sc, klines=klines, kline_source=source)
        done_batches = i // kline_batch + 1
        logger.info(
            f"K线预获取进度: 批 {done_batches}/{kline_total_batches} 完成，"
            f"已获取 {len(kline_results)}/{len(kline_stocks_to_fetch)} 只"
        )
        if i + kline_batch < len(kline_stocks_to_fetch):
            time.sleep(kline_pause)

    # 合并写入缓存
    for sc in stocks_to_fetch:
        old = cache.get(sc, {}) if isinstance(cache.get(sc, {}), dict) else {}
        old_mf = old.get("money_flow", {}) if isinstance(old.get("money_flow", {}), dict) else {}
        if sc in money_stocks_to_fetch:
            rank_mf = _fetch_money_flow_via_akshare_rank(sc) or {}
            merged_mf = _merge_money_flow(
                qmt_mf_results.get(sc, {}),
                mx_mf_results.get(sc, {}),
                em_mf_results.get(sc, {}),
                ak_mf_results.get(sc, {}),
                rank_mf,
                old_mf,
            )
            # 最后兜底：候选池已有主力资金值时，补齐 main_net_flow，避免后续辩论包主力资金空缺。
            if merged_mf.get("main_net_flow") is None:
                seeded = _pool_main_flow_to_yi(candidate_map.get(str(sc).zfill(6), {}))
                if seeded is not None:
                    merged_mf = _merge_money_flow(
                        merged_mf,
                        {"main_net_flow": seeded, "source": "pool_seed", "as_of": ""},
                    )
            final_mf = merged_mf if merged_mf else old_mf
            if _money_flow_is_partial(final_mf):
                missing_fields = [
                    k for k in ("main_net_flow", "super_net_flow", "ddx_5", "ddy_10")
                    if final_mf.get(k) is None
                ]
                used_sources = [
                    name for name, data in (
                        ("mx", mx_mf_results.get(sc, {})),
                        ("eastmoney", em_mf_results.get(sc, {})),
                        ("ak", ak_mf_results.get(sc, {})),
                        ("ak_rank", rank_mf),
                        ("old_cache", old_mf),
                    )
                    if _money_flow_has_any_value(data)
                ]
                logger.warning(
                    f"资金流字段仍部分缺失 {sc}: missing={missing_fields} "
                    f"sources={'+'.join(used_sources) or 'none'} value={final_mf}"
                )
        else:
            final_mf = old_mf
        news_new = _merge_news(ak_news_results.get(sc, []), mx_news_results.get(sc, []))
        mf_flags: List[str] = []
        if not final_mf or not _money_flow_has_any_value(final_mf):
            mf_flags.append("MONEY_FLOW_MISSING")
        elif final_mf.get("main_net_flow") is None:
            mf_flags.append("MONEY_FLOW_MISSING")
        elif _money_flow_has_gap(final_mf):
            mf_flags.append("MONEY_FLOW_PARTIAL")
        mf_flags.extend(_money_flow_aux_flags(final_mf or {}))
        selected_klines = kline_results.get(sc) or old.get("klines") or []
        if not selected_klines:
            kline_flags = ["KLINE_MISSING"]
        elif not _kline_has_min_bars(selected_klines):
            kline_flags = ["KLINE_SHORT"]
        else:
            kline_flags = []
        final_news = news_new if news_new else old.get("news", [])
        news_content_as_of = max(
            (
                _normalize_kline_date(x.get("time"), dashed=False)
                for x in (final_news or [])
                if isinstance(x, dict)
            ),
            default="",
        )
        news_checked_at = today if sc in news_stocks_to_fetch else str(
            ((old.get("data_contract") or {}).get("news") or {}).get("checked_at")
            or old.get("updated")
            or ""
        )
        news_content_old = False
        if news_content_as_of:
            try:
                news_content_old = (date.today() - datetime.strptime(news_content_as_of, "%Y%m%d").date()).days > 7
            except Exception:
                news_content_old = False
        news_flags = ["NEWS_NO_RECENT_ITEMS"] if sc in news_stocks_to_fetch and (not final_news or news_content_old) else []
        news_status = (
            "checked_fresh_no_recent_items"
            if sc in news_stocks_to_fetch and (not final_news or news_content_old)
            else _contract_status(bool(final_news))
        )
        old_contract = old.get("data_contract", {}) if isinstance(old.get("data_contract", {}), dict) else {}
        cache_item = {
            "money_flow": final_mf,
            "news": final_news,
            "klines": kline_results.get(sc) or old.get("klines", []),
            "screening_metadata": dict(old.get("screening_metadata") or {}),
            "data_contract": {
                "money_flow": _data_result(
                    source=str((final_mf or {}).get("source") or "none"),
                    status=_contract_status(
                        bool(final_mf and _money_flow_has_any_value(final_mf)),
                        bool(final_mf and _money_flow_has_any_value(final_mf) and _money_flow_is_partial(final_mf)),
                    ),
                    quality_flags=mf_flags,
                    as_of=str((final_mf or {}).get("as_of") or ""),
                ),
                "kline": _data_result(
                    source=kline_sources.get(sc) or ("debate_data_cache" if old.get("klines") else "none"),
                    status=_contract_status(bool(selected_klines), bool(selected_klines) and not _kline_has_min_bars(selected_klines)),
                    quality_flags=kline_flags,
                    as_of=_normalize_kline_date((selected_klines[-1] or {}).get("date"), dashed=False) if selected_klines else "",
                ),
                "news": _data_result(
                    source=(
                        "akshare+mx-search"
                        if sc in news_stocks_to_fetch
                        else str(((old.get("data_contract") or {}).get("news") or {}).get("source") or "debate_data_cache")
                    ),
                    status=news_status,
                    quality_flags=news_flags,
                    as_of=news_checked_at or news_content_as_of,
                ),
                "instrument": dict(old_contract.get("instrument") or {}),
                "sector_screening": dict(old_contract.get("sector_screening") or {}),
            },
            "updated": date.today().strftime("%Y%m%d"),
            "cache_schema_version": DEBATE_CACHE_SCHEMA_VERSION,
        }
        cache_item["data_contract"]["money_flow"].update({
            "field_status": {
                field: ("ok" if (final_mf or {}).get(field) is not None else "missing")
                for field in (
                    "main_net_flow", "super_net_flow", "ddx_5", "ddy_10",
                    "main_net_flow_5d", "main_net_flow_10d",
                )
            },
            "field_sources": dict((final_mf or {}).get("field_sources") or {}),
            "field_as_of": dict((final_mf or {}).get("field_as_of") or {}),
            "units": dict((final_mf or {}).get("units") or {}),
            "diagnostics": dict((final_mf or {}).get("diagnostics") or {}),
        })
        cache_item["data_contract"]["news"].update({
            "checked_at": news_checked_at,
            "content_as_of": news_content_as_of,
        })
        cache[sc] = cache_item
        raw_sc = raw_key_by_stock.get(sc)
        if raw_sc and raw_sc != sc:
            cache[raw_sc] = cache_item
    _save_debate_data_cache(cache)
    complete = 0
    aux_complete = 0
    ddx_missing = 0
    ddy_missing = 0
    missing_main = 0
    all_missing = 0
    seeded_main = 0
    for sc in money_stocks_to_fetch:
        item = cache.get(sc, {}) if isinstance(cache.get(sc, {}), dict) else {}
        mf = item.get("money_flow", {}) if isinstance(item.get("money_flow", {}), dict) else {}
        if not mf or all(mf.get(k) is None for k in ("main_net_flow", "super_net_flow", "ddx_5", "ddy_10")):
            all_missing += 1
            continue
        src_text = str(mf.get("source") or "")
        if "pool_seed" in src_text and mf.get("main_net_flow") is not None:
            seeded_main += 1
        if mf.get("main_net_flow") is None:
            missing_main += 1
            continue
        if not _money_flow_has_gap(mf):
            complete += 1
        if not _money_flow_aux_has_gap(mf):
            aux_complete += 1
        if mf.get("ddx_5") is None:
            ddx_missing += 1
        if mf.get("ddy_10") is None:
            ddy_missing += 1
    kline_complete = 0
    for sc in stocks:
        item = cache.get(sc, {}) if isinstance(cache.get(sc, {}), dict) else {}
        if _cached_klines_valid(item, today):
            kline_complete += 1
    logger.info(f"辩论数据预获取完成: {len(cache)} 只已缓存 "
               f"(qmt_mf={len(qmt_mf_results)} ak_mf={len(ak_mf_results)} mx_mf={len(mx_mf_results)} em_mf={len(em_mf_results)} "
               f"ak_news={len(ak_news_results)} mx_news={len(mx_news_results)} "
               f"kline_complete={kline_complete}/{len(stocks)} "
               f"money_flow_core_complete={complete}/{len(money_stocks_to_fetch)} "
               f"money_flow_aux_complete={aux_complete}/{len(money_stocks_to_fetch)} "
               f"ddx_missing={ddx_missing} ddy_missing={ddy_missing} "
               f"money_flow_main_missing={missing_main} all_missing={all_missing} "
               f"money_flow_seeded_main={seeded_main})")


def load_phase1_cache(output_dir: Path) -> Dict:
    """
    加载 Phase 1 缓存的财务数据
    来自 output/fundamental_cache/all_stocks_financial.json
    """
    cache_file = output_dir / "fundamental_cache" / "all_stocks_financial.json"
    if not cache_file.exists():
        logger.warning(f"Phase1 财务缓存不存在: {cache_file}")
        return {}

    try:
        with open(cache_file) as f:
            data = json.load(f)
        records = data.get("data", data) if isinstance(data, dict) else {}
        if not isinstance(records, dict):
            return {}
        normalized = {}
        for raw_code, record in records.items():
            if not isinstance(record, dict):
                continue
            code = str(raw_code).strip().split(".", 1)[0].zfill(6)
            normalized[code] = normalize_financial_record(record)
        return normalized
    except Exception as e:
        logger.warning(f"读取 Phase1 缓存失败: {e}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# 新增：技术信号检测（蜡烛图/量价背离/海龟/波浪/江恩）
# ─────────────────────────────────────────────────────────────────────────────

def _detect_technical_signals(kline_data: List[Dict]) -> dict:
    """
    整合所有技术信号检测
    输入: kline_data - List[Dict](columns: date,open,high,low,close,volume)
    返回技术信号字典
    """
    result = {}

    # 转换为 DataFrame（如果还不是）
    if kline_data is None or len(kline_data) < 5:
        return {
            "candlestick_patterns": {"verdict": "数据不足"},
            "volume_price_divergence": {"verdict": "数据不足"},
            "turtle_signals": {"signal": "数据不足"},
            "elliott_wave": {"verdict": "数据不足"},
            "gann_levels": {"verdict": "数据不足"},
        }

    if not isinstance(kline_data, pd.DataFrame):
        kline_df = pd.DataFrame(kline_data)
    else:
        kline_df = kline_data

    # 确保日期排序
    if 'date' in kline_df.columns:
        kline_df = kline_df.sort_values('date').reset_index(drop=True)

    # 蜡烛图信号
    if candlestick_signals is not None:
        try:
            cs_result = candlestick_signals.detect_candlestick_signals(kline_df)
            result['candlestick_patterns'] = {
                'buy_signals': cs_result.get('buy_signals', []),
                'sell_signals': cs_result.get('sell_signals', []),
                'pattern_score': cs_result.get('pattern_score', 0),
                'verdict': cs_result.get('verdict', '无信号'),
            }
        except Exception as e:
            logger.warning(f"蜡烛图信号检测失败: {e}")
            result['candlestick_patterns'] = {'error': str(e)}
    else:
        result['candlestick_patterns'] = {'verdict': '模块不可用'}

    # 量价背离
    if volume_price_divergence is not None:
        try:
            vp_result = volume_price_divergence.detect_volume_price_divergence(kline_df)
            result['volume_price_divergence'] = {
                'signal': vp_result.get('verdict', '无信号'),
                'details': vp_result.get('signals', []),
            }
        except Exception as e:
            logger.warning(f"量价背离检测失败: {e}")
            result['volume_price_divergence'] = {'error': str(e)}
    else:
        result['volume_price_divergence'] = {'verdict': '模块不可用'}

    # 海龟突破
    if turtle_signals is not None:
        try:
            turtle_result = turtle_signals.detect_turtle_signals(kline_df)
            result['turtle_signals'] = {
                'breakout_20d': turtle_result.get('breakout_20d', False),
                'breakout_55d': turtle_result.get('breakout_55d', False),
                'false_breakout': turtle_result.get('false_breakout', False),
                'atr_n': turtle_result.get('atr_n', None),
                'signal': turtle_result.get('signal', '无信号'),
            }
        except Exception as e:
            logger.warning(f"海龟信号检测失败: {e}")
            result['turtle_signals'] = {'error': str(e)}
    else:
        result['turtle_signals'] = {'signal': '模块不可用'}

    # 艾略特波浪
    if elliott_signals is not None:
        try:
            ew_result = elliott_signals.detect_elliott_position(kline_df)
            result['elliott_wave'] = {
                'wave_position': ew_result.get('wave_position', '难判断'),
                'wave5_warning': ew_result.get('wave5_warning', False),
                'verdict': ew_result.get('verdict', '无信号'),
            }
        except Exception as e:
            logger.warning(f"艾略特波浪判断失败: {e}")
            result['elliott_wave'] = {'error': str(e)}
    else:
        result['elliott_wave'] = {'verdict': '模块不可用'}

    # 江恩回调位
    if gann_signals is not None:
        try:
            gann_result = gann_signals.detect_gann_levels(kline_df)
            result['gann_levels'] = {
                'near_support': gann_result.get('near_support', False),
                'near_resistance': gann_result.get('near_resistance', False),
                'key_levels': gann_result.get('key_retracement_levels', []),
                'verdict': gann_result.get('verdict', '无信号'),
            }
        except Exception as e:
            logger.warning(f"江恩回调位检测失败: {e}")
            result['gann_levels'] = {'error': str(e)}
    else:
        result['gann_levels'] = {'verdict': '模块不可用'}

    return result
