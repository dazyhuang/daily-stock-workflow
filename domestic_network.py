#!/usr/bin/env python3
"""
国内金融数据源网络保护层。

目标：
- 东方财富/同花顺/巨潮/腾讯行情等国内源绕过环境代理。
- akshare/mx-data 类接口统一重试、降速。
- 子进程调用 mx-data / mx-xuangu 时继承同样的直连策略。
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from typing import Callable, Iterable, Optional, TypeVar
from urllib.parse import urlparse

T = TypeVar("T")

logger = logging.getLogger("domestic_network")

DOMESTIC_DIRECT_HOSTS = (
    "mkapi2.dfcfs.com",
    ".dfcfs.com",
    ".eastmoney.com",
    ".eastmoney.com.cn",
    ".10jqka.com.cn",
    ".cninfo.com.cn",
    ".sse.com.cn",
    ".szse.cn",
    ".gtimg.cn",
    ".qq.com",
    "web.ifzq.gtimg.cn",
    "push2.eastmoney.com",
    "push2his.eastmoney.com",
    "datacenter-web.eastmoney.com",
    "emdata.eastmoney.com",
    "80.push2.eastmoney.com",
    "127.0.0.1",
    "localhost",
    "127.0.0.1",
)

PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "http_proxy",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
    "HTTPProxy",
    "HTTPSProxy",
)

_REQUESTS_PATCHED = False
_PATCH_LOCK = threading.Lock()
_THROTTLE_LOCK = threading.Lock()
_LAST_CALL_AT: dict[str, float] = {}


def _global_min_interval() -> float:
    try:
        return max(0.0, float(os.getenv("DOMESTIC_REQUEST_INTERVAL_SEC", "3.0")))
    except Exception:
        return 3.0


def _normalize_host(host: str) -> str:
    return str(host or "").split(":", 1)[0].strip().lower()


def _host_matches(host: str, patterns: Iterable[str] = DOMESTIC_DIRECT_HOSTS) -> bool:
    host = _normalize_host(host)
    if not host:
        return False
    for pattern in patterns:
        pattern = pattern.lower()
        if pattern.startswith("."):
            if host.endswith(pattern) or host == pattern[1:]:
                return True
        elif host == pattern or host.endswith("." + pattern):
            return True
    return False


def should_bypass_proxy(url: str) -> bool:
    return _host_matches(urlparse(str(url)).netloc)


def _append_no_proxy(env: dict[str, str]) -> None:
    additions = list(DOMESTIC_DIRECT_HOSTS)
    for key in ("NO_PROXY", "no_proxy"):
        existing = [p.strip() for p in env.get(key, "").split(",") if p.strip()]
        merged = existing[:]
        seen = {p.lower() for p in existing}
        for item in additions:
            if item.lower() not in seen:
                merged.append(item)
                seen.add(item.lower())
        env[key] = ",".join(merged)


def configure_domestic_direct_network() -> None:
    """配置环境与 requests patch，让国内金融数据源不走环境代理。"""
    _append_no_proxy(os.environ)  # type: ignore[arg-type]
    install_requests_direct_patch()


def domestic_subprocess_env(base_env: Optional[dict[str, str]] = None, *, clear_proxy: bool = True) -> dict[str, str]:
    """给 mx-data / mx-xuangu 子进程使用的环境变量。"""
    env = dict(base_env or os.environ)
    _append_no_proxy(env)
    if clear_proxy:
        for key in PROXY_ENV_KEYS:
            env.pop(key, None)
    env.setdefault("A_STOCK_DOMESTIC_DIRECT", "1")
    return env


def install_requests_direct_patch() -> None:
    global _REQUESTS_PATCHED
    with _PATCH_LOCK:
        if _REQUESTS_PATCHED:
            return
        try:
            import requests
        except Exception:
            return

        original_merge = requests.sessions.Session.merge_environment_settings

        def merge_environment_settings(self, url, proxies, stream, verify, cert):
            settings = original_merge(self, url, proxies, stream, verify, cert)
            if should_bypass_proxy(url):
                settings["proxies"] = {}
            return settings

        requests.sessions.Session.merge_environment_settings = merge_environment_settings
        _REQUESTS_PATCHED = True


def direct_session():
    import requests

    session = requests.Session()
    session.trust_env = False
    return session


def request_direct(method: str, url: str, **kwargs):
    """对国内数据接口发起直连 requests 请求。"""
    configure_domestic_direct_network()
    with direct_session() as session:
        return session.request(method, url, **kwargs)


def throttle(key: str, min_interval: float = 0.8) -> None:
    """同一数据源请求之间留出一点时间，降低远端断连概率。"""
    min_interval = max(float(min_interval or 0.0), _global_min_interval())
    if min_interval <= 0:
        return
    now = time.monotonic()
    wait = 0.0
    with _THROTTLE_LOCK:
        last = _LAST_CALL_AT.get(key, 0.0)
        wait = min_interval - (now - last)
        if wait > 0:
            _LAST_CALL_AT[key] = now + wait
        else:
            _LAST_CALL_AT[key] = now
    if wait > 0:
        time.sleep(wait)


def retry_call(
    label: str,
    fn: Callable[[], T],
    *,
    retries: int = 4,
    base_delay: float = 1.5,
    max_delay: float = 20.0,
    throttle_key: Optional[str] = None,
    min_interval: float = 0.8,
) -> T:
    """带指数退避的请求重试，专门兜 RemoteDisconnected/接口抽风。"""
    last_err: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        if throttle_key:
            throttle(throttle_key, min_interval=min_interval)
        try:
            return fn()
        except Exception as exc:
            last_err = exc
            if attempt >= retries:
                break
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay += random.uniform(0.2, 0.8)
            logger.warning("%s 第%d/%d次失败，%.1fs 后重试: %s", label, attempt, retries, delay, exc)
            time.sleep(delay)
    assert last_err is not None
    raise last_err


configure_domestic_direct_network()
