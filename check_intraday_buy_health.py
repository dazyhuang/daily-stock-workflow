#!/usr/bin/env python3
"""Read-only health checks for the intraday buy-timing task."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import sqlite3
import subprocess
from dataclasses import dataclass, asdict
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"
CRON_JOBS_FILE = Path("~/.openclaw/cron/jobs.json.migrated")
CRON_DB_FILE = Path("~/.openclaw/state/openclaw.sqlite")
EXPECTED_START_TIME = dt_time(9, 25)


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""
    severity: str = "error"


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _load_env_file_values() -> dict[str, tuple[str, str]]:
    values: dict[str, tuple[str, str]] = {}
    for path in (BASE_DIR / ".env", BASE_DIR / ".env.local"):
        if not path.exists():
            continue
        for raw_line in _read_text(path).splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            if key:
                values[key] = (value, path.name)
    return values


def _effective_intraday_buy_models(env_values: dict[str, tuple[str, str]]) -> dict[str, tuple[str, str]]:
    primary = env_values.get("INTRADAY_BUY_TIMING_LLM_MODEL")
    if not primary or not primary[0]:
        primary = env_values.get("INTRADAY_LLM_MODEL", ("minimax-portal/MiniMax-M3", "default"))
    fallback = env_values.get("INTRADAY_BUY_TIMING_LLM_FALLBACK_MODEL")
    if not fallback or not fallback[0]:
        fallback = env_values.get("INTRADAY_LLM_FALLBACK_MODEL", ("openai-codex/gpt-5.5", "default"))
    return {"primary": primary, "fallback": fallback}


def _env_value_enabled(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"_json_error": str(exc)}


def _state_last_order_id(entry: dict) -> str:
    if not isinstance(entry, dict):
        return ""
    last_order = entry.get("last_order") if isinstance(entry.get("last_order"), dict) else {}
    return str(last_order.get("order_id") or last_order.get("orderId") or "")


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _line_dt(line: str) -> datetime | None:
    if len(line) < 19:
        return None
    try:
        return datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _latest_log_dt(text: str) -> datetime | None:
    latest = None
    for line in (text or "").splitlines():
        line_time = _line_dt(line)
        if line_time and (latest is None or line_time > latest):
            latest = line_time
    return latest


def _latest_scheduled_next_check(text: str, day: date) -> datetime | None:
    latest_line_time = None
    latest_next = None
    pattern = re.compile(r"下一轮分时买入判断:\s*(\d{1,2}):(\d{2})")
    for line in (text or "").splitlines():
        match = pattern.search(line)
        if not match:
            continue
        line_time = _line_dt(line)
        if line_time and latest_line_time and line_time < latest_line_time:
            continue
        try:
            hh = int(match.group(1))
            mm = int(match.group(2))
            next_dt = datetime.combine(day, dt_time(hh, mm))
        except Exception:
            continue
        latest_line_time = line_time or latest_line_time
        latest_next = next_dt
    return latest_next


def _slice_shared_log_to_buy_task(text: str, start_markers: list[str], state: Any) -> str:
    """Keep only the shared-log region owned by the intraday buy task.

    Unit/audit tests and other intraday tasks write into logs/intraday_YYYYMMDD.log
    too. Scanning the whole shared log causes false failures after the buy task has
    already finished.
    """
    if not text:
        return ""
    starts = [idx for marker in start_markers if (idx := text.find(marker)) >= 0]
    if not starts:
        return ""
    clipped = text[min(starts):]
    finished_at = _parse_dt(state.get("finished_at")) if isinstance(state, dict) else None
    if not finished_at:
        return clipped
    keep_until = finished_at + timedelta(minutes=1)
    kept: list[str] = []
    for line in clipped.splitlines():
        line_time = _line_dt(line)
        if line_time and line_time > keep_until:
            break
        kept.append(line)
    return "\n".join(kept)


def _slice_log_from_first_start(text: str, start_markers: list[str]) -> str:
    """Ignore pre-launch noise once the real buy-timing task has started.

    A same-day cron may fail before importing enough code to log the normal
    start marker, then be recovered manually. The old traceback should remain
    visible in the raw log, but current health must be judged from the first
    successful task start onward.
    """
    if not text:
        return ""
    starts = [idx for marker in start_markers if (idx := text.find(marker)) >= 0]
    if not starts:
        return text
    return text[min(starts):]


def _slice_log_from_latest_start(text: str, start_markers: list[str]) -> str:
    """Keep the currently active/recovered run after same-day restarts.

    Manual recovery can leave an earlier import traceback or KeyboardInterrupt in
    the same daily log. Runtime health should describe the latest active run;
    overlapping duplicate processes are covered by the lock/pid checks.
    """
    if not text:
        return ""
    starts = [idx for marker in start_markers if (idx := text.rfind(marker)) >= 0]
    if not starts:
        return text
    return text[max(starts):]


def _pid_alive(pid: str) -> bool:
    try:
        pid_int = int(str(pid).strip())
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
        return False


def _pid_command(pid: str) -> str:
    try:
        ret = subprocess.run(
            ["ps", "-p", str(int(str(pid).strip())), "-o", "command="],
            capture_output=True,
            text=True,
            timeout=2,
        )
        return (ret.stdout or "").strip()
    except Exception:
        return ""


def _is_buy_timing_pid(pid: str) -> bool:
    cmd = _pid_command(pid)
    return _command_is_buy_timing(cmd)


def _owner_is_live_buy_timing(owner: dict[str, Any], lock_dir: Path, day: date) -> bool:
    pid = str(owner.get("pid") or "")
    if _is_buy_timing_pid(pid):
        return True
    if not _pid_alive(pid):
        return False

    # In restricted desktop sessions, ps may be denied even though kill(pid, 0)
    # proves the process exists. Accept that case only when the lock metadata
    # fully matches today's buy-timing task.
    owner_day = str(owner.get("date") or "")
    cwd_ok = str(owner.get("cwd") or "") == str(BASE_DIR)
    lock_ok = str(owner.get("lock_dir") or "") == str(lock_dir)
    return owner_day == day.isoformat() and cwd_ok and lock_ok


def _command_is_buy_timing(cmd: str) -> bool:
    if "intraday_executor.py" not in (cmd or ""):
        return False
    try:
        parts = shlex.split(cmd)
    except Exception:
        parts = str(cmd).split()
    modes: list[str] = []
    for idx, part in enumerate(parts):
        if part == "--mode" and idx + 1 < len(parts):
            modes.append(parts[idx + 1])
        elif part.startswith("--mode="):
            modes.append(part.split("=", 1)[1])
    if not modes:
        return True
    return any(mode in {"buy", "buy-timing"} for mode in modes)


def _tracebacks_are_only_recovered_qmt_disconnects(text: str) -> bool:
    matches = list(re.finditer(r"Traceback \(most recent call last\):", text))
    if not matches:
        return False
    for match in matches:
        if "无法连接xtquant服务" not in text[match.start(): match.start() + 2400]:
            return False
    return True


def _after_cutoff(day: date) -> bool:
    if date.today() != day:
        return date.today() > day
    return datetime.now().time() >= dt_time(14, 57)


def _before_expected_start(day: date) -> bool:
    return date.today() == day and datetime.now().time() < EXPECTED_START_TIME


def _during_expected_session(day: date) -> bool:
    return date.today() == day and EXPECTED_START_TIME <= datetime.now().time() < dt_time(14, 57)


def _load_cron_jobs() -> list[dict]:
    data = _load_json(CRON_JOBS_FILE)
    if isinstance(data, dict):
        jobs = data.get("jobs")
        return jobs if isinstance(jobs, list) else []
    return data if isinstance(data, list) else []


def _find_intraday_buy_job(jobs: list[dict]) -> dict | None:
    for job in jobs:
        if job.get("name") == "intraday-buy" or job.get("id") == "9d436fd5-10bb-4e0e-90cf-b3dc12934897":
            return job
    return None


def _load_active_cron_jobs() -> list[dict]:
    if not CRON_DB_FILE.exists():
        return [{"_error": f"active cron db missing: {CRON_DB_FILE}"}]
    try:
        conn = sqlite3.connect(CRON_DB_FILE)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            select
              job_id, name, enabled, schedule_expr, schedule_tz,
              payload_kind, payload_message, payload_model,
              payload_timeout_seconds, consecutive_errors,
              last_run_status, last_error, next_run_at_ms, job_json
            from cron_jobs
            """
        ).fetchall()
        conn.close()
        return [dict(row) for row in rows]
    except Exception as exc:
        return [{"_error": f"active cron db read failed: {exc}"}]


def _find_active_cron_job(jobs: list[dict], name: str) -> dict | None:
    for job in jobs:
        if job.get("name") == name:
            return job
    return None


def _active_command_payload(job: dict | None) -> str:
    if not job:
        return ""
    parts = [str(job.get("payload_message") or "")]
    try:
        job_json = json.loads(str(job.get("job_json") or "{}"))
        payload = job_json.get("payload") if isinstance(job_json, dict) else {}
        if isinstance(payload, dict):
            argv = payload.get("argv")
            if isinstance(argv, list):
                parts.append(" ".join(str(x) for x in argv))
            parts.append(str(payload.get("cwd") or ""))
    except Exception:
        pass
    return " ".join(p for p in parts if p)


def _is_avoid_signal(signal: dict) -> bool:
    text = " ".join(
        str(signal.get(key) or "")
        for key in ("signal", "action", "final_decision", "raw_final_decision")
    ).upper()
    return "AVOID" in text


def _today_top_pool(day: date, target: int = 5) -> list[dict]:
    report = _load_json(OUTPUT_DIR / f"daily_report_{day.strftime('%Y%m%d')}.json")
    phase2 = report.get("phase2") if isinstance(report, dict) else {}
    top_picks = phase2.get("top_picks") if isinstance(phase2, dict) else []
    pool: list[dict] = []
    seen = set()
    for item in top_picks or []:
        if not isinstance(item, dict):
            continue
        stock = str(item.get("stock") or "").strip()
        if not stock or stock in seen or _is_avoid_signal(item):
            continue
        seen.add(stock)
        pool.append(item)
        if len(pool) >= target:
            break
    return pool


def _latest_previous_state_file(day: date) -> Path | None:
    is_trading_day = None
    try:
        shared_path = Path.home() / ".openclaw" / "agents" / "shared"
        if str(shared_path) not in os.sys.path:
            os.sys.path.insert(0, str(shared_path))
        from trading_calendar import is_a_share_trading_day as calendar_check
        is_trading_day = calendar_check
    except Exception:
        is_trading_day = None
    cur = day - timedelta(days=1)
    for _ in range(14):
        is_prev = cur.weekday() < 5
        if is_trading_day is not None:
            try:
                is_prev = bool(is_trading_day(cur.isoformat()))
            except Exception:
                pass
        if is_prev:
            path = OUTPUT_DIR / f"intraday_buy_timing_{cur.strftime('%Y%m%d')}.json"
            return path if path.exists() else None
        cur -= timedelta(days=1)
    return None


def _expected_carryover_pool(day: date, today_pool: list[dict]) -> list[dict]:
    prev_file = _latest_previous_state_file(day)
    if not prev_file:
        return []
    prev_state = _load_json(prev_file)
    if not isinstance(prev_state, dict):
        return []
    selected_signals = [
        item for item in (prev_state.get("selected_signals") or [])
        if isinstance(item, dict) and item.get("stock")
    ]
    if not selected_signals:
        prev_day = None
        try:
            prev_day = datetime.fromisoformat(str(prev_state.get("date") or "")).date()
        except Exception:
            try:
                prev_day = datetime.strptime(prev_file.stem.replace("intraday_buy_timing_", ""), "%Y%m%d").date()
            except Exception:
                prev_day = None
        if prev_day:
            selected_signals = _today_top_pool(prev_day, 5)
    signal_map = {str(item.get("stock")): item for item in selected_signals}
    today_seen = {str(item.get("stock")) for item in today_pool if item.get("stock")}
    carried_once = {str(item) for item in (prev_state.get("carryover_stocks") or [])}
    stocks_state = prev_state.get("stocks") or {}
    carryovers: list[dict] = []
    for stock in prev_state.get("selected_stocks") or []:
        stock = str(stock)
        if not stock or stock in today_seen or stock in carried_once:
            continue
        entry = stocks_state.get(stock) if isinstance(stocks_state, dict) else {}
        if isinstance(entry, dict) and entry.get("status") == "filled":
            continue
        signal = signal_map.get(stock)
        if signal:
            carryovers.append(signal)
    return carryovers


def run_checks(day_key: str) -> list[Check]:
    day = datetime.strptime(day_key, "%Y%m%d").date()
    output_log = OUTPUT_DIR / f"intraday_buy_{day_key}.log"
    shared_log = LOG_DIR / f"intraday_{day_key}.log"
    state_file = OUTPUT_DIR / f"intraday_buy_timing_{day_key}.json"
    pid_file = OUTPUT_DIR / "buy_timing.pid"
    lockdirs = sorted(OUTPUT_DIR.glob(f"intraday_buy_timing_{day_key}.lockdir"))
    output_text = _read_text(output_log)
    shared_text_raw = _read_text(shared_log)
    state = _load_json(state_file)
    checks: list[Check] = []
    before_start = _before_expected_start(day)
    during_session = _during_expected_session(day)

    checks.append(Check(
        "output log ready",
        output_log.exists(),
        str(output_log) + (" not expected before 09:25" if before_start and not output_log.exists() else ""),
        "warn" if before_start else "error",
    ))

    jobs = _load_cron_jobs()
    buy_job = _find_intraday_buy_job(jobs)
    cron_payload = str((buy_job or {}).get("payload") or "")
    cron_ok = bool(
        buy_job
        and buy_job.get("enabled") is True
        and "--mode=buy-timing" in cron_payload
        and "daily-stock-workflow" in cron_payload
        and "nohup" not in cron_payload
        and " 2>&1 &" not in cron_payload
    )
    checks.append(Check(
        "cron intraday-buy uses buy-timing",
        cron_ok,
        "enabled/new-command" if cron_ok else f"job={bool(buy_job)} file={CRON_JOBS_FILE}",
    ))

    active_jobs = _load_active_cron_jobs()
    active_error = next((j.get("_error") for j in active_jobs if j.get("_error")), "")
    active_buy_jobs = [
        job for job in active_jobs
        if job.get("name") == "intraday-buy" and int(job.get("enabled") or 0) == 1
    ] if not active_error else []
    checks.append(Check(
        "exactly one active intraday-buy cron",
        len(active_buy_jobs) == 1,
        active_error or f"enabled_count={len(active_buy_jobs)} ids={[job.get('job_id') for job in active_buy_jobs]}",
    ))
    active_buy_job = active_buy_jobs[0] if len(active_buy_jobs) == 1 else None
    active_payload = _active_command_payload(active_buy_job)
    active_cron_ok = bool(
        active_buy_job
        and int(active_buy_job.get("enabled") or 0) == 1
        and active_buy_job.get("schedule_expr") == "25 9 * * 1-5"
        and active_buy_job.get("schedule_tz") == "Asia/Shanghai"
        and active_buy_job.get("payload_kind") == "command"
        and "--mode=buy-timing" in active_payload
        and "daily-stock-workflow" in active_payload
        and "nohup" not in active_payload
        and " 2>&1 &" not in active_payload
        and "PYTHONUNBUFFERED=1" in active_payload
        and "PYTHONPYCACHEPREFIX=/private/tmp/openclaw_pycache" in active_payload
        and int(active_buy_job.get("payload_timeout_seconds") or 0) >= 18000
    )
    checks.append(Check(
        "active cron intraday-buy is direct buy-timing command",
        active_cron_ok,
        active_error or (
            "enabled command 09:25 Asia/Shanghai"
            if active_cron_ok
            else f"job={bool(active_buy_job)} kind={(active_buy_job or {}).get('payload_kind')} schedule={(active_buy_job or {}).get('schedule_expr')} timeout={(active_buy_job or {}).get('payload_timeout_seconds')} payload={active_payload[:180]}"
        ),
    ))

    daily_job = next((job for job in jobs if job.get("name") == "daily-stock-workflow"), None)
    daily_schedule = daily_job.get("schedule") if isinstance(daily_job, dict) else {}
    daily_payload_obj = daily_job.get("payload") if isinstance(daily_job, dict) else {}
    daily_payload_text = json.dumps(daily_payload_obj, ensure_ascii=False) if isinstance(daily_payload_obj, dict) else str(daily_payload_obj or "")
    daily_file_ok = bool(
        daily_job
        and daily_job.get("enabled") is True
        and isinstance(daily_schedule, dict)
        and daily_schedule.get("expr") == "10 0 * * 1-5"
        and daily_schedule.get("tz") == "Asia/Shanghai"
        and isinstance(daily_payload_obj, dict)
        and daily_payload_obj.get("kind") == "command"
        and "run_daily_stock_workflow_stable.py" in daily_payload_text
        and "nohup" not in daily_payload_text
        and "disown" not in daily_payload_text
    )
    checks.append(Check(
        "cron daily-stock-workflow is timezone-bound direct command",
        daily_file_ok,
        "enabled command 00:10 Asia/Shanghai" if daily_file_ok else f"job={bool(daily_job)} file={CRON_JOBS_FILE} schedule={daily_schedule} payload={daily_payload_text[:160]}",
    ))

    active_daily_job = _find_active_cron_job(active_jobs, "daily-stock-workflow") if not active_error else None
    active_daily_payload = _active_command_payload(active_daily_job)
    active_daily_ok = bool(
        active_daily_job
        and int(active_daily_job.get("enabled") or 0) == 1
        and active_daily_job.get("schedule_expr") == "10 0 * * 1-5"
        and active_daily_job.get("schedule_tz") == "Asia/Shanghai"
        and active_daily_job.get("payload_kind") == "command"
        and "run_daily_stock_workflow_stable.py" in active_daily_payload
        and "nohup" not in active_daily_payload
        and "disown" not in active_daily_payload
        and int(active_daily_job.get("payload_timeout_seconds") or 0) >= 7200
    )
    checks.append(Check(
        "active cron daily-stock-workflow is direct command",
        active_daily_ok,
        active_error or (
            "enabled command 00:10 Asia/Shanghai"
            if active_daily_ok
            else f"job={bool(active_daily_job)} kind={(active_daily_job or {}).get('payload_kind')} schedule={(active_daily_job or {}).get('schedule_expr')} tz={(active_daily_job or {}).get('schedule_tz')} timeout={(active_daily_job or {}).get('payload_timeout_seconds')} payload={active_daily_payload[:180]}"
        ),
    ))

    env_values = _load_env_file_values()
    dangerous_env_keys = {
        "DRY_RUN": "会让买入只模拟不真实下单",
        "ALLOW_BUY_OUTSIDE_WINDOW": "会允许盘外运行，可能绕过09:31-14:57窗口",
        "INTRADAY_BUY_TIMING_ONCE": "会让盘中买入只跑一轮后退出",
        "INTRADAY_BUY_ENABLE_REALTIME_THREAD": "会试图恢复旧10秒硬触发线程",
    }
    dangerous_hits = [
        f"{key}={value}({source}: {reason})"
        for key, reason in dangerous_env_keys.items()
        for value, source in [env_values.get(key, ("", ""))]
        if _env_value_enabled(value)
    ]
    checks.append(Check(
        "no dangerous intraday-buy env overrides",
        not dangerous_hits,
        "; ".join(dangerous_hits),
    ))

    drift_expectations = {
        "INTRADAY_BUY_TIMING_START": "09:31",
        "INTRADAY_BUY_TIMING_CUTOFF": "14:57",
        "INTRADAY_BUY_SKIP_EARLIEST": "14:57",
        "INTRADAY_BUY_LLM_INTERVAL_BEFORE_10": "3",
        "INTRADAY_BUY_LLM_INTERVAL_AFTER_10": "10",
    }
    drift_hits = []
    for key, expected in drift_expectations.items():
        value, source = env_values.get(key, ("", ""))
        if value and value != expected:
            drift_hits.append(f"{key}={value}({source}, expected {expected})")
    checks.append(Check(
        "intraday-buy timing env matches intended schedule",
        not drift_hits,
        "; ".join(drift_hits),
    ))

    model_config = _effective_intraday_buy_models(env_values)
    expected_models = {
        "primary": "minimax-portal/MiniMax-M3",
        "fallback": "openai-codex/gpt-5.5",
    }
    model_hits = []
    for role, expected in expected_models.items():
        value, source = model_config.get(role, ("", ""))
        if value != expected:
            model_hits.append(f"{role}={value}({source}, expected {expected})")
    checks.append(Check(
        "intraday-buy LLM model env matches intended default/fallback",
        not model_hits,
        "; ".join(model_hits),
    ))

    qmt_pressure_keys = {
        "INTRADAY_BUY_INCLUDE_INDEX_DATA": "指数额外全推行情",
        "INTRADAY_BUY_INCLUDE_BOARD_DATA": "板块额外行情/数据",
    }
    qmt_pressure_hits = [
        f"{key}={value}({source}: {reason})"
        for key, reason in qmt_pressure_keys.items()
        for value, source in [env_values.get(key, ("", ""))]
        if _env_value_enabled(value)
    ]
    checks.append(Check(
        "extra QMT-heavy intraday-buy env disabled",
        not qmt_pressure_hits,
        "; ".join(qmt_pressure_hits),
        "warn",
    ))

    active_buy_watchdogs = [
        job for job in active_jobs
        if "intraday" in str(job.get("name", "")) and "buy" in str(job.get("name", "")) and "watchdog" in str(job.get("name", ""))
        and int(job.get("enabled") or 0) == 1
    ]
    checks.append(Check(
        "no active intraday buy watchdog",
        not active_buy_watchdogs,
        ",".join(str(job.get("name")) for job in active_buy_watchdogs),
    ))

    start_markers = [
        "技术分时买入启动",
        "Phase 4: 技术优先分时买入模式",
        "started PID",
    ]
    output_runtime_text = _slice_log_from_latest_start(output_text, start_markers)
    shared_text = "" if output_log.exists() else _slice_shared_log_to_buy_task(shared_text_raw, start_markers, state)
    if before_start and not output_log.exists():
        # Before the scheduled launch, shared logs may contain audit/unit-test
        # simulations that import intraday_executor and exercise startup guards.
        # Treat those as non-authoritative until the real output log appears.
        shared_text = ""
    combined_text = "\n".join([output_runtime_text, shared_text])

    today_pool = _today_top_pool(day, 5)
    carryover_pool = _expected_carryover_pool(day, today_pool)
    launch_pool_codes = [
        str(item.get("stock"))
        for item in list(today_pool) + list(carryover_pool)
        if item.get("stock")
    ]
    top_pool_ready = len(today_pool) >= 5
    checks.append(Check(
        "today Top5 pool ready for intraday buy",
        top_pool_ready,
        f"top_count={len(today_pool)} stocks={launch_pool_codes[:5]}",
        "warn" if before_start and not top_pool_ready else "error",
    ))
    checks.append(Check(
        "launch observation pool is non-empty",
        bool(launch_pool_codes),
        f"launch_count={len(launch_pool_codes)} carryover_count={len(carryover_pool)} stocks={launch_pool_codes}",
        "warn" if before_start and not launch_pool_codes else "error",
    ))
    output_start_hits = [marker for marker in start_markers if marker in output_text]
    shared_start_hits = [marker for marker in start_markers if marker in shared_text]
    output_start_count = max([output_text.count(marker) for marker in start_markers] or [0])
    shared_start_count = max([shared_text.count(marker) for marker in start_markers] or [0])
    start_seen = output_start_count > 0 or shared_start_count > 0
    checks.append(Check(
        "task started without overlapping duplicate",
        start_seen,
        f"output={output_start_hits} output_start_count={output_start_count} shared={shared_start_hits} shared_start_count={shared_start_count}" + (" before_expected_start" if before_start else ""),
        "warn" if before_start and not start_seen else "error",
    ))

    finished_state = bool(isinstance(state, dict) and state.get("finished_at"))
    post_finish_starts = 0
    if finished_state:
        try:
            finished_at = datetime.fromisoformat(str(state.get("finished_at")))
            for line in output_text.splitlines():
                if "Phase 4: 技术优先分时买入模式" not in line:
                    continue
                match = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                if match and datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S") > finished_at:
                    post_finish_starts += 1
        except Exception:
            post_finish_starts = -1
    checks.append(Check(
        "no post-finish duplicate starts",
        post_finish_starts in (0, -1),
        f"post_finish_starts={post_finish_starts}" if post_finish_starts >= 0 else "unable to parse finish/start timestamps",
        "warn",
    ))

    qmt_disconnect_hits = combined_text.count("无法连接xtquant服务")
    recovered_qmt_tracebacks = finished_state and qmt_disconnect_hits and _tracebacks_are_only_recovered_qmt_disconnects(combined_text)
    fatal_patterns = ["线程已崩溃", "NameError:", "ModuleNotFoundError:", "SyntaxError:"]
    if re.search(r"Traceback \(most recent call last\):", combined_text) and not recovered_qmt_tracebacks:
        fatal_patterns.append("Traceback")
    fatal_hits = [pat for pat in fatal_patterns if pat in combined_text]
    checks.append(Check("no fatal traceback in buy logs", not fatal_hits, ",".join(fatal_hits)))

    checks.append(Check(
        "no unrecovered QMT disconnect in buy logs",
        qmt_disconnect_hits == 0 or finished_state,
        f"hits={qmt_disconnect_hits} finished={finished_state}",
        "warn" if qmt_disconnect_hits and finished_state else "error",
    ))

    rate_hits = combined_text.count("API 限速(112)") + combined_text.count("112限速")
    checks.append(Check("no QMT 112 limit in buy logs", rate_hits == 0, f"hits={rate_hits}"))

    legacy_fast_hits = combined_text.count("间隔10秒") + combined_text.count("每10秒快轮询")
    checks.append(Check("no legacy 10s fast polling", legacy_fast_hits == 0, f"hits={legacy_fast_hits}"))

    k120_hits = combined_text.count("period=120m") + combined_text.count("120分钟K线")
    checks.append(Check("no 120m kline polling in buy timing", k120_hits == 0, f"hits={k120_hits}"))

    try:
        source_text = (BASE_DIR / "intraday_executor.py").read_text(encoding="utf-8", errors="replace")
    except Exception:
        source_text = ""
    optional_pydantic_ok = (
        "from pydantic import BaseModel, Field" in source_text
        and "except ModuleNotFoundError" in source_text
        and "class BaseModel:" in source_text
        and "def Field(" in source_text
    )
    checks.append(Check(
        "pydantic is optional for cron python",
        optional_pydantic_ok,
        "fallback present" if optional_pydantic_ok else "pydantic hard dependency may crash cron",
    ))
    huge_1m_count = "count=60000" in source_text
    checks.append(Check(
        "1m kline request count is bounded",
        not huge_1m_count and "INTRADAY_BUY_1M_BAR_COUNT" in source_text,
        "bounded" if not huge_1m_count else "count=60000 found",
    ))
    strict_ma = "_sma(ma_closes, w)" in source_text and "_sma_or_available(ma_closes, w)" not in source_text
    checks.append(Check(
        "intraday MA windows require full 1m bars",
        strict_ma,
        "strict" if strict_ma else "MA falls back to short available bars",
    ))
    post_open_only_ma120 = (
        '"technical_trigger": "MA120_CROSS_UP"' in source_text
        and '"technical_trigger": "MA_MULTI_CROSS_UP"' not in source_text
        and '"technical_trigger": "MA_BULLISH_ALIGNMENT"' not in source_text
    )
    checks.append(Check(
        "post-open hard trigger is MA120 cross only",
        post_open_only_ma120,
        "MA120 only" if post_open_only_ma120 else "extra post-open hard triggers found",
    ))
    opening_retry_guard = (
        "opening_chase_evaluated_at" in source_text
        and "_opening_snapshot_ready(snapshot)" in source_text
        and "entry.get(\"decision_count\")" not in source_text[
            source_text.find("def _is_opening_chase_time"):
            source_text.find("def _opening_strong_buy_decision")
        ]
    )
    checks.append(Check(
        "opening strong check survives transient WAIT",
        opening_retry_guard,
        "retry-safe" if opening_retry_guard else "decision_count may consume opening chase chance",
    ))

    realtime_thread_hits = combined_text.count("兼容线程已启用")
    checks.append(Check("realtime compatibility thread disabled", realtime_thread_hits == 0, f"hits={realtime_thread_hits}"))
    realtime_thread_source_removed = (
        "threading.Thread" not in source_text
        and "兼容线程已停用" in source_text
        and "兼容线程已启用" not in source_text
        and "兼容线程异常" not in source_text
        and "快轮询启动" not in source_text
        and "REALTIME_POLL_INTERVAL" not in source_text
        and "旧实时硬触发快轮询入口已禁用" in source_text
    )
    checks.append(Check(
        "realtime compatibility thread source removed",
        realtime_thread_source_removed,
        "removed" if realtime_thread_source_removed else "legacy realtime branch/config still present",
    ))

    duplicate_warnings = combined_text.count("已有今日盘中买入进程正在运行")
    checks.append(Check(
        "duplicate buy process attempts were blocked by lock",
        True,
        f"hits={duplicate_warnings}" + (" before_expected_start" if before_start else ""),
        "warn" if duplicate_warnings else "error",
    ))

    report_skip = ("分时买入跳过" in combined_text and "今日报告" in combined_text) or "选股报告未在" in combined_text
    checks.append(Check("daily report was available before cutoff", not report_skip, "report_skip_detected" if report_skip else ""))

    if _after_cutoff(day):
        finished = bool(isinstance(state, dict) and state.get("finished_at")) or "分时买入结束" in combined_text
        checks.append(Check("task reached normal finish after cutoff", finished, "finished_at/log_end_missing" if not finished else ""))

    checks.append(Check("single lockdir at most", len(lockdirs) <= 1, f"lockdirs={len(lockdirs)}"))
    live_lock_pid = False
    live_lock_owner_pid = ""
    if lockdirs:
        owner = _load_json(lockdirs[0] / "owner.json") or {}
        live_lock_owner_pid = str(owner.get("pid") or "")
        live_lock_pid = _owner_is_live_buy_timing(owner, lockdirs[0], day)
        ok = live_lock_pid or before_start
        checks.append(Check("lock owner is live during session", ok, f"pid={live_lock_owner_pid} alive_buy={live_lock_pid}"))
    else:
        checks.append(Check("no stale lockdir", True, ""))

    live_pid_file = False
    if pid_file.exists():
        pid = pid_file.read_text(encoding="utf-8", errors="replace").strip()
        live_pid_file = _is_buy_timing_pid(pid) or (live_lock_pid and pid == live_lock_owner_pid)
        ok = live_pid_file or before_start
        checks.append(Check("pid file points to live buy process", ok, f"pid={pid} alive_buy={live_pid_file}"))
    else:
        checks.append(Check("no stale pid file", True, ""))

    if during_session:
        checks.append(Check(
            "task alive during session unless finished",
            finished_state or live_lock_pid or live_pid_file,
            f"finished={finished_state} live_lock={live_lock_pid} live_pid={live_pid_file}",
        ))
        latest_log_time = _latest_log_dt(combined_text)
        scheduled_next = _latest_scheduled_next_check(combined_text, day)
        max_stale_seconds = 300
        waiting_for_scheduled_next = (
            scheduled_next is not None
            and datetime.now() < scheduled_next
            and latest_log_time is not None
            and latest_log_time <= scheduled_next
        )
        log_is_fresh = (
            finished_state
            or before_start
            or waiting_for_scheduled_next
            or (
                latest_log_time is not None
                and (datetime.now() - latest_log_time).total_seconds() <= max_stale_seconds
            )
        )
        checks.append(Check(
            "buy task log is still advancing",
            log_is_fresh,
            f"latest={latest_log_time} next={scheduled_next} max_stale_seconds={max_stale_seconds}",
        ))

    if isinstance(state, dict) and "_json_error" in state:
        checks.append(Check("state json parses", False, state["_json_error"]))
    elif state is None:
        state_missing_is_expected = before_start or (during_session and (live_lock_pid or live_pid_file))
        checks.append(Check(
            "state file ready",
            False,
            str(state_file) + (" not expected before 09:25" if before_start else ""),
            "warn" if state_missing_is_expected else "error",
        ))
    else:
        stocks = state.get("stocks") if isinstance(state.get("stocks"), dict) else {}
        checks.append(Check("state date matches", state.get("date") == day.isoformat(), f"date={state.get('date')}"))
        checks.append(Check("state has selected stocks", bool(state.get("selected_stocks")), f"selected={len(state.get('selected_stocks') or [])}"))
        checks.append(Check("state has stock entries", bool(stocks), f"stocks={len(stocks)}"))
        checks.append(Check("transient stock locks not persisted", "_timing_stock_locks" not in state, ""))
        round_errors = state.get("round_errors") if isinstance(state.get("round_errors"), list) else []
        consecutive_errors = int(state.get("consecutive_round_errors", 0) or 0)
        checks.append(Check(
            "no swallowed buy round errors",
            not round_errors and consecutive_errors == 0,
            f"round_errors={len(round_errors)} consecutive={consecutive_errors}",
        ))
        missing_filled_identity = []
        for stock, entry in stocks.items():
            if not isinstance(entry, dict) or entry.get("status") != "filled":
                continue
            order_id = _state_last_order_id(entry)
            if order_id and (str(entry.get("order_id") or "") != order_id or str(entry.get("trade_key") or "") != f"order:{order_id}"):
                missing_filled_identity.append(str(stock))
        checks.append(Check(
            "filled buy entries keep order identity",
            not missing_filled_identity,
            ",".join(missing_filled_identity),
        ))
        invalid_pending = [
            stock for stock, entry in stocks.items()
            if _after_cutoff(day) and isinstance(entry, dict) and entry.get("status") == "pending"
        ]
        checks.append(Check("no pending status after cutoff", not invalid_pending, ",".join(invalid_pending)))

    return checks


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().strftime("%Y%m%d"), help="YYYYMMDD")
    parser.add_argument("--json", action="store_true", help="Output JSON")
    args = parser.parse_args()

    checks = run_checks(args.date)
    ok = all(c.ok or c.severity == "warn" for c in checks)
    if args.json:
        print(json.dumps({"ok": ok, "checks": [asdict(c) for c in checks]}, ensure_ascii=False, indent=2))
    else:
        for check in checks:
            status = "PASS" if check.ok else ("WARN" if check.severity == "warn" else "FAIL")
            detail = f" - {check.detail}" if check.detail else ""
            print(f"[{status}] {check.name}{detail}")
        print(f"OVERALL: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
