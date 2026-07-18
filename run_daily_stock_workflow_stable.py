#!/usr/bin/env python3
"""Stable cron entry for the daily stock workflow.

Runs the normal workflow only when there is no same-day checkpoint. If today's
workflow was interrupted, resume from the checkpoint instead of starting a new
full workflow.
"""

import json
import hashlib
import os
import subprocess
import sys
import time
from datetime import date
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
WORKFLOW = BASE_DIR / "workflow.py"


def _today() -> str:
    return date.today().strftime("%Y%m%d")


def _report_path() -> Path:
    return OUTPUT_DIR / f"daily_report_{_today()}.json"


def _push_marker_path() -> Path:
    return OUTPUT_DIR / f"daily_report_push_{_today()}.json"


def _checkpoint_path() -> Path:
    return OUTPUT_DIR / "debate_checkpoint.json"


def _phase1_context_path() -> Path:
    return OUTPUT_DIR / "phase1_context.json"


def _load_dotenv_into(env: dict) -> None:
    env_file = BASE_DIR / ".env"
    if not env_file.exists():
        return
    for raw_line in env_file.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in env:
            env[key] = value


def _is_mtime_today(path: Path) -> bool:
    try:
        return date.fromtimestamp(path.stat().st_mtime).strftime("%Y%m%d") == _today()
    except Exception:
        return False


def _checkpoint_matches_today() -> bool:
    cp_file = _checkpoint_path()
    if not cp_file.exists() or not _is_mtime_today(cp_file):
        return False
    try:
        data = json.loads(cp_file.read_text(encoding="utf-8"))
    except Exception:
        return False
    return isinstance(data, dict) and data.get("date") == _today()


def _phase1_context_matches_today() -> bool:
    phase1_file = _phase1_context_path()
    if not phase1_file.exists() or not _is_mtime_today(phase1_file):
        return False
    try:
        data = json.loads(phase1_file.read_text(encoding="utf-8"))
    except Exception:
        return False
    return isinstance(data, list) and any(
        isinstance(item, dict) and item.get("status") == "success"
        for item in data
    )


def _resume_state_kind() -> str:
    if _checkpoint_matches_today():
        return "checkpoint"
    if _phase1_context_matches_today():
        return "phase1"
    return ""


def _resume_state_available() -> bool:
    """Return True when today has enough state to continue, not restart."""
    return bool(_resume_state_kind())


def _report_is_complete(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(data, dict) or data.get("status") != "success":
        return False
    report_day = str(data.get("date") or "").replace("-", "")[:8]
    if report_day != _today():
        return False
    picks = ((data.get("phase2") or {}).get("top_picks") or [])
    if len(picks) != 5:
        return False
    artifacts = data.get("artifacts") or {}
    for key in ("candidates_jsonl", "trace_json", "summary_json"):
        artifact = artifacts.get(key)
        if not artifact or not Path(artifact).exists():
            return False

    push_marker = _push_marker_path()
    if not push_marker.exists():
        return False
    try:
        push_data = json.loads(push_marker.read_text(encoding="utf-8"))
    except Exception:
        return False
    if (
        not isinstance(push_data, dict)
        or push_data.get("date") != date.today().isoformat()
        or push_data.get("status") != "success"
        or push_data.get("top_picks_count") != 5
    ):
        return False
    payload = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str)
    report_digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    if push_data.get("report_digest") != report_digest:
        return False
    return True


def _pid_alive(pid) -> bool:
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


def _read_lock_owner(lock_dir: Path) -> dict:
    try:
        return json.loads((lock_dir / "owner.json").read_text(encoding="utf-8"))
    except Exception:
        return {}


def _remove_lock_dir(lock_dir: Path) -> bool:
    try:
        for child in lock_dir.iterdir():
            if child.is_file() or child.is_symlink():
                child.unlink(missing_ok=True)
        lock_dir.rmdir()
        return True
    except FileNotFoundError:
        return True
    except Exception as exc:
        print(f"清理旧工作流锁失败: {exc}", file=sys.stderr)
        return False


@contextmanager
def _workflow_singleton_lock() -> Iterator[bool]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    lock_dir = OUTPUT_DIR / f"daily_stock_workflow_{_today()}.lockdir"
    lock_path = OUTPUT_DIR / f"daily_stock_workflow_{_today()}.lock"
    owner_file = lock_dir / "owner.json"
    acquired = False
    while True:
        try:
            lock_dir.mkdir(mode=0o755)
            acquired = True
            break
        except FileExistsError:
            owner = _read_lock_owner(lock_dir)
            pid = owner.get("pid")
            if _pid_alive(pid):
                print(f"已有今日选股工作流实例正在运行: pid={pid}, started_at={owner.get('started_at')}")
                yield False
                return
            print(f"发现锁记录中的PID已不存活，立即清理后继续: {owner}", file=sys.stderr)
            if not _remove_lock_dir(lock_dir):
                yield False
                return

    payload = {
        "pid": os.getpid(),
        "started_at": __import__("datetime").datetime.now().isoformat(),
        "lock_dir": str(lock_dir),
        "owner": "run_daily_stock_workflow_stable.py",
    }
    tmp_owner = owner_file.with_name(f"{owner_file.name}.{os.getpid()}.tmp")
    tmp_owner.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp_owner, owner_file)
    lock_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    try:
        yield True
    finally:
        if acquired:
            try:
                owner_file.unlink(missing_ok=True)
                lock_dir.rmdir()
                lock_path.unlink(missing_ok=True)
            except FileNotFoundError:
                pass
            except Exception as exc:
                print(f"释放工作流锁失败: {exc}", file=sys.stderr)


# ★ 6-05 老板拍板：watchdog 30 分钟没进展 kill 进程（6-05 实测 59 只预取经常超过 10 分钟，触发误杀）
WATCHDOG_TIMEOUT_SECONDS = int(os.environ.get("WORKFLOW_WATCHDOG_SECONDS", "1800"))  # 默认 30 分钟（覆盖 59 只预取最坏耗时）
WATCHDOG_GRACE_SECONDS = 30  # SIGTERM 后等 30 秒优雅退出，超时 SIGKILL


def _run(label: str, args: list[str]) -> int:
    print(f"== {label} == (watchdog: {WATCHDOG_TIMEOUT_SECONDS}s 没进展则 SIGTERM)")
    env = os.environ.copy()
    _load_dotenv_into(env)
    env["PYTHONUNBUFFERED"] = "1"
    env.setdefault("OPENCLAW_WORKSPACE", ".")
    env.setdefault("DAILY_STOCK_WORKFLOW_SEND_FEISHU", "1")
    env.setdefault("TA_DEFAULT_MODEL", "volcengine-plan/ark-code-latest")
    env.setdefault("TA_FALLBACK_MODEL", "openai/gpt-5.6-sol")
    env.setdefault("TA_SECONDARY_FALLBACK_MODEL", "minimax-portal/MiniMax-M3")
    env.setdefault("TA_ROLE_MAX_TOKENS", "12288")
    env.setdefault("TA_THINKING_BUDGET_VOLCAN", "8192")
    env.setdefault("TECH_ANALYST_MODEL", "volcengine-plan/ark-code-latest")
    env.setdefault("TECH_ANALYST_FALLBACK_MODEL", "openai/gpt-5.6-sol")
    env.setdefault("PORTFOLIO_MANAGER_PRIMARY_MODEL", "openai/gpt-5.6-sol")
    env.setdefault("PORTFOLIO_MANAGER_PRIMARY_REASONING_EFFORT", "max")
    env.setdefault("PORTFOLIO_MANAGER_TIMEOUT", "300")
    env.setdefault("PORTFOLIO_MANAGER_SECONDARY_MODEL", "openai/gpt-5.6-sol")
    env.setdefault("PORTFOLIO_MANAGER_SECONDARY_REASONING_EFFORT", "max")
    env.setdefault("PORTFOLIO_MANAGER_SECONDARY_FALLBACK_MODEL", "")
    env.setdefault("PORTFOLIO_MANAGER_TERTIARY_MODEL", "minimax-portal/MiniMax-M3")
    env.setdefault("MINIMAX_ALLOW_MX_DIRECT_KEY", "1")
    env["DAILY_STOCK_WORKFLOW_LOCK_HELD"] = "1"
    proc = subprocess.Popen(
        [sys.executable, str(WORKFLOW), *args],
        cwd=str(BASE_DIR),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None

    import select
    import time as _time
    last_progress_ts = _time.time()
    watchdog_thread_running = True

    while True:
        # select 0.5s 超时，避免阻塞读 stdout
        ready, _, _ = select.select([proc.stdout], [], [], 0.5)
        if ready:
            line = proc.stdout.readline()
            if line == "":
                # 子进程退出
                break
            print(line, end="")
            last_progress_ts = _time.time()
        # 检查 watchdog 超时
        elif _time.time() - last_progress_ts > WATCHDOG_TIMEOUT_SECONDS:
            elapsed = int(_time.time() - last_progress_ts)
            print(f"\n⚠️ [WATCHDOG]  {elapsed}s 没新 log 输出（超时阈值 {WATCHDOG_TIMEOUT_SECONDS}s）→ 发送 SIGTERM 优雅退出\n", flush=True)
            proc.terminate()
            # 等待优雅退出
            try:
                proc.wait(timeout=WATCHDOG_GRACE_SECONDS)
                print(f"  [WATCHDOG] 子进程 {proc.pid} 优雅退出（返回码 {proc.returncode}）", flush=True)
            except subprocess.TimeoutExpired:
                print(f"  [WATCHDOG] 子进程 {proc.pid} 优雅退出超时 {WATCHDOG_GRACE_SECONDS}s → SIGKILL 强杀", flush=True)
                proc.kill()
                proc.wait()
            # 返回非 0 让外层识别 watchdog kill（OpenClaw 拉起会自动 Resume）
            return 137  # 128+9 = SIGKILL；这里实际上是 SIGTERM 返回 143+ 我们用 137 表示 watchdog 介入
        # 子进程是否已退出
        if proc.poll() is not None:
            # 把剩余 stdout 读空
            for line in proc.stdout:
                print(line, end="")
            break

    return proc.wait()


def _pick_list(data: dict) -> list[dict]:
    phase2 = data.get("phase2") or {}
    picks = phase2.get("top_picks") or phase2.get("top5") or []
    if picks:
        return picks[:5]
    ranked = phase2.get("ranked_candidates") or []
    ranked = [r for r in ranked if r.get("signal") in ("BUY", "WATCH")]
    ranked.sort(key=lambda r: (r.get("signal") != "BUY", -(float(r.get("confidence") or 0))))
    return ranked[:5]


def _summary(report_file: Path) -> str:
    data = json.loads(report_file.read_text(encoding="utf-8"))
    phase2 = data.get("phase2") or {}
    ranked = phase2.get("ranked_candidates") or []
    buy = [r for r in ranked if r.get("signal") == "BUY"]
    watch = [r for r in ranked if r.get("signal") == "WATCH"]
    avoid = [r for r in ranked if r.get("signal") == "AVOID"]
    lines = [
        f"每日选股工作流已完成：{data.get('date') or _today()}",
        f"候选股：{len(ranked)} 只；BUY：{len(buy)}；WATCH：{len(watch)}；AVOID：{len(avoid)}",
        "",
        "Top 5：",
    ]
    for i, item in enumerate(_pick_list(data), 1):
        stock = item.get("stock") or item.get("stock_code") or ""
        name = item.get("name") or item.get("stock_name") or ""
        signal = item.get("signal") or ""
        confidence = item.get("confidence") or item.get("final_score") or ""
        reason = (item.get("reason") or item.get("final_decision") or "").replace("\n", " ")
        lines.append(f"{i}. {name} {stock} | {signal} | 置信度 {confidence} | {reason[:120]}")
    lines.append("")
    lines.append(f"报告文件：{report_file}")
    return "\n".join(lines)


def main() -> int:
    with _workflow_singleton_lock() as acquired:
        if not acquired:
            return 0
        report_file = _report_path()
        if not _report_is_complete(report_file):
            if report_file.exists():
                print("今日已有报告但状态、Top5或配套产物不完整，继续修复而不是判定成功")
            resume_kind = _resume_state_kind()
            if resume_kind:
                label = "辩论 checkpoint" if resume_kind == "checkpoint" else "Phase 1 上下文"
                print(f"发现今日{label}，从已有状态续跑，避免重新启动完整选股工作流")
                env_model = os.environ.get("TA_DEFAULT_MODEL", "volcengine-plan/ark-code-latest")
                os.environ.setdefault("TA_DEFAULT_MODEL", env_model)
                rc = _run("resume", ["--model", env_model, "--resume"])
            else:
                env_model = os.environ.get("TA_DEFAULT_MODEL", "volcengine-plan/ark-code-latest")
                os.environ.setdefault("TA_DEFAULT_MODEL", env_model)
                rc = _run("normal", ["--model", env_model])
                if rc == 137:
                    # ★ 6-04 老板拍板：watchdog 主动 kill（10 分钟没进展）→ 立即走 resume，不需重跑 normal
                    print(f"[WATCHDOG] normal 被 watchdog kill（rc=137），检查 checkpoint 后决定是否 resume")
                if (rc != 0 or not _report_is_complete(report_file)) and _resume_state_available():
                    print(f"normal 未生成今日报告，开始从已保存状态续跑；normal rc={rc}")
                    rc = _run("resume", ["--model", env_model, "--resume"])
            if rc != 0 and not _report_is_complete(report_file):
                print(f"工作流失败，且未生成完整今日报告；续跑 rc={rc}", file=sys.stderr)
                return rc or 1

        if not _report_is_complete(report_file):
            print("工作流结束但今日报告仍不完整", file=sys.stderr)
            return 1
        print("\n" + _summary(report_file))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
