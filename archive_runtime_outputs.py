#!/usr/bin/env python3
"""Archive old runtime artifacts out of output/ without touching trade state.

Default mode is a dry run. Add --execute to move eligible files into
runtime_archive/output/ while preserving their relative paths.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = BASE_DIR / "output"
DEFAULT_ARCHIVE_DIR = BASE_DIR / "runtime_archive" / "output"

DATE_RE = re.compile(r"(20\d{6})")

PROTECTED_NAMES = {
    "trades.json",
    "debate_checkpoint.json",
    "phase1_context.json",
    "weekly_review_latest.json",
    "weekly_debate_result_latest.json",
    "scored_progress.json",
}

PROTECTED_DIRS = {
    "data_cache",
    "fundamental_cache",
}

GENERATED_DIRS = {
    "mx_data",
    "mx_search",
    "xuangu",
    "xuangu_test",
    "debug_test",
}


@dataclass(frozen=True)
class ArchiveCandidate:
    path: Path
    relative_path: Path
    reason: str
    artifact_date: Optional[date]


def _parse_date(text: str) -> Optional[date]:
    match = DATE_RE.search(text)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def _file_mtime_date(path: Path) -> date:
    return datetime.fromtimestamp(path.stat().st_mtime).date()


def _is_lock_or_pid(relative_path: Path) -> bool:
    parts = relative_path.parts
    if any(part.endswith(".lockdir") for part in parts):
        return True
    return relative_path.name.endswith(".pid") or relative_path.name.endswith(".lock")


def _is_protected(relative_path: Path, include_cache: bool) -> bool:
    if relative_path.name in PROTECTED_NAMES:
        return True
    if _is_lock_or_pid(relative_path):
        return True
    if not include_cache and relative_path.parts and relative_path.parts[0] in PROTECTED_DIRS:
        return True
    return False


def iter_archive_candidates(
    output_dir: Path,
    *,
    today: date,
    keep_days: int,
    include_cache: bool,
    include_trades: bool,
) -> Iterable[ArchiveCandidate]:
    cutoff = today - timedelta(days=keep_days)
    for path in sorted(output_dir.rglob("*")):
        if not path.is_file():
            continue
        relative_path = path.relative_to(output_dir)
        if relative_path.name == "trades.json" and include_trades:
            artifact_date = _file_mtime_date(path)
            if artifact_date < cutoff:
                yield ArchiveCandidate(path, relative_path, "trade-state-explicit", artifact_date)
            continue
        if _is_protected(relative_path, include_cache=include_cache):
            continue

        artifact_date = _parse_date(relative_path.as_posix())
        if artifact_date:
            if artifact_date < cutoff:
                yield ArchiveCandidate(path, relative_path, "dated-artifact", artifact_date)
            continue

        top_dir = relative_path.parts[0] if relative_path.parts else ""
        if top_dir in GENERATED_DIRS:
            mtime_date = _file_mtime_date(path)
            if mtime_date < cutoff:
                yield ArchiveCandidate(path, relative_path, f"generated-dir:{top_dir}", mtime_date)


def _unique_destination(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    index = 1
    while True:
        candidate = parent / f"{stem}.{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def archive_candidates(
    candidates: Iterable[ArchiveCandidate],
    output_dir: Path,
    archive_dir: Path,
    *,
    execute: bool,
) -> int:
    moved = 0
    for item in candidates:
        destination = _unique_destination(archive_dir / item.relative_path)
        print(f"{'MOVE' if execute else 'DRY '} {item.relative_path} -> {destination.relative_to(archive_dir.parent)} [{item.reason}]")
        if execute:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(os.fspath(item.path), os.fspath(destination))
            moved += 1
    if execute:
        _remove_empty_dirs(output_dir)
    return moved


def _remove_empty_dirs(root: Path) -> None:
    for path in sorted(root.rglob("*"), key=lambda p: len(p.parts), reverse=True):
        if path.is_dir():
            try:
                path.rmdir()
            except OSError:
                pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Archive old daily-stock-workflow runtime output files.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_ARCHIVE_DIR)
    parser.add_argument("--keep-days", type=int, default=30, help="Keep dated artifacts newer than this many days.")
    parser.add_argument("--today", default=date.today().strftime("%Y-%m-%d"), help="Override current date for repeatable dry runs.")
    parser.add_argument("--include-cache", action="store_true", help="Allow data_cache/ and fundamental_cache/ to be archived.")
    parser.add_argument("--include-trades", action="store_true", help="Allow trades.json to be archived based on mtime. Off by default.")
    parser.add_argument("--execute", action="store_true", help="Move files. Without this flag only prints a dry run.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    today = datetime.strptime(args.today, "%Y-%m-%d").date()
    output_dir = args.output_dir.resolve()
    archive_dir = args.archive_dir.resolve()

    if not output_dir.exists():
        print(f"Nothing to archive: {output_dir} does not exist")
        return 0

    candidates = list(
        iter_archive_candidates(
            output_dir,
            today=today,
            keep_days=args.keep_days,
            include_cache=args.include_cache,
            include_trades=args.include_trades,
        )
    )
    by_reason = Counter(item.reason for item in candidates)
    print(
        f"archive candidates={len(candidates)} keep_days={args.keep_days} "
        f"cutoff={today - timedelta(days=args.keep_days)} execute={args.execute}"
    )
    for reason, count in sorted(by_reason.items()):
        print(f"  {reason}: {count}")
    moved = archive_candidates(candidates, output_dir, archive_dir, execute=args.execute)
    if args.execute:
        print(f"moved={moved} archive_dir={archive_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
