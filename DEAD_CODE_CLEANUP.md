# Dead Code Cleanup Notes

Last updated: 2026-06-12

## Active Entrypoints

Keep these until the corresponding automation is changed:

- `run_daily_stock_workflow_stable.py` -> `workflow.py`
- `intraday_executor.py`
- `intraday_monitor_realtime.py`
- `weekly_review.py` -> `weekly_strategy/run_weekly_strategy_debate.py`
- `position_debate/run_position_debate.py`
- `execute_debate_result.py`

## Removed In This Pass

These had no current code/test/automation references and were either backup snapshots or one-off local repair/debug scripts:

- `fix_workflow.py`
- `run_debug_test.py`
- `workflow.py.bak_20260509`
- `intraday_executor.py.bak_20260601`
- `llm_scorer.py.backup_20260416_1119`
- `llm_scorer.py.bak_before_refactor`
- `weekly_review.py.bak.20260418`
- `weekly_debate_result.json.bak16`
- `params.json.bak.20260514170335`
- `intraday_monitor_watchdog.py`

The working tree also already had these obsolete files removed before this pass, and they should stay removed unless a specific historical replay needs them:

- `_proxy_patch.py`
- `_run_no_proxy.sh`
- `build_financial_cache.py`
- `weekly_debate_catchup.py`
- `weekly_review_debate.py`

## Deferred Cleanup

These are not source dead code, but the repository currently tracks runtime/generated files. Keep the distinction explicit so operational records are not accidentally removed:

- Done in the second cleanup pass: untracked Python bytecode caches (`__pycache__/`, `*.pyc`) from Git while leaving local files ignored.
- Done in the second cleanup pass: untracked log files (`logs/`, `stock_selection_debate/logs/`, `weekly_strategy/logs/`, `output/*.log`) from Git while leaving local files ignored where present.
- Done in the third cleanup pass: untracked the rest of `output/` from Git while leaving local files in place. This includes historical reports, checkpoints, mx-data dumps, xuangu dumps, caches, and `trades.json`.
- Added `archive_runtime_outputs.py` for ongoing local retention. It defaults to dry-run mode, keeps recent dated artifacts, and protects `trades.json`, latest state files, lock files, and cache dirs unless explicitly overridden.
- old `knowledge-base/stock_debate_*.json` / weekly debate snapshots if they are no longer used for retrieval

## Rules For Future Cleanup

- Delete one-off repair scripts once their changes are merged into source.
- Do not add backup copies of source files; use Git history.
- Keep trading entrypoints until LaunchAgent/cron and README no longer reference them.
- Treat output, logs, checkpoints, and trading records as runtime data, not source code.
- Archive local runtime history with `python3 archive_runtime_outputs.py` first, then add `--execute` only after reviewing the dry-run list.
