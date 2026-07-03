# Demo Walkthrough

This demo is intentionally dry-run first. It shows the expected shape of the
workflow without requiring private account data.

## 1. Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
printf '\nDRY_RUN=1\n' >> .env
```

## 2. Run the Daily Workflow

```bash
python3 workflow.py
```

Expected output files are written under `output/` when the configured data
providers are available. `output/` is intentionally ignored by Git.

## 3. Inspect Money-Flow Quality

```bash
python3 check_money_flow_quality.py --days 10
```

Use this to understand whether missing money-flow fields came from unavailable
data providers or from local logic regressions.

## 4. Review Top5 Attribution

```bash
python3 top5_review_attribution.py --days 10
```

This summarizes recent Top5 candidates and classifies attribution patterns.
Provider failures should be treated separately from model or workflow logic
failures.

## 5. Optional Intraday Helpers

```bash
python3 intraday_executor.py --mode=status
python3 intraday_executor.py --mode=buy-timing
python3 intraday_executor.py --mode=monitor
```

Keep `DRY_RUN=1` unless you have reviewed the execution code and configured
your own private broker bridge.

## Workflow Shape

```mermaid
flowchart TD
    A[Collect context] --> B[Build candidates]
    B --> C[LLM score and debate]
    C --> D[Backtest]
    D --> E[Daily report]
    E --> F[Dry-run intraday helper]
    F --> G[Weekly review]
```
