# Contributing

Thanks for considering a contribution. This project is research tooling, so the
main standard is practical reproducibility: changes should be easy to run in
dry-run mode and should not require private account data.

## Good First Contributions

- Improve setup docs for a clean machine.
- Add provider-free examples and fixtures.
- Reduce hard dependencies on local market-data bridges.
- Improve tests around fallback behavior.
- Add English or Chinese documentation for existing scripts.

## Development Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Keep `DRY_RUN=1` unless you are intentionally testing your own private trading
environment.

## Pull Request Checklist

- Do not commit `.env`, logs, runtime reports, account exports, or broker data.
- Keep changes scoped to the issue or feature being addressed.
- Update README or docs when behavior changes.
- Add or update tests when changing fallback logic, scoring, execution helpers,
  or file formats.
- Run at least a syntax check before opening a PR:

```bash
python3 -m compileall -q .
```

## Reporting Issues

When reporting a bug, include:

- Command you ran.
- Expected result.
- Actual result.
- Relevant sanitized logs or traceback.
- Whether external providers were configured.

Do not paste API keys, account identifiers, webhook URLs, or real trading
records into issues.
