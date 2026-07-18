# Changelog

All notable public changes are recorded here. This project follows Semantic
Versioning for repository releases.

## [0.2.0] - 2026-07-18

### Added

- Versioned data-routing contracts with source, freshness, fallback, and quality metadata.
- Deterministic technical snapshots for MA, RSI, MACD, KDJ, ATR, volume, and price position.
- Evidence-bound local knowledge rules and bidirectional historical edge overlays.
- Candidate, scoring-version, and debate-node aware checkpoint resume behavior.
- Central model route resolution with primary and two fallback slots.
- Intraday decision audit events and weekly buy-timing attribution.
- Expanded focused offline regression coverage.

### Changed

- Expanded candidate-pool construction, scoring, Top5 diversification, and report compaction.
- Improved money-flow provenance, stale-data handling, causal backtest guards, and provider fallback diagnostics.
- Local bridge addresses and OpenClaw paths now use environment configuration in the public build.

### Security

- Removed public defaults containing a private network address, local username, and personal provider profile names.
- The public build no longer reads the Codex desktop OAuth token; OpenAI API routes use `OPENAI_API_KEY`.

## [0.1.0] - 2026-07-03

- First sanitized public release.
- Added bilingual onboarding, dry-run defaults, community templates, and security guidance.

[0.2.0]: https://github.com/dazyhuang/daily-stock-workflow/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/dazyhuang/daily-stock-workflow/releases/tag/v0.1.0
