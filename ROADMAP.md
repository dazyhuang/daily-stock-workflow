# Roadmap

This roadmap favors making the project easier to run, inspect, and extend
without private infrastructure.

## Completed in v0.1.x: Public Usability

- Improve README and bilingual onboarding.
- Add dry-run examples that do not require broker credentials.
- Document key scripts and their expected inputs/outputs.
- Add issue templates and contribution guidelines.

## Completed in v0.2.0: Traceable Workflow Core

- Add source/freshness contracts and deterministic market snapshots.
- Add evidence-bound knowledge rules and historical edge overlays.
- Add version-aware checkpoint and debate-node resume behavior.
- Add centralized model routing and fallback metadata.
- Add expanded focused offline regression coverage.

## Next in v0.2.x: Reproducible Examples

- Add sanitized sample reports under `examples/`.
- Expand provider-free fixtures for candidate scoring and attribution.
- Split optional local bridge integrations behind clearer interfaces.
- Document common failure modes for data providers and LLM providers.

## Planned for v0.3.x: Packaging and Automation

- Add a simple CLI entry point.
- Add CI checks for syntax and focused tests.
- Add configuration validation.
- Publish a small demo workflow that can run fully offline.

## Non-Goals

- This project will not ship real API keys, account data, or broker-specific
  production credentials.
- This project will not provide investment advice or stock recommendations.
- This project will not make live trading the default behavior.
