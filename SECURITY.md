# Security

Do not commit `.env`, broker/account exports, runtime reports, execution logs,
or generated files under `output/` and `logs/`.

This public version is intended to be configured through environment variables.
Use `.env.example` as a template and keep real API keys, webhook URLs, account
identifiers, and trading records local.
