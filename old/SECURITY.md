# Security and deployment notes

- Never commit `.env`, API keys, broker tokens, or database files.
- `STOCKLAB_API_KEY` is a lightweight local/server boundary, not a complete identity system.
- The LLM provider key must remain server-side.
- Paper trading is intentionally isolated from broker/live execution.
- For multi-user deployment, add an identity provider, role-based authorization, secret manager, database isolation, HTTPS, rate limiting and audit logging before exposing the service publicly.
- Treat imported CSV files as untrusted input and validate schema/size before ingestion.
