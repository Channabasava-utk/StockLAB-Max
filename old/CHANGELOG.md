# StockLab MAX — FINAL FINAL changelog

## 7.0.0-FINAL

- Consolidated the entire v5/v6 research platform into one release.
- Added production security headers and local rate limiting.
- Added liveness/readiness health endpoints.
- Added durable job status endpoints.
- Added research-agent whitelisted tool execution.
- Added research dossier Markdown export.
- Added audit-log storage and API.
- Added schema version 4 with audit_log.
- Corrected Local CSV provenance: it is not automatically marked point-in-time-ready.
- Added Makefile, GitHub Actions CI, .gitignore and MIT license.
- Refreshed final architecture and deployment documentation.
- Preserved SQLite zero-config development and PostgreSQL deployment path.
- Preserved optional LLM integration with deterministic local fallback.
