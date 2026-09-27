# StockLab MAX — Production Architecture

## Runtime modes

### Local / zero-config
- SQLite
- thread worker
- yfinance or local CSV
- browser UI
- no external database required

### Docker production foundation
- PostgreSQL 16
- FastAPI
- durable job metadata
- provider abstraction
- server-side LLM integration
- paper broker only

## Data boundary

```text
Provider Interface
 ├── Yahoo adapter (prototype / research)
 ├── Local CSV adapter
 └── Future licensed point-in-time adapter
          ↓
     Normalized OHLCV
          ↓
       Fingerprint
          ↓
     Research Engine
```

A production Indian-market deployment should use a licensed provider that preserves historical availability timestamps, delistings, corporate actions, fundamentals availability, and a survivorship-bias-controlled universe.

## Persistence boundary

```text
Repository boundary
       ├── SQLite (local)
       └── PostgreSQL (deployment)
```

Schema migrations are idempotent and tracked with `schema_meta`.

## Job boundary

The API creates a durable job record before starting a local worker thread. The worker updates status/result/error in `job_history`. This can later be replaced by Celery/RQ/Arq without changing the HTTP contract.

## AI boundary

The model is never the source of truth for quantitative metrics. Tool calls execute against StockLab APIs and return structured data; the model can explain or organize those outputs.

## Execution boundary

```text
Research signal
      ↓
Risk checks
      ↓
Paper broker

Live broker adapter is intentionally not enabled.
```

Before live execution, add an audited broker adapter, idempotency keys, order reconciliation, kill switch, limits, secrets management, and independent risk controls.
