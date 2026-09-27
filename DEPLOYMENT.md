# StockLab MAX — Deployment

## Docker + PostgreSQL

```bash
cp .env.example .env
docker compose up --build
```

The Compose stack starts PostgreSQL first, waits for its health check, then starts StockLab. The API will run at `http://localhost:8000`.

## Local SQLite

```bash
pip install -r requirements.txt
uvicorn backend:app --reload
```

No database setup is required. `experiments.db` is created automatically.

## PostgreSQL outside Docker

Set:

```text
STOCKLAB_DATABASE_URL=postgresql://user:password@host:5432/stocklab
```

The application runs idempotent migrations on startup.

## Production environment checklist

- Set a strong `STOCKLAB_API_KEY` or place the API behind an identity-aware reverse proxy.
- Restrict `STOCKLAB_CORS_ORIGINS`; do not use `*` in production.
- Store `LLM_API_KEY` only on the server.
- Use HTTPS.
- Back up PostgreSQL.
- Mount persistent data storage for imported datasets.
- Replace yfinance with a licensed point-in-time provider before institutional research.
- Add external queue workers before high-concurrency research workloads.
- Keep live broker credentials isolated from the research service.
