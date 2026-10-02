# StockLab MAX — FINAL FINAL

StockLab MAX is an India-oriented quantitative research workstation prototype combining market data, fundamentals, technical research, screening, backtesting, validation, regime analysis, ML baselines, portfolio analytics, experiment memory, paper trading, and a tool-bounded research agent.

## What is included

### Research engine
- Historical OHLCV data through provider adapters
- Technical indicators: SMA, EMA, RSI, Bollinger Bands, ATR, volume
- Strategy builder and historical backtesting
- Transaction cost + slippage modeling
- Equity curves versus benchmark
- Automated parameter discovery
- Chronological train/test validation
- Walk-forward re-optimization
- Robustness heatmaps
- Strategy-return Monte Carlo bootstrap
- Regime detection and strategy attribution
- Research Health diagnostics
- Factor diagnostics
- ML baseline with chronological evaluation

### Fundamental and market research
- P/E, forward P/E, P/B
- ROE, ROA, margins, debt/equity
- Revenue and earnings growth
- Quant screener
- Technical filters
- Event context
- Company metadata

### Research memory
- SQLite default database
- PostgreSQL deployment path
- Experiment Vault
- Thesis Vault
- Research runs
- Dataset fingerprints
- Code fingerprints
- Data provenance
- Portable experiment JSON
- Markdown research dossier export
- Audit log

### AI research layer
- Natural-language hypothesis formalization
- Deterministic local research planner
- Optional OpenAI-compatible Responses API integration
- Whitelisted research tools
- Server-side LLM secret handling
- Tool execution audit events
- No arbitrary code execution
- No live broker actions from the agent

### Paper trading
- Virtual cash
- BUY/SELL validation
- Holdings
- Average cost
- Realized and unrealized P&L
- Order journal
- Broker abstraction
- Live broker boundary deliberately disabled

### Platform engineering
- FastAPI backend
- Browser frontend
- API-key/session boundary
- CORS configuration
- Request IDs
- Security headers
- Lightweight local rate limiting
- Liveness/readiness endpoints
- Durable research jobs
- SQLite + PostgreSQL repository path
- Docker + Docker Compose
- GitHub Actions CI
- Makefile
- Architecture, deployment and security docs

## Quick start

### Local development

```bash
pip install -r requirements.txt
uvicorn backend:app --reload
```

Open `index.html` in your browser.

### Docker + PostgreSQL

```bash
cp .env.example .env
docker compose up --build
```

The default Compose deployment uses PostgreSQL 16 and the same application repository interface used by SQLite.

### Tests

```bash
python production_smoke_test.py
python smoke_test.py
```

or:

```bash
make test
```

## Optional LLM configuration

Set these on the server, never in browser code:

```text
LLM_API_KEY=...
LLM_MODEL=gpt-5.6-luna
LLM_BASE_URL=https://api.openai.com/v1
```

If no key is configured, the local deterministic research planner remains available.

## Optional API protection

```text
STOCKLAB_API_KEY=your-secret
STOCKLAB_CORS_ORIGINS=http://localhost:8000
```

## Data-provider reality check

The default yfinance adapter is suitable for prototyping and exploration. It is **not** an institutional point-in-time data solution. Production research should replace it with a licensed provider that supplies historical availability timestamps, corporate actions, delisted securities, restatements and survivorship-bias-controlled universes.

The provider abstraction is designed so that replacement does not require rewriting the research engine.

## Research philosophy

StockLab is a research system, not an automatic investment-advice engine. Backtests are historical simulations. ML probabilities are model outputs. Monte Carlo paths are stress scenarios. Factor diagnostics are descriptive. Paper trading is simulated execution. Always inspect assumptions, data quality, costs, drawdowns, trade counts, out-of-sample behavior and regime dependence.

## Production roadmap after this release

The codebase is intentionally structured for a future migration to:

1. Licensed point-in-time Indian market/fundamental/event data
2. Managed PostgreSQL
3. Object storage for research artifacts
4. Redis + Celery/RQ/Arq or another external queue
5. Full RBAC and external identity provider
6. Observability and centralized logs
7. Sandboxed agent tool execution
8. Broker sandbox integration
9. Reconciliation and risk gates before any live execution

Those are infrastructure upgrades, not missing “features” in the research prototype.
