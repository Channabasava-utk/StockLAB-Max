# StockLab MAX — Vercel Deployment

This version is prepared for Vercel's current FastAPI/Python runtime.

## 1. Push this folder to GitHub

The GitHub repository root must contain:

- `backend.py`
- `index.html`
- `requirements.txt`
- `database.py`
- `worker.py`
- the other StockLab files

Do **not** select a parent directory above `stocklab_final_all` as the Vercel Root Directory unless you move these files up first.

## 2. Import into Vercel

In Vercel:

1. **Add New → Project**
2. Import your GitHub repository.
3. Set **Root Directory** to the folder containing `backend.py` and `index.html` if your repository has an outer wrapper folder.
4. Leave Build Command and Output Directory at their defaults.
5. Deploy.

Vercel detects FastAPI applications through its Python runtime. The existing `index.html` is served by the `/` FastAPI route added for this deployment.

## 3. Add environment variables

For a real deployment, connect a managed PostgreSQL database (Neon is available through the Vercel Marketplace) and add:

```text
STOCKLAB_DATABASE_URL=<your PostgreSQL connection string>
STOCKLAB_API_KEY=<long random secret>
STOCKLAB_CORS_ORIGINS=https://<your-vercel-domain>
STOCKLAB_DATA_PROVIDER=yfinance
STOCKLAB_CACHE=1
STOCKLAB_WORKER_MODE=thread
STOCKLAB_SESSION_TTL_HOURS=24
```

Optional AI variables:

```text
LLM_API_KEY=<server-side key>
LLM_MODEL=gpt-5.6-luna
LLM_BASE_URL=https://api.openai.com/v1
```

Never put `LLM_API_KEY`, database credentials, or `STOCKLAB_API_KEY` in `index.html` or browser JavaScript.

## 4. Important persistence note

StockLab uses SQLite locally, but Vercel's function filesystem is not durable. This deployment therefore uses `/tmp` only as a transient fallback. For saved experiments, theses, paper orders, users, and sessions that must survive deployments/instances, set `STOCKLAB_DATABASE_URL` to PostgreSQL.

The application already supports PostgreSQL through `psycopg`.

## 5. Test after deployment

Open:

```text
https://YOUR-DOMAIN.vercel.app/
https://YOUR-DOMAIN.vercel.app/api/health
https://YOUR-DOMAIN.vercel.app/docs
```

The homepage should show the StockLab MAX interface. The health endpoint should return the application status.

Then test a simple action such as **Strategy Lab → Run Backtest**.

## 6. If Vercel shows a Python build error

Make sure the Vercel Root Directory is the directory containing:

```text
backend.py
requirements.txt
index.html
```

Do not use the Docker command as the Vercel build command. Docker remains useful for the existing local/PostgreSQL setup.

## 7. GitHub → Vercel automatic deployments

Once connected, pushes to the selected Git branch trigger Vercel deployments automatically. Use the Vercel dashboard's deployment logs if a build fails.
