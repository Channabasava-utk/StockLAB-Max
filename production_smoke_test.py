"""Offline architecture smoke test for StockLab MAX.
Stubs yfinance so imports and core production boundaries can be validated without network access.
"""
import sys, types, tempfile, os
import pandas as pd

fake = types.ModuleType("yfinance")
fake.download = lambda *a, **k: pd.DataFrame()
fake.Ticker = lambda s: object()
sys.modules["yfinance"] = fake
sys.path.insert(0, ".")

import backend

assert backend.APP_VERSION == "7.0.0-FINAL"
assert backend.app.version == "7.0.0-FINAL"
manifest = backend.system_manifest()
assert manifest["execution"] == {"paper": True, "broker": False, "live_automation": False}
assert "regime" in manifest["research_layers"]
plan = backend.build_research_agent_plan("high quality growth companies in an uptrend", "RELIANCE.NS", "5y")
assert plan["agent_mode"] == "local-explainable-planner"
assert len(plan["workflow"]) >= 10
assert backend.code_fingerprint()

# Verify schema migrations are idempotent in a fresh SQLite database.
from database import migrate, connect, info
fd, path = tempfile.mkstemp(suffix=".db"); os.close(fd)
migrate(path); migrate(path)
c = connect(path)
cols = {r[1] for r in c.execute("PRAGMA table_info(job_history)").fetchall()}
assert {"finished_at", "payload", "result", "error"}.issubset(cols)
c.close(); os.remove(path)

# Verify durable job store round-trip.
from job_store import create, update, get as get_job, list_jobs
jid = create("smoke", {"ok": True}); update(jid, "completed", {"answer": 42})
assert get_job(jid)["result"]["answer"] == 42
assert any(x["id"] == jid for x in list_jobs(20))
print("StockLab MAX v7 final architecture smoke test: PASS")
