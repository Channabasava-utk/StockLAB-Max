"""Offline smoke test for StockLab MAX v5.
Uses a tiny yfinance stub so the core app can be checked without network access.
Run after installing requirements with: python smoke_test.py
"""
import sys, types
import pandas as pd

fake = types.ModuleType("yfinance")
fake.download = lambda *a, **k: pd.DataFrame()
fake.Ticker = lambda s: object()
sys.modules["yfinance"] = fake
sys.path.insert(0, ".")

import backend

assert backend.APP_VERSION == "7.0.0-FINAL"
assert backend.app.version == "7.0.0-FINAL"
plan = backend.build_research_agent_plan(
    "high quality growth companies in an uptrend",
    "RELIANCE.NS", "5y"
)
assert plan["agent_mode"] == "local-explainable-planner"
assert len(plan["workflow"]) >= 10
manifest = backend.system_manifest()
assert manifest["execution"] == {"paper": True, "broker": False, "live_automation": False}
assert "regime" in manifest["research_layers"]
print("StockLab MAX v7 offline smoke test: PASS")
