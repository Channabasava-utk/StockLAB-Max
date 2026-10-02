"""Structured research-agent tool catalog.

The LLM should select tools and provide JSON arguments; it must not invent
metrics. Each tool maps to an auditable backend operation.
"""
TOOLS = [
    {"name":"get_quote","description":"Get the latest available quote snapshot for a symbol.","input_schema":{"type":"object","properties":{"symbol":{"type":"string"}},"required":["symbol"]}},
    {"name":"backtest","description":"Run the configured historical strategy backtest.","input_schema":{"type":"object","properties":{"symbol":{"type":"string"},"period":{"type":"string"}},"required":["symbol"]}},
    {"name":"regime_attribution","description":"Measure historical strategy behavior grouped by detected market regime.","input_schema":{"type":"object","properties":{"symbol":{"type":"string"},"period":{"type":"string"}},"required":["symbol"]}},
    {"name":"research_health","description":"Run anti-overfitting and out-of-sample diagnostics.","input_schema":{"type":"object","properties":{"symbol":{"type":"string"},"period":{"type":"string"}},"required":["symbol"]}},
    {"name":"data_health","description":"Inspect data completeness, gaps and provenance.","input_schema":{"type":"object","properties":{"symbol":{"type":"string"},"period":{"type":"string"}},"required":["symbol"]}},
]
