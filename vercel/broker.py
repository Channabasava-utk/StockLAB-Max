"""Broker boundary. Live execution is intentionally disabled in this build."""
from dataclasses import dataclass

@dataclass
class BrokerOrder:
    symbol: str
    side: str
    qty: float
    price: float | None = None
    mode: str = "paper"

class BrokerAdapter:
    name = "abstract"
    live_enabled = False
    def place(self, order: BrokerOrder): raise NotImplementedError

class PaperBroker(BrokerAdapter):
    name = "paper"
    live_enabled = False
    def place(self, order):
        return {"accepted": True, "mode":"paper", "symbol":order.symbol.upper(), "side":order.side.upper(), "qty":float(order.qty), "price":order.price}

class LiveBrokerBoundary(BrokerAdapter):
    name = "live-boundary"
    live_enabled = False
    def place(self, order):
        raise RuntimeError("Live broker execution is disabled. Implement and audit a broker adapter before enabling it.")
