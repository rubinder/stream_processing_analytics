"""Synthetic equity trade factory. Deterministic given a seed (except event_ts)."""
from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence


@dataclass
class TradeFactory:
    symbols: Sequence[str]
    client_count: int
    seed: int = 0
    venues: Sequence[str] = ("NASDAQ", "NYSE", "ARCA")

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)
        self._client_ids = [f"C{i:04d}" for i in range(self.client_count)]
        self._open_orders: dict[str, str] = {}  # client_id -> active order_id
        # Per-symbol price band, fixed per seed
        self._price_band = {
            s: Decimal(f"{self._rng.uniform(20, 800):.4f}") for s in self.symbols
        }

    def next_trade(self) -> dict:
        client_id = self._rng.choice(self._client_ids)
        symbol = self._rng.choice(self.symbols)
        side = self._rng.choice(("BUY", "SELL"))
        quantity = self._rng.randint(1, 500)
        # Walk price within ±2% of band centre
        base = self._price_band[symbol]
        jitter = Decimal(f"{self._rng.uniform(-0.02, 0.02):.4f}")
        price = (base * (Decimal("1") + jitter)).quantize(Decimal("0.0001"))
        venue = self._rng.choice(self.venues)
        # Reuse order_id occasionally → multi-fill orders
        if client_id in self._open_orders and self._rng.random() < 0.3:
            order_id = self._open_orders[client_id]
        else:
            order_id = str(uuid.UUID(int=self._rng.getrandbits(128)))
            self._open_orders[client_id] = order_id
        return {
            "trade_id": str(uuid.UUID(int=self._rng.getrandbits(128))),
            "order_id": order_id,
            "client_id": client_id,
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "currency": "USD",
            "venue": venue,
            "event_ts": int(time.time() * 1000),
        }
