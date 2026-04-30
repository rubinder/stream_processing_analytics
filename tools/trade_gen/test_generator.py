import json
from decimal import Decimal
from pathlib import Path

from trade_gen.generator import TradeFactory


def test_factory_produces_valid_trade_with_required_fields():
    sectors = json.loads((Path(__file__).parent / "sectors.json").read_text())
    factory = TradeFactory(symbols=list(sectors.keys()), client_count=10, seed=42)

    trade = factory.next_trade()

    assert trade["trade_id"]
    assert trade["order_id"]
    assert trade["client_id"].startswith("C")
    assert trade["symbol"] in sectors
    assert trade["side"] in ("BUY", "SELL")
    assert trade["quantity"] > 0
    assert isinstance(trade["price"], Decimal)
    assert trade["price"] > 0
    assert trade["currency"] == "USD"
    assert trade["venue"]
    assert trade["event_ts"] > 0


def test_factory_is_deterministic_with_seed():
    sectors = json.loads((Path(__file__).parent / "sectors.json").read_text())
    f1 = TradeFactory(symbols=list(sectors.keys()), client_count=10, seed=42)
    f2 = TradeFactory(symbols=list(sectors.keys()), client_count=10, seed=42)

    # event_ts is wall-clock based; everything else should match
    t1 = f1.next_trade(); t2 = f2.next_trade()
    for k in ("order_id", "client_id", "symbol", "side", "quantity", "price", "venue"):
        assert t1[k] == t2[k], f"diverged at {k}"
