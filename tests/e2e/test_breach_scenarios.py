"""End-to-end breach scenarios. Assumes `make all` is up and pipeline is running."""
import json
import os
import time
import uuid
from decimal import Decimal
from io import BytesIO
from pathlib import Path

import fastavro
import pytest
import requests
from confluent_kafka import Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
REGISTRY = os.environ.get("SCHEMA_REGISTRY", "http://localhost:8081")
SCHEMAS_DIR = Path(__file__).resolve().parent.parent.parent / "schemas"


_SCHEMA_TO_TOPIC = {
    "trade": "trades",
    "enriched_trade": "enriched-trades",
    "client_limit": "client-limits",
    "position": "positions",
    "breach": "breaches",
}


def _avro(schema_name, payload):
    schema = fastavro.parse_schema(json.loads((SCHEMAS_DIR / f"{schema_name}.avsc").read_text()))
    subject = f"{_SCHEMA_TO_TOPIC[schema_name]}-value"
    sid = requests.get(f"{REGISTRY}/subjects/{subject}/versions/latest").json()["id"]
    out = BytesIO()
    out.write(b"\x00")
    out.write(sid.to_bytes(4, "big"))
    fastavro.schemaless_writer(out, schema, payload)
    return out.getvalue()


@pytest.fixture
def producer():
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    yield p
    p.flush(10)


def _send_trade(producer, client, symbol, qty, price, side="BUY"):
    payload = {
        "trade_id": str(uuid.uuid4()),
        "order_id": str(uuid.uuid4()),
        "client_id": client,
        "symbol": symbol,
        "side": side,
        "quantity": qty,
        "price": Decimal(str(price)),
        "currency": "USD",
        "venue": "X",
        "event_ts": int(time.time() * 1000),
    }
    producer.produce("trades", key=client.encode(), value=_avro("trade", payload))
    producer.poll(0)


def _set_limit(producer, client, max_gross, max_order, max_tpm):
    payload = {
        "client_id": client,
        "max_gross_notional": Decimal(str(max_gross)),
        "max_single_order_notional": Decimal(str(max_order)),
        "max_trades_per_minute": max_tpm,
        "updated_ts": int(time.time() * 1000),
    }
    producer.produce("client-limits", key=client.encode(), value=_avro("client_limit", payload))
    producer.flush(5)


def test_single_order_notional_breach(producer, query, waitfor):
    cid = "TC_SINGLE_" + uuid.uuid4().hex[:6]
    _set_limit(producer, cid, max_gross=10_000_000, max_order=1_000, max_tpm=999)
    time.sleep(2)
    _send_trade(producer, cid, "AAPL", qty=100, price="50.00")
    assert waitfor(
        lambda: any(
            r["breach_type"] == "SINGLE_ORDER_NOTIONAL"
            for r in query(f"SELECT breach_type FROM breaches WHERE client_id = '{cid}'")
        ),
        timeout=20,
    )


def test_gross_notional_breach(producer, query, waitfor):
    cid = "TC_GROSS_" + uuid.uuid4().hex[:6]
    _set_limit(producer, cid, max_gross=1_000, max_order=10_000_000, max_tpm=999)
    time.sleep(2)
    for _ in range(3):
        _send_trade(producer, cid, "AAPL", qty=10, price="100.00")
    producer.flush(2)
    time.sleep(3)
    _send_trade(producer, cid, "MSFT", qty=1, price="1.00")
    assert waitfor(
        lambda: any(
            r["breach_type"] == "GROSS_NOTIONAL"
            for r in query(f"SELECT breach_type FROM breaches WHERE client_id = '{cid}'")
        ),
        timeout=30,
    )


def test_velocity_breach(producer, query, waitfor):
    cid = "TC_VEL_" + uuid.uuid4().hex[:6]
    _set_limit(producer, cid, max_gross=10_000_000, max_order=10_000_000, max_tpm=3)
    time.sleep(2)
    for _ in range(5):
        _send_trade(producer, cid, "AAPL", qty=1, price="1.00")
    assert waitfor(
        lambda: any(
            r["breach_type"] == "TRADE_VELOCITY"
            for r in query(f"SELECT breach_type FROM breaches WHERE client_id = '{cid}'")
        ),
        timeout=20,
    )
