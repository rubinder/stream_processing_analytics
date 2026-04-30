"""CLI: streams synthetic trades to the `trades` Kafka topic in Avro format."""
from __future__ import annotations

import json
import time
from io import BytesIO
from pathlib import Path

import click
import fastavro
import requests
from confluent_kafka import Producer

from trade_gen.generator import TradeFactory

SCHEMAS_DIR = Path(__file__).resolve().parent.parent.parent / "schemas"
SECTORS_PATH = Path(__file__).resolve().parent / "sectors.json"


def _load_schema(name: str) -> dict:
    return json.loads((SCHEMAS_DIR / f"{name}.avsc").read_text())


def _registered_schema_id(registry_url: str, subject: str) -> int:
    r = requests.get(f"{registry_url}/subjects/{subject}/versions/latest", timeout=5)
    r.raise_for_status()
    return r.json()["id"]


def _avro_encode(schema: dict, schema_id: int, payload: dict) -> bytes:
    """Confluent wire format: magic byte (0x00) + 4-byte schema id + Avro body."""
    out = BytesIO()
    out.write(b"\x00")
    out.write(schema_id.to_bytes(4, "big"))
    fastavro.schemaless_writer(out, schema, payload)
    return out.getvalue()


@click.command()
@click.option("--bootstrap", default="localhost:9094")
@click.option("--registry", default="http://localhost:8081")
@click.option("--rate", default=20, help="Trades per second")
@click.option("--clients", default=50)
@click.option("--seed", default=42)
@click.option("--duration", default=0, help="0 = run forever")
def main(bootstrap, registry, rate, clients, seed, duration):
    schema = _load_schema("trade")
    schema_id = _registered_schema_id(registry, "trades-value")
    sectors = json.loads(SECTORS_PATH.read_text())
    factory = TradeFactory(symbols=list(sectors.keys()), client_count=clients, seed=seed)

    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 5, "compression.type": "lz4"})
    interval = 1.0 / rate
    start = time.time()
    n = 0
    try:
        while True:
            t = factory.next_trade()
            payload = dict(t)
            # Decimal → bytes(decimal) handled by fastavro via the schema's logicalType
            value = _avro_encode(schema, schema_id, payload)
            producer.produce("trades", key=t["client_id"].encode(), value=value)
            n += 1
            if n % 100 == 0:
                producer.poll(0)
                click.echo(f"sent {n} trades", err=True)
            if duration and time.time() - start > duration:
                break
            time.sleep(interval)
    finally:
        producer.flush(10)
        click.echo(f"flushed; total={n}", err=True)


if __name__ == "__main__":
    main()
