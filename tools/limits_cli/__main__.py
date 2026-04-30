"""CLI: publishes ClientLimit records to the `client-limits` compacted topic."""
from __future__ import annotations

import json
import time
from decimal import Decimal
from io import BytesIO
from pathlib import Path

import click
import fastavro
import requests
from confluent_kafka import Producer

SCHEMAS_DIR = Path(__file__).resolve().parent.parent.parent / "schemas"


def _avro_encode(schema, schema_id, payload):
    out = BytesIO()
    out.write(b"\x00")
    out.write(schema_id.to_bytes(4, "big"))
    fastavro.schemaless_writer(out, schema, payload)
    return out.getvalue()


@click.group()
@click.option("--bootstrap", default="localhost:9094")
@click.option("--registry", default="http://localhost:8081")
@click.pass_context
def cli(ctx, bootstrap, registry):
    ctx.obj = {
        "bootstrap": bootstrap,
        "registry": registry,
    }


@cli.command()
@click.option("--client-id", required=True)
@click.option("--max-gross", type=Decimal, default=Decimal("1000000"))
@click.option("--max-order", type=Decimal, default=Decimal("100000"))
@click.option("--max-tpm",   type=int,     default=30)
@click.pass_obj
def set(obj, client_id, max_gross, max_order, max_tpm):
    """Publish/replace one client's limits."""
    schema = json.loads((SCHEMAS_DIR / "client_limit.avsc").read_text())
    sid = requests.get(f"{obj['registry']}/subjects/client-limits-value/versions/latest").json()["id"]
    producer = Producer({"bootstrap.servers": obj["bootstrap"]})

    payload = {
        "client_id": client_id,
        "max_gross_notional": max_gross,
        "max_single_order_notional": max_order,
        "max_trades_per_minute": max_tpm,
        "updated_ts": int(time.time() * 1000),
    }
    producer.produce(
        "client-limits",
        key=client_id.encode(),
        value=_avro_encode(schema, sid, payload),
    )
    producer.flush(10)
    click.echo(f"set {client_id}: gross={max_gross} order={max_order} tpm={max_tpm}")


@cli.command()
@click.option("--clients", default=50)
@click.pass_obj
def seed(obj, clients):
    """Publish default limits for C0000..C{clients-1}."""
    schema = json.loads((SCHEMAS_DIR / "client_limit.avsc").read_text())
    sid = requests.get(f"{obj['registry']}/subjects/client-limits-value/versions/latest").json()["id"]
    producer = Producer({"bootstrap.servers": obj["bootstrap"]})

    for i in range(clients):
        cid = f"C{i:04d}"
        payload = {
            "client_id": cid,
            "max_gross_notional": Decimal("1000000"),
            "max_single_order_notional": Decimal("100000"),
            "max_trades_per_minute": 30,
            "updated_ts": int(time.time() * 1000),
        }
        producer.produce(
            "client-limits",
            key=cid.encode(),
            value=_avro_encode(schema, sid, payload),
        )
    producer.flush(10)
    click.echo(f"seeded {clients} clients")


if __name__ == "__main__":
    cli()
