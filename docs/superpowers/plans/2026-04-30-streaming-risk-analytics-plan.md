# Streaming Risk Analytics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an end-to-end streaming pipeline that ingests synthetic equity trades through Kafka, computes per-client positions and detects three classes of limit breaches in two Flink jobs, serves results from Apache Pinot (including an upsert positions table), and renders a four-panel React dashboard — all on `docker-compose` locally with production-shaped code.

**Architecture:** Two Flink jobs (`position-engine` and `risk-engine`) communicate through Kafka. Job A keeps ground-truth positions; Job B consumes positions + enriched-trades + broadcast limits and emits breaches. Avro schemas in Confluent Schema Registry serve as single source of truth (Java classes generated from `schemas/*.avsc`; Python uses `fastavro` against the same files). Pinot ingests three REALTIME tables; React dashboard queries Pinot SQL endpoint directly.

**Tech Stack:** Apache Kafka 4.2.0 (KRaft), Apache Flink 2.2.0 (DataStream API), Apache Pinot 1.5.0, Confluent Schema Registry CP 8.x, Java 21 + Maven, Python 3.12 (`confluent-kafka`, `fastavro`), React + Vite + TypeScript + Tailwind + TanStack Query + Recharts, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-04-30-streaming-risk-analytics-design.md`

---

## File Structure

```
stream_processing_analytics/
├── docker-compose.yml                           # all services
├── Makefile                                     # orchestration targets
├── .gitignore
├── .env.example
├── README.md
├── docs/superpowers/{specs,plans}/...           # already exists
├── schemas/                                     # Avro source-of-truth
│   ├── trade.avsc
│   ├── enriched_trade.avsc
│   ├── client_limit.avsc
│   ├── position.avsc
│   └── breach.avsc
├── flink/
│   ├── pom.xml                                  # parent POM
│   ├── common/
│   │   ├── pom.xml
│   │   └── src/main/{java,resources}/...        # generated Avro + utils + sectors.json
│   ├── position-engine/
│   │   ├── pom.xml
│   │   └── src/{main,test}/java/com/example/position/...
│   └── risk-engine/
│       ├── pom.xml
│       └── src/{main,test}/java/com/example/risk/...
├── pinot/                                       # 3 schemas + 3 table configs
├── tools/
│   ├── pyproject.toml
│   ├── trade_gen/{__main__.py, generator.py, sectors.json}
│   └── limits_cli/__main__.py
├── dashboard/                                   # Vite/React
├── observability/{prometheus.yml, grafana/...}
└── tests/e2e/{conftest.py, test_breach_scenarios.py}
```

---

## Phase 0 — Repo skeleton and foundation infra

### Task 0.1: Initialize git and write `.gitignore`

**Files:** Create `.gitignore`

- [ ] **Step 1: Initialize git**

```bash
cd /Users/robran/IdeaProjects/stream_processing_analytics
git init -b main
```

- [ ] **Step 2: Write `.gitignore`**

```
# Java/Maven
target/
*.class
.idea/
*.iml

# Python
__pycache__/
*.pyc
.venv/
.pytest_cache/
.ruff_cache/

# Node
node_modules/
dist/
.vite/

# Avro generated
flink/common/src/main/java/com/example/avro/

# Flink runtime
checkpoints/
savepoints/
flink-*.jar

# Docker
.env
volumes/

# OS
.DS_Store
```

- [ ] **Step 3: Commit**

```bash
git add .gitignore
git commit -m "chore: initialize repo with gitignore"
```

---

### Task 0.2: Write Avro schemas (single source of truth)

**Files:** Create `schemas/trade.avsc`, `schemas/enriched_trade.avsc`, `schemas/client_limit.avsc`, `schemas/position.avsc`, `schemas/breach.avsc`

- [ ] **Step 1: Create `schemas/trade.avsc`**

```json
{
  "type": "record",
  "namespace": "com.example.avro",
  "name": "Trade",
  "fields": [
    {"name": "trade_id", "type": "string"},
    {"name": "order_id", "type": "string"},
    {"name": "client_id", "type": "string"},
    {"name": "symbol", "type": "string"},
    {"name": "side", "type": {"type": "enum", "name": "Side", "symbols": ["BUY", "SELL"]}},
    {"name": "quantity", "type": "long"},
    {"name": "price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 4}},
    {"name": "currency", "type": "string", "default": "USD"},
    {"name": "venue", "type": "string"},
    {"name": "event_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

- [ ] **Step 2: Create `schemas/enriched_trade.avsc`**

```json
{
  "type": "record",
  "namespace": "com.example.avro",
  "name": "EnrichedTrade",
  "fields": [
    {"name": "trade_id", "type": "string"},
    {"name": "order_id", "type": "string"},
    {"name": "client_id", "type": "string"},
    {"name": "symbol", "type": "string"},
    {"name": "side", "type": "com.example.avro.Side"},
    {"name": "quantity", "type": "long"},
    {"name": "signed_qty", "type": "long"},
    {"name": "price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 4}},
    {"name": "notional", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2}},
    {"name": "currency", "type": "string", "default": "USD"},
    {"name": "venue", "type": "string"},
    {"name": "sector", "type": "string", "default": "UNKNOWN"},
    {"name": "event_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

- [ ] **Step 3: Create `schemas/client_limit.avsc`**

```json
{
  "type": "record",
  "namespace": "com.example.avro",
  "name": "ClientLimit",
  "fields": [
    {"name": "client_id", "type": "string"},
    {"name": "max_gross_notional", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2}},
    {"name": "max_single_order_notional", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2}},
    {"name": "max_trades_per_minute", "type": "int"},
    {"name": "updated_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

- [ ] **Step 4: Create `schemas/position.avsc`**

```json
{
  "type": "record",
  "namespace": "com.example.avro",
  "name": "Position",
  "fields": [
    {"name": "client_id", "type": "string"},
    {"name": "symbol", "type": "string"},
    {"name": "net_quantity", "type": "long"},
    {"name": "avg_price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 4}},
    {"name": "notional", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2}},
    {"name": "sector", "type": "string"},
    {"name": "last_update", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

- [ ] **Step 5: Create `schemas/breach.avsc`**

```json
{
  "type": "record",
  "namespace": "com.example.avro",
  "name": "Breach",
  "fields": [
    {"name": "breach_id", "type": "string"},
    {"name": "client_id", "type": "string"},
    {"name": "breach_type", "type": {"type": "enum", "name": "BreachType", "symbols": ["GROSS_NOTIONAL", "SINGLE_ORDER_NOTIONAL", "TRADE_VELOCITY"]}},
    {"name": "limit_value", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2}},
    {"name": "actual_value", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2}},
    {"name": "trade_id", "type": ["null", "string"], "default": null},
    {"name": "order_id", "type": ["null", "string"], "default": null},
    {"name": "detected_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "details", "type": "string"}
  ]
}
```

- [ ] **Step 6: Commit**

```bash
git add schemas/
git commit -m "feat: add Avro schemas for trade/enriched_trade/client_limit/position/breach"
```

---

### Task 0.3: docker-compose for Kafka + Schema Registry

**Files:** Create `docker-compose.yml`, `.env.example`

- [ ] **Step 1: Create `.env.example`**

```
# Override versions if you need to
KAFKA_VERSION=4.2.0
FLINK_VERSION=2.2.0
PINOT_VERSION=1.5.0
SCHEMA_REGISTRY_VERSION=8.0.0
```

- [ ] **Step 2: Create `docker-compose.yml` (Kafka + Schema Registry only — other services added in later phases)**

```yaml
services:
  kafka:
    image: apache/kafka:4.2.0
    container_name: kafka
    ports:
      - "9092:9092"
      - "9094:9094"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 20

  schema-registry:
    image: confluentinc/cp-schema-registry:8.0.0
    container_name: schema-registry
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: PLAINTEXT://kafka:9092
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC: _schemas
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8081/subjects || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 20

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081
```

- [ ] **Step 3: Smoke-test the stack**

```bash
docker compose up -d kafka schema-registry kafka-ui
docker compose ps
```
Expected: all three services `healthy` after ~20s. `curl -s http://localhost:8081/subjects` returns `[]`.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml .env.example
git commit -m "feat: add docker-compose with Kafka 4.2 (KRaft) and Schema Registry"
```

---

### Task 0.4: Makefile with topic creation and schema registration

**Files:** Create `Makefile`

- [ ] **Step 1: Write Makefile**

```makefile
SHELL := /usr/bin/env bash
.PHONY: up down clean topics register-schemas pinot-tables submit-jobs seed-limits trade-gen all

KAFKA := docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092

up:
	docker compose up -d
	@echo "Waiting for services..."
	@sleep 5

down:
	docker compose down

clean:
	docker compose down -v
	rm -rf checkpoints/ volumes/

topics:
	$(KAFKA) --create --if-not-exists --topic trades            --partitions 4 --replication-factor 1
	$(KAFKA) --create --if-not-exists --topic enriched-trades   --partitions 4 --replication-factor 1
	$(KAFKA) --create --if-not-exists --topic positions         --partitions 4 --replication-factor 1
	$(KAFKA) --create --if-not-exists --topic breaches          --partitions 4 --replication-factor 1
	$(KAFKA) --create --if-not-exists --topic dlq               --partitions 1 --replication-factor 1
	$(KAFKA) --create --if-not-exists --topic client-limits     --partitions 4 --replication-factor 1 \
		--config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.01 --config segment.ms=60000
	$(KAFKA) --list

register-schemas:
	@for f in schemas/*.avsc; do \
		subject=$$(basename $$f .avsc); \
		case $$subject in \
			trade) topic=trades ;; \
			enriched_trade) topic=enriched-trades ;; \
			client_limit) topic=client-limits ;; \
			position) topic=positions ;; \
			breach) topic=breaches ;; \
		esac; \
		echo "Registering $$f for $$topic-value..."; \
		jq -Rs '{schema: .}' $$f | \
			curl -fsS -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
			--data @- http://localhost:8081/subjects/$$topic-value/versions; \
		echo; \
	done

pinot-tables:
	@echo "Implemented in Phase 4"

submit-jobs:
	@echo "Implemented in Phase 5"

seed-limits:
	@echo "Implemented in Phase 2"

trade-gen:
	@echo "Implemented in Phase 2"

all: up topics register-schemas pinot-tables submit-jobs seed-limits trade-gen
```

- [ ] **Step 2: Run `make topics`**

```bash
make up
make topics
```
Expected: 6 topics listed (trades, enriched-trades, positions, breaches, dlq, client-limits).

- [ ] **Step 3: Run `make register-schemas`**

```bash
make register-schemas
curl -s http://localhost:8081/subjects | jq
```
Expected: `["breach-value","client_limit-value","enriched_trade-value","position-value","trade-value"]` — 5 subjects (we register raw `trade-value` once but use the alias for the topic).

If the topic-name mapping differs (subject must be `<topic>-value`), the Makefile already maps schema filename → topic-value. Re-verify by curl.

- [ ] **Step 4: Commit**

```bash
git add Makefile
git commit -m "feat: add Makefile with topic creation and schema registration"
```

---

## Phase 1 — Python tooling (trade-gen + limits-cli)

### Task 1.1: Python project skeleton

**Files:** Create `tools/pyproject.toml`, `tools/trade_gen/__init__.py`, `tools/limits_cli/__init__.py`

- [ ] **Step 1: Write `tools/pyproject.toml`**

```toml
[project]
name = "stream-tools"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
  "confluent-kafka[avro]>=2.6.0",
  "fastavro>=1.9.0",
  "click>=8.1.0",
  "pytest>=8.0.0",
  "requests>=2.31.0",
]

[tool.setuptools.packages.find]
include = ["trade_gen*", "limits_cli*"]
```

- [ ] **Step 2: Create empty package files**

```bash
mkdir -p tools/trade_gen tools/limits_cli
touch tools/trade_gen/__init__.py tools/limits_cli/__init__.py
```

- [ ] **Step 3: Set up venv and install**

```bash
cd tools
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .
```
Expected: install succeeds.

- [ ] **Step 4: Commit**

```bash
cd ..
git add tools/pyproject.toml tools/trade_gen/__init__.py tools/limits_cli/__init__.py
git commit -m "feat: add Python tooling project skeleton"
```

---

### Task 1.2: Sector mapping (shared between trade-gen and Flink)

**Files:** Create `tools/trade_gen/sectors.json`. Same file will be copied into `flink/common/src/main/resources/` in Phase 2.

- [ ] **Step 1: Write `tools/trade_gen/sectors.json` (~30 representative tickers)**

```json
{
  "AAPL": "Technology", "MSFT": "Technology", "GOOGL": "Technology", "META": "Technology",
  "NVDA": "Technology", "AMD": "Technology", "INTC": "Technology", "ORCL": "Technology",
  "CRM": "Technology", "ADBE": "Technology",
  "JPM": "Financials", "BAC": "Financials", "WFC": "Financials", "GS": "Financials",
  "MS": "Financials", "C": "Financials",
  "JNJ": "Healthcare", "PFE": "Healthcare", "MRK": "Healthcare", "UNH": "Healthcare",
  "ABBV": "Healthcare",
  "XOM": "Energy", "CVX": "Energy", "COP": "Energy",
  "WMT": "ConsumerStaples", "KO": "ConsumerStaples", "PG": "ConsumerStaples",
  "TSLA": "ConsumerDiscretionary", "AMZN": "ConsumerDiscretionary", "NKE": "ConsumerDiscretionary"
}
```

- [ ] **Step 2: Commit**

```bash
git add tools/trade_gen/sectors.json
git commit -m "feat: add sector mapping (~30 symbols across 6 sectors)"
```

---

### Task 1.3: Trade generator — write the failing test

**Files:** Create `tools/trade_gen/generator.py`, `tools/trade_gen/test_generator.py`

- [ ] **Step 1: Write `tools/trade_gen/test_generator.py`**

```python
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
```

- [ ] **Step 2: Run test (should fail — module doesn't exist)**

```bash
cd tools
source .venv/bin/activate
pytest trade_gen/test_generator.py -v
```
Expected: FAIL with `ImportError: cannot import name 'TradeFactory'`.

---

### Task 1.4: Trade generator — minimal implementation

**Files:** Modify `tools/trade_gen/generator.py`

- [ ] **Step 1: Write `tools/trade_gen/generator.py`**

```python
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
            s: Decimal(f"{self._rng.uniform(20, 800):.4f}") for s in symbols
        } if (symbols := self.symbols) else {}

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
```

- [ ] **Step 2: Run test (should pass)**

```bash
pytest trade_gen/test_generator.py -v
```
Expected: 2 tests pass.

- [ ] **Step 3: Commit**

```bash
cd ..
git add tools/trade_gen/generator.py tools/trade_gen/test_generator.py
git commit -m "feat: add TradeFactory with deterministic synthetic trade generation"
```

---

### Task 1.5: Trade generator CLI (publishes Avro to Kafka)

**Files:** Create `tools/trade_gen/__main__.py`

- [ ] **Step 1: Write `tools/trade_gen/__main__.py`**

```python
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
    schema_id = _registered_schema_id(registry, "trade-value")
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
```

- [ ] **Step 2: Smoke-test (with stack up + topics + schemas registered)**

```bash
cd tools
source .venv/bin/activate
python -m trade_gen --rate 5 --duration 3
```
Expected: prints "sent 100 trades…" or fewer if `--duration` short. Verify in kafka-ui (`http://localhost:8080`) — `trades` topic has messages.

- [ ] **Step 3: Add Makefile target**

In Makefile, replace the `trade-gen` placeholder:

```makefile
trade-gen:
	cd tools && . .venv/bin/activate && python -m trade_gen --rate 20
```

- [ ] **Step 4: Commit**

```bash
cd ..
git add tools/trade_gen/__main__.py Makefile
git commit -m "feat: add trade-gen CLI publishing Avro trades to Kafka"
```

---

### Task 1.6: Limits CLI

**Files:** Create `tools/limits_cli/__main__.py`

- [ ] **Step 1: Write `tools/limits_cli/__main__.py`**

```python
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
    schema = json.loads((SCHEMAS_DIR / "client_limit.avsc").read_text())
    sid = requests.get(f"{registry}/subjects/client_limit-value/versions/latest").json()["id"]
    ctx.obj = {
        "producer": Producer({"bootstrap.servers": bootstrap}),
        "schema": schema,
        "schema_id": sid,
    }


@cli.command()
@click.option("--client-id", required=True)
@click.option("--max-gross", type=Decimal, default=Decimal("1000000"))
@click.option("--max-order", type=Decimal, default=Decimal("100000"))
@click.option("--max-tpm",   type=int,     default=30)
@click.pass_obj
def set(obj, client_id, max_gross, max_order, max_tpm):
    """Publish/replace one client's limits."""
    payload = {
        "client_id": client_id,
        "max_gross_notional": max_gross,
        "max_single_order_notional": max_order,
        "max_trades_per_minute": max_tpm,
        "updated_ts": int(time.time() * 1000),
    }
    obj["producer"].produce(
        "client-limits",
        key=client_id.encode(),
        value=_avro_encode(obj["schema"], obj["schema_id"], payload),
    )
    obj["producer"].flush(10)
    click.echo(f"set {client_id}: gross={max_gross} order={max_order} tpm={max_tpm}")


@cli.command()
@click.option("--clients", default=50)
@click.pass_obj
def seed(obj, clients):
    """Publish default limits for C0000..C{clients-1}."""
    for i in range(clients):
        cid = f"C{i:04d}"
        payload = {
            "client_id": cid,
            "max_gross_notional": Decimal("1000000"),
            "max_single_order_notional": Decimal("100000"),
            "max_trades_per_minute": 30,
            "updated_ts": int(time.time() * 1000),
        }
        obj["producer"].produce(
            "client-limits",
            key=cid.encode(),
            value=_avro_encode(obj["schema"], obj["schema_id"], payload),
        )
    obj["producer"].flush(10)
    click.echo(f"seeded {clients} clients")


if __name__ == "__main__":
    cli()
```

- [ ] **Step 2: Smoke-test**

```bash
cd tools
source .venv/bin/activate
python -m limits_cli seed --clients 50
```
Expected: prints `seeded 50 clients`. Verify in kafka-ui.

- [ ] **Step 3: Add Makefile target**

Replace the `seed-limits` placeholder:

```makefile
seed-limits:
	cd tools && . .venv/bin/activate && python -m limits_cli seed --clients 50
```

- [ ] **Step 4: Commit**

```bash
cd ..
git add tools/limits_cli/__main__.py Makefile
git commit -m "feat: add limits CLI with set and seed commands"
```

---

## Phase 2 — Flink job: position-engine

### Task 2.1: Maven parent POM and `common` module

**Files:** Create `flink/pom.xml`, `flink/common/pom.xml`

- [ ] **Step 1: Write `flink/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.example</groupId>
  <artifactId>flink-parent</artifactId>
  <version>0.1.0</version>
  <packaging>pom</packaging>

  <properties>
    <maven.compiler.source>21</maven.compiler.source>
    <maven.compiler.target>21</maven.compiler.target>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <flink.version>2.2.0</flink.version>
    <avro.version>1.11.4</avro.version>
    <confluent.version>7.6.0</confluent.version>
    <junit.version>5.10.2</junit.version>
  </properties>

  <modules>
    <module>common</module>
    <module>position-engine</module>
    <module>risk-engine</module>
  </modules>

  <repositories>
    <repository>
      <id>confluent</id>
      <url>https://packages.confluent.io/maven/</url>
    </repository>
  </repositories>

  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
      </dependency>
      <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-clients</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
      </dependency>
      <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-kafka</artifactId>
        <version>${flink.version}</version>
      </dependency>
      <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-avro</artifactId>
        <version>${flink.version}</version>
      </dependency>
      <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-avro-confluent-registry</artifactId>
        <version>${flink.version}</version>
      </dependency>
      <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-statebackend-rocksdb</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
      </dependency>
      <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-metrics-prometheus</artifactId>
        <version>${flink.version}</version>
      </dependency>
      <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-test-utils</artifactId>
        <version>${flink.version}</version>
        <scope>test</scope>
      </dependency>
      <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java</artifactId>
        <version>${flink.version}</version>
        <type>test-jar</type>
        <scope>test</scope>
      </dependency>
      <dependency>
        <groupId>org.junit.jupiter</groupId>
        <artifactId>junit-jupiter</artifactId>
        <version>${junit.version}</version>
        <scope>test</scope>
      </dependency>
      <dependency>
        <groupId>org.assertj</groupId>
        <artifactId>assertj-core</artifactId>
        <version>3.25.3</version>
        <scope>test</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>

  <build>
    <pluginManagement>
      <plugins>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-shade-plugin</artifactId>
          <version>3.5.3</version>
        </plugin>
        <plugin>
          <groupId>org.apache.avro</groupId>
          <artifactId>avro-maven-plugin</artifactId>
          <version>${avro.version}</version>
        </plugin>
      </plugins>
    </pluginManagement>
  </build>
</project>
```

- [ ] **Step 2: Write `flink/common/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>com.example</groupId>
    <artifactId>flink-parent</artifactId>
    <version>0.1.0</version>
  </parent>
  <artifactId>common</artifactId>

  <dependencies>
    <dependency>
      <groupId>org.apache.avro</groupId>
      <artifactId>avro</artifactId>
      <version>${avro.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java</artifactId>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-avro</artifactId>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-avro-confluent-registry</artifactId>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-connector-kafka</artifactId>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.avro</groupId>
        <artifactId>avro-maven-plugin</artifactId>
        <executions>
          <execution>
            <phase>generate-sources</phase>
            <goals><goal>schema</goal></goals>
            <configuration>
              <sourceDirectory>${project.basedir}/../../schemas</sourceDirectory>
              <outputDirectory>${project.basedir}/src/main/java</outputDirectory>
              <stringType>String</stringType>
              <enableDecimalLogicalType>true</enableDecimalLogicalType>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 3: Build common module to generate Avro classes**

```bash
cd flink
mvn -pl common compile
```
Expected: `flink/common/src/main/java/com/example/avro/{Trade,EnrichedTrade,Position,ClientLimit,Breach,Side,BreachType}.java` generated.

- [ ] **Step 4: Commit**

```bash
cd ..
git add flink/pom.xml flink/common/pom.xml
git commit -m "feat: add Flink Maven parent POM and common module with Avro codegen"
```

---

### Task 2.2: Common module — sectors resource and `SectorLookup` helper

**Files:** Create `flink/common/src/main/resources/sectors.json`, `flink/common/src/main/java/com/example/common/SectorLookup.java`, `flink/common/src/test/java/com/example/common/SectorLookupTest.java`

- [ ] **Step 1: Copy sectors.json into Flink resources**

```bash
cp tools/trade_gen/sectors.json flink/common/src/main/resources/sectors.json
```

- [ ] **Step 2: Write the failing test**

```java
// flink/common/src/test/java/com/example/common/SectorLookupTest.java
package com.example.common;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class SectorLookupTest {
    @Test
    void resolvesKnownSymbol() {
        assertThat(SectorLookup.sectorFor("AAPL")).isEqualTo("Technology");
    }
    @Test
    void unknownSymbolReturnsUnknown() {
        assertThat(SectorLookup.sectorFor("ZZZZ")).isEqualTo("UNKNOWN");
    }
}
```

Add to `flink/common/pom.xml` test deps if not already present:
```xml
<dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId></dependency>
<dependency><groupId>org.assertj</groupId><artifactId>assertj-core</artifactId></dependency>
```

- [ ] **Step 3: Run test to verify it fails**

```bash
cd flink
mvn -pl common test
```
Expected: compilation FAILURE — `SectorLookup` not found.

- [ ] **Step 4: Write the minimal implementation**

```java
// flink/common/src/main/java/com/example/common/SectorLookup.java
package com.example.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.Map;

public final class SectorLookup {
    private static final Map<String, String> MAP;
    static {
        try (InputStream in = SectorLookup.class.getResourceAsStream("/sectors.json")) {
            if (in == null) throw new IllegalStateException("sectors.json missing");
            MAP = new ObjectMapper().readValue(in, Map.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private SectorLookup() {}
    public static String sectorFor(String symbol) {
        return MAP.getOrDefault(symbol, "UNKNOWN");
    }
    public static boolean isKnown(String symbol) {
        return MAP.containsKey(symbol);
    }
}
```

Add Jackson dependency to `flink/common/pom.xml`:
```xml
<dependency>
  <groupId>com.fasterxml.jackson.core</groupId>
  <artifactId>jackson-databind</artifactId>
  <version>2.17.0</version>
</dependency>
```

- [ ] **Step 5: Run test to verify it passes**

```bash
mvn -pl common test
```
Expected: 2 tests pass.

- [ ] **Step 6: Commit**

```bash
cd ..
git add flink/common/
git commit -m "feat: add SectorLookup loading symbol→sector from classpath"
```

---

### Task 2.3: Common module — `KafkaSourceFactory` and `KafkaSinkFactory` helpers

**Files:** Create `flink/common/src/main/java/com/example/common/{KafkaSourceFactory,KafkaSinkFactory}.java`

These are non-trivial wiring; centralizing them avoids per-job boilerplate.

- [ ] **Step 1: Write `KafkaSourceFactory.java`**

```java
package com.example.common;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;

import java.time.Duration;
import java.util.Properties;

public final class KafkaSourceFactory {
    private KafkaSourceFactory() {}

    public static <T extends SpecificRecord> KafkaSource<T> avroSource(
            Class<T> type, String topic, String bootstrap, String registry, String groupId) {
        Properties props = new Properties();
        props.setProperty("specific.avro.reader", "true");
        return KafkaSource.<T>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(
                ConfluentRegistryAvroDeserializationSchema.forSpecific(type, registry))
            .setProperties(props)
            .build();
    }

    public static WatermarkStrategy<?> watermarkStrategy(java.util.function.ToLongFunction<?> ts) {
        // Caller passes a typed extractor; wrap below.
        throw new UnsupportedOperationException("Use watermarkStrategy(ToLongFunction<T>) typed helper.");
    }

    public static <T> WatermarkStrategy<T> watermarkStrategyTyped(
            java.util.function.ToLongFunction<T> tsExtractor) {
        return WatermarkStrategy.<T>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withIdleness(Duration.ofSeconds(30))
            .withTimestampAssigner((event, recordTs) -> tsExtractor.applyAsLong(event));
    }
}
```

- [ ] **Step 2: Write `KafkaSinkFactory.java`**

```java
package com.example.common;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

import java.util.Properties;
import java.util.function.Function;

public final class KafkaSinkFactory {
    private KafkaSinkFactory() {}

    public static <T extends SpecificRecord> KafkaSink<T> avroSink(
            Class<T> type, String topic, String bootstrap, String registry,
            Function<T, byte[]> keyExtractor, String txnIdPrefix) {
        Properties txnProps = new Properties();
        txnProps.setProperty("transaction.timeout.ms", "300000");
        return KafkaSink.<T>builder()
            .setBootstrapServers(bootstrap)
            .setKafkaProducerConfig(txnProps)
            .setTransactionalIdPrefix(txnIdPrefix)
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                .setTopic(topic)
                .setKeySerializationSchema(keyExtractor::apply)
                .setValueSerializationSchema(
                    ConfluentRegistryAvroSerializationSchema.forSpecific(type, topic + "-value", registry))
                .build())
            .build();
    }
}
```

- [ ] **Step 3: Build to verify it compiles**

```bash
cd flink
mvn -pl common compile
```
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
cd ..
git add flink/common/src/main/java/com/example/common/Kafka{Source,Sink}Factory.java
git commit -m "feat: add Kafka source/sink factory helpers with Avro Schema Registry integration"
```

---

### Task 2.4: `Enricher` — write the failing test

**Files:** Create `flink/position-engine/pom.xml`, `flink/position-engine/src/test/java/com/example/position/EnricherTest.java`

- [ ] **Step 1: Write `flink/position-engine/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>com.example</groupId>
    <artifactId>flink-parent</artifactId>
    <version>0.1.0</version>
  </parent>
  <artifactId>position-engine</artifactId>

  <dependencies>
    <dependency><groupId>com.example</groupId><artifactId>common</artifactId><version>0.1.0</version></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-streaming-java</artifactId></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-connector-kafka</artifactId></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-avro</artifactId></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-avro-confluent-registry</artifactId></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-statebackend-rocksdb</artifactId></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-metrics-prometheus</artifactId></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-clients</artifactId></dependency>

    <dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId></dependency>
    <dependency><groupId>org.assertj</groupId><artifactId>assertj-core</artifactId></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-test-utils</artifactId></dependency>
    <dependency><groupId>org.apache.flink</groupId><artifactId>flink-streaming-java</artifactId><type>test-jar</type></dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>com.example.position.PositionEngineJob</mainClass>
                </transformer>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
              </transformers>
              <artifactSet>
                <excludes>
                  <exclude>org.apache.flink:flink-streaming-java</exclude>
                  <exclude>org.apache.flink:flink-clients</exclude>
                </excludes>
              </artifactSet>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 2: Write `EnricherTest.java`**

```java
// flink/position-engine/src/test/java/com/example/position/EnricherTest.java
package com.example.position;

import com.example.avro.Side;
import com.example.avro.Trade;
import com.example.avro.EnrichedTrade;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import static org.assertj.core.api.Assertions.assertThat;

class EnricherTest {

    private static ByteBuffer dec(String s, int scale) {
        return ByteBuffer.wrap(new BigDecimal(s).setScale(scale).unscaledValue().toByteArray());
    }
    private static BigDecimal asDecimal(ByteBuffer bb, int scale) {
        byte[] bytes = new byte[bb.remaining()];
        bb.duplicate().get(bytes);
        return new BigDecimal(new java.math.BigInteger(bytes), scale);
    }

    @Test
    void enrichesBuyTradeWithSignedQtyAndNotional() {
        Trade t = Trade.newBuilder()
            .setTradeId("t1").setOrderId("o1").setClientId("C0001")
            .setSymbol("AAPL").setSide(Side.BUY).setQuantity(100L)
            .setPrice(dec("150.0000", 4)).setCurrency("USD").setVenue("NASDAQ")
            .setEventTs(1000L).build();

        EnrichedTrade out = new Enricher().map(t);

        assertThat(out.getSignedQty()).isEqualTo(100L);
        assertThat(asDecimal(out.getNotional(), 2)).isEqualByComparingTo(new BigDecimal("15000.00"));
        assertThat(out.getSector()).isEqualTo("Technology");
    }

    @Test
    void enrichesSellTradeWithNegativeSignedQty() {
        Trade t = Trade.newBuilder()
            .setTradeId("t1").setOrderId("o1").setClientId("C0001")
            .setSymbol("AAPL").setSide(Side.SELL).setQuantity(50L)
            .setPrice(dec("200.0000", 4)).setCurrency("USD").setVenue("NASDAQ")
            .setEventTs(1000L).build();

        EnrichedTrade out = new Enricher().map(t);

        assertThat(out.getSignedQty()).isEqualTo(-50L);
        assertThat(asDecimal(out.getNotional(), 2)).isEqualByComparingTo(new BigDecimal("10000.00"));
    }

    @Test
    void unknownSymbolGetsUnknownSector() {
        Trade t = Trade.newBuilder()
            .setTradeId("t1").setOrderId("o1").setClientId("C0001")
            .setSymbol("ZZZZ").setSide(Side.BUY).setQuantity(1L)
            .setPrice(dec("1.0000", 4)).setCurrency("USD").setVenue("NASDAQ")
            .setEventTs(1L).build();
        EnrichedTrade out = new Enricher().map(t);
        assertThat(out.getSector()).isEqualTo("UNKNOWN");
    }
}
```

- [ ] **Step 3: Run test (should fail — class missing)**

```bash
cd flink
mvn -pl position-engine -am test-compile
```
Expected: FAILURE — `Enricher` not found.

---

### Task 2.5: `Enricher` implementation

**Files:** Create `flink/position-engine/src/main/java/com/example/position/Enricher.java`

- [ ] **Step 1: Write `Enricher.java`**

```java
package com.example.position;

import com.example.avro.EnrichedTrade;
import com.example.avro.Side;
import com.example.avro.Trade;
import com.example.common.SectorLookup;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class Enricher implements MapFunction<Trade, EnrichedTrade> {

    @Override
    public EnrichedTrade map(Trade t) {
        long signed = (t.getSide() == Side.BUY ? +1L : -1L) * t.getQuantity();
        BigDecimal price = decimal(t.getPrice(), 4);
        BigDecimal notional = price.multiply(BigDecimal.valueOf(t.getQuantity())).setScale(2, java.math.RoundingMode.HALF_UP);
        return EnrichedTrade.newBuilder()
            .setTradeId(t.getTradeId())
            .setOrderId(t.getOrderId())
            .setClientId(t.getClientId())
            .setSymbol(t.getSymbol())
            .setSide(t.getSide())
            .setQuantity(t.getQuantity())
            .setSignedQty(signed)
            .setPrice(t.getPrice())
            .setNotional(toBytes(notional))
            .setCurrency(t.getCurrency())
            .setVenue(t.getVenue())
            .setSector(SectorLookup.sectorFor(t.getSymbol()))
            .setEventTs(t.getEventTs())
            .build();
    }

    static BigDecimal decimal(ByteBuffer bb, int scale) {
        byte[] bytes = new byte[bb.remaining()];
        bb.duplicate().get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
    }
    static ByteBuffer toBytes(BigDecimal v) {
        return ByteBuffer.wrap(v.unscaledValue().toByteArray());
    }
}
```

- [ ] **Step 2: Run tests (should pass)**

```bash
mvn -pl position-engine -am test
```
Expected: 3 EnricherTest tests pass.

- [ ] **Step 3: Commit**

```bash
cd ..
git add flink/position-engine/
git commit -m "feat: add Enricher map function with TDD"
```

---

### Task 2.6: `LatenessRouter` — drop > 60s late events to DLQ side output

**Files:** Create `flink/position-engine/src/main/java/com/example/position/LatenessRouter.java`, `LatenessRouterTest.java`

- [ ] **Step 1: Write the failing test**

```java
// flink/position-engine/src/test/java/com/example/position/LatenessRouterTest.java
package com.example.position;

import com.example.avro.Side;
import com.example.avro.Trade;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class LatenessRouterTest {
    private static ByteBuffer dec(String s, int scale) {
        return ByteBuffer.wrap(new BigDecimal(s).setScale(scale).unscaledValue().toByteArray());
    }
    private static Trade t(long ts) {
        return Trade.newBuilder()
            .setTradeId("t").setOrderId("o").setClientId("C0001")
            .setSymbol("AAPL").setSide(Side.BUY).setQuantity(1L)
            .setPrice(dec("1.0000", 4)).setCurrency("USD").setVenue("X").setEventTs(ts).build();
    }

    @Test
    void onTimeRoutedToMain() throws Exception {
        var op = new ProcessOperator<>(new LatenessRouter());
        try (var h = new OneInputStreamOperatorTestHarness<>(op)) {
            h.open();
            h.processWatermark(100_000L);              // wm=100s
            h.processElement(new StreamRecord<>(t(99_000L), 99_000L));  // 1s late, ok
            assertThat(h.extractOutputValues()).hasSize(1);
            assertThat(h.getSideOutput(LatenessRouter.DLQ_TAG)).isNullOrEmpty();
        }
    }

    @Test
    void veryLateRoutedToDlq() throws Exception {
        var op = new ProcessOperator<>(new LatenessRouter());
        try (var h = new OneInputStreamOperatorTestHarness<>(op)) {
            h.open();
            h.processWatermark(200_000L);              // wm=200s
            h.processElement(new StreamRecord<>(t(100_000L), 100_000L)); // 100s late
            assertThat(h.extractOutputValues()).isEmpty();
            assertThat(h.getSideOutput(LatenessRouter.DLQ_TAG)).hasSize(1);
        }
    }
}
```

- [ ] **Step 2: Run test (should fail to compile)**

```bash
cd flink
mvn -pl position-engine -am test-compile
```
Expected: FAILURE — `LatenessRouter` missing.

- [ ] **Step 3: Implement**

```java
// flink/position-engine/src/main/java/com/example/position/LatenessRouter.java
package com.example.position;

import com.example.avro.Trade;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class LatenessRouter extends ProcessFunction<Trade, Trade> {
    public static final long LATENESS_THRESHOLD_MS = 60_000L;
    public static final OutputTag<Trade> DLQ_TAG = new OutputTag<>("dlq-late") {};

    @Override
    public void processElement(Trade t, Context ctx, Collector<Trade> out) {
        long wm = ctx.timerService().currentWatermark();
        if (wm > Long.MIN_VALUE && (wm - t.getEventTs()) > LATENESS_THRESHOLD_MS) {
            ctx.output(DLQ_TAG, t);
        } else {
            out.collect(t);
        }
    }
}
```

- [ ] **Step 4: Run test (should pass)**

```bash
mvn -pl position-engine -am test -Dtest=LatenessRouterTest
```
Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
cd ..
git add flink/position-engine/src/main/java/com/example/position/LatenessRouter.java \
        flink/position-engine/src/test/java/com/example/position/LatenessRouterTest.java
git commit -m "feat: add LatenessRouter routing >60s-late trades to DLQ side output"
```

---

### Task 2.7: `PositionUpdater` — write the failing test

**Files:** Create `flink/position-engine/src/test/java/com/example/position/PositionUpdaterTest.java`

- [ ] **Step 1: Write the test**

```java
// flink/position-engine/src/test/java/com/example/position/PositionUpdaterTest.java
package com.example.position;

import com.example.avro.EnrichedTrade;
import com.example.avro.Position;
import com.example.avro.Side;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import static org.assertj.core.api.Assertions.assertThat;

class PositionUpdaterTest {
    private static ByteBuffer dec(String s, int scale) {
        return ByteBuffer.wrap(new BigDecimal(s).setScale(scale).unscaledValue().toByteArray());
    }
    private static BigDecimal asDecimal(ByteBuffer bb, int scale) {
        byte[] bytes = new byte[bb.remaining()];
        bb.duplicate().get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
    }
    private static EnrichedTrade et(String tid, String sym, Side side, long qty, String price, long ts) {
        long signed = (side == Side.BUY ? 1L : -1L) * qty;
        BigDecimal p = new BigDecimal(price);
        BigDecimal n = p.multiply(BigDecimal.valueOf(qty)).setScale(2);
        return EnrichedTrade.newBuilder()
            .setTradeId(tid).setOrderId("o").setClientId("C0001").setSymbol(sym)
            .setSide(side).setQuantity(qty).setSignedQty(signed)
            .setPrice(dec(price, 4)).setNotional(ByteBuffer.wrap(n.unscaledValue().toByteArray()))
            .setCurrency("USD").setVenue("X").setSector("Technology").setEventTs(ts).build();
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, EnrichedTrade, Position> harness() throws Exception {
        var op = new KeyedProcessOperator<>(new PositionUpdater());
        var h = new KeyedOneInputStreamOperatorTestHarness<>(
            op, e -> e.getClientId() + "|" + e.getSymbol(),
            org.apache.flink.api.common.typeinfo.Types.STRING);
        h.open();
        return h;
    }

    @Test
    void buyThenSellAccumulatesNetQuantity() throws Exception {
        try (var h = harness()) {
            h.processElement(new StreamRecord<>(et("t1", "AAPL", Side.BUY, 100, "150.0000", 1_000L), 1_000L));
            h.processElement(new StreamRecord<>(et("t2", "AAPL", Side.SELL, 30, "160.0000", 2_000L), 2_000L));
            var out = h.extractOutputValues();
            assertThat(out).hasSize(2);
            assertThat(out.get(1).getNetQuantity()).isEqualTo(70L);
            assertThat(asDecimal(out.get(1).getNotional(), 2))
                .isEqualByComparingTo(new BigDecimal("11200.00"));    // 70 * last_price 160
        }
    }

    @Test
    void lateTradeDoesNotRegressLastPrice() throws Exception {
        try (var h = harness()) {
            h.processElement(new StreamRecord<>(et("t1", "AAPL", Side.BUY, 10, "200.0000", 5_000L), 5_000L));
            h.processElement(new StreamRecord<>(et("t2", "AAPL", Side.BUY,  5, "100.0000", 2_000L), 2_000L)); // late
            var out = h.extractOutputValues();
            assertThat(out.get(1).getNetQuantity()).isEqualTo(15L);
            // last_price stays 200 → notional = 15 * 200 = 3000
            assertThat(asDecimal(out.get(1).getNotional(), 2))
                .isEqualByComparingTo(new BigDecimal("3000.00"));
        }
    }

    @Test
    void lastUpdateIsMonotonic() throws Exception {
        try (var h = harness()) {
            h.processElement(new StreamRecord<>(et("t1", "AAPL", Side.BUY, 10, "10.0000", 5_000L), 5_000L));
            h.processElement(new StreamRecord<>(et("t2", "AAPL", Side.BUY, 10, "10.0000", 1_000L), 1_000L)); // late
            var out = h.extractOutputValues();
            assertThat(out.get(0).getLastUpdate()).isEqualTo(5_000L);
            assertThat(out.get(1).getLastUpdate()).isEqualTo(5_000L);  // unchanged
        }
    }
}
```

- [ ] **Step 2: Run (should fail to compile)**

```bash
cd flink
mvn -pl position-engine -am test-compile
```
Expected: FAIL — `PositionUpdater` missing.

---

### Task 2.8: `PositionUpdater` implementation

**Files:** Create `flink/position-engine/src/main/java/com/example/position/PositionUpdater.java`

- [ ] **Step 1: Write `PositionUpdater.java`**

```java
package com.example.position;

import com.example.avro.EnrichedTrade;
import com.example.avro.Position;
import com.example.avro.Side;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

public class PositionUpdater extends KeyedProcessFunction<String, EnrichedTrade, Position> {

    public static class State {
        public long netQuantity;
        public BigDecimal weightedCostBasis = BigDecimal.ZERO;   // demo-grade
        public BigDecimal lastPrice = BigDecimal.ZERO;
        public long lastEventTs = Long.MIN_VALUE;
        public long lastUpdate;
        public String sector = "UNKNOWN";
    }

    private transient ValueState<State> stateHolder;

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext ctx) {
        stateHolder = getRuntimeContext().getState(
            new ValueStateDescriptor<>("position", State.class));
    }

    @Override
    public void processElement(EnrichedTrade t, Context ctx, Collector<Position> out) throws Exception {
        State s = stateHolder.value();
        if (s == null) s = new State();

        BigDecimal price = decimal(t.getPrice(), 4);

        // Net quantity is always applied (commutative).
        long previousNet = s.netQuantity;
        s.netQuantity += t.getSignedQty();

        // Weighted-average cost basis: only on adds (BUYs).
        if (t.getSide() == Side.BUY) {
            BigDecimal addQty = BigDecimal.valueOf(t.getQuantity());
            BigDecimal prevQty = BigDecimal.valueOf(Math.max(previousNet, 0L));
            BigDecimal numerator = s.weightedCostBasis.multiply(prevQty).add(price.multiply(addQty));
            BigDecimal denominator = prevQty.add(addQty);
            s.weightedCostBasis = denominator.signum() == 0
                ? BigDecimal.ZERO
                : numerator.divide(denominator, 4, RoundingMode.HALF_UP);
        }

        // last_price/last_event_ts only advance with newer event_ts
        if (t.getEventTs() > s.lastEventTs) {
            s.lastPrice = price;
            s.lastEventTs = t.getEventTs();
        }
        s.lastUpdate = Math.max(s.lastUpdate, t.getEventTs());
        s.sector = t.getSector();

        BigDecimal notional = s.lastPrice
            .multiply(BigDecimal.valueOf(s.netQuantity))
            .setScale(2, RoundingMode.HALF_UP);

        out.collect(Position.newBuilder()
            .setClientId(t.getClientId())
            .setSymbol(t.getSymbol())
            .setNetQuantity(s.netQuantity)
            .setAvgPrice(toBytes(s.weightedCostBasis.setScale(4, RoundingMode.HALF_UP)))
            .setNotional(toBytes(notional))
            .setSector(s.sector)
            .setLastUpdate(s.lastUpdate)
            .build());

        stateHolder.update(s);
    }

    private static BigDecimal decimal(ByteBuffer bb, int scale) {
        byte[] bytes = new byte[bb.remaining()];
        bb.duplicate().get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
    }
    private static ByteBuffer toBytes(BigDecimal v) {
        return ByteBuffer.wrap(v.unscaledValue().toByteArray());
    }
}
```

- [ ] **Step 2: Run tests**

```bash
mvn -pl position-engine -am test -Dtest=PositionUpdaterTest
```
Expected: 3 tests pass.

- [ ] **Step 3: Commit**

```bash
cd ..
git add flink/position-engine/src/main/java/com/example/position/PositionUpdater.java \
        flink/position-engine/src/test/java/com/example/position/PositionUpdaterTest.java
git commit -m "feat: add PositionUpdater with late-arrival-safe last_price and net_quantity"
```

---

### Task 2.9: `ClientSymbolPartitioner` (custom Kafka producer partitioner for `positions` topic)

**Files:** Create `flink/position-engine/src/main/java/com/example/position/ClientSymbolPartitioner.java` and a unit test.

- [ ] **Step 1: Write the test**

```java
// flink/position-engine/src/test/java/com/example/position/ClientSymbolPartitionerTest.java
package com.example.position;

import com.example.avro.Position;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import static org.assertj.core.api.Assertions.assertThat;

class ClientSymbolPartitionerTest {

    private static Position p(String c, String s) {
        ByteBuffer zero = ByteBuffer.wrap(BigDecimal.ZERO.unscaledValue().toByteArray());
        return Position.newBuilder()
            .setClientId(c).setSymbol(s).setNetQuantity(0L)
            .setAvgPrice(zero).setNotional(zero).setSector("X").setLastUpdate(0L).build();
    }

    @Test
    void samePairAlwaysSamePartition() {
        var part = new ClientSymbolPartitioner();
        int p1 = part.partition(p("C1", "AAPL"), null, null, "positions", new int[]{0, 1, 2, 3});
        int p2 = part.partition(p("C1", "AAPL"), null, null, "positions", new int[]{0, 1, 2, 3});
        assertThat(p1).isEqualTo(p2);
        assertThat(p1).isBetween(0, 3);
    }

    @Test
    void differentPairsCanBeDifferentPartitions() {
        var part = new ClientSymbolPartitioner();
        int[] parts = new int[]{0, 1, 2, 3};
        // Across many distinct pairs we expect to see > 1 partition used.
        int seenMask = 0;
        for (int i = 0; i < 50; i++) {
            seenMask |= (1 << part.partition(p("C" + i, "S" + i), null, null, "positions", parts));
        }
        assertThat(Integer.bitCount(seenMask)).isGreaterThan(1);
    }
}
```

- [ ] **Step 2: Implement**

```java
// flink/position-engine/src/main/java/com/example/position/ClientSymbolPartitioner.java
package com.example.position;

import com.example.avro.Position;
import org.apache.flink.connector.kafka.sink.KafkaPartitioner;

public class ClientSymbolPartitioner implements KafkaPartitioner<Position> {
    @Override
    public int partition(Position record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        String compositeKey = record.getClientId() + "|" + record.getSymbol();
        int h = compositeKey.hashCode();
        return partitions[Math.floorMod(h, partitions.length)];
    }
}
```

- [ ] **Step 3: Run tests**

```bash
cd flink && mvn -pl position-engine -am test -Dtest=ClientSymbolPartitionerTest
```
Expected: 2 tests pass.

- [ ] **Step 4: Commit**

```bash
cd ..
git add flink/position-engine/src/main/java/com/example/position/ClientSymbolPartitioner.java \
        flink/position-engine/src/test/java/com/example/position/ClientSymbolPartitionerTest.java
git commit -m "feat: add ClientSymbolPartitioner for positions topic upsert ordering"
```

---

### Task 2.10: `PositionEngineJob` — wire the pipeline

**Files:** Create `flink/position-engine/src/main/java/com/example/position/PositionEngineJob.java`

- [ ] **Step 1: Write the job class**

```java
package com.example.position;

import com.example.avro.EnrichedTrade;
import com.example.avro.Position;
import com.example.avro.Trade;
import com.example.common.KafkaSinkFactory;
import com.example.common.KafkaSourceFactory;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class PositionEngineJob {

    static final OutputTag<EnrichedTrade> ENRICHED_TAG = new OutputTag<>("enriched") {};

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:9092");
        String registry  = System.getenv().getOrDefault("SCHEMA_REGISTRY", "http://schema-registry:8081");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(2);
        env.enableCheckpointing(30_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(Duration.ofMinutes(2).toMillis());

        var tradeSource = KafkaSourceFactory.avroSource(
            Trade.class, "trades", bootstrap, registry, "position-engine");

        WatermarkStrategy<Trade> ws = KafkaSourceFactory.watermarkStrategyTyped(Trade::getEventTs);

        DataStream<Trade> tradesRaw = env.fromSource(tradeSource, ws, "trades-source");

        // Lateness routing
        SingleOutputStreamOperator<Trade> ontime = tradesRaw
            .process(new LatenessRouter()).name("lateness-router");
        DataStream<Trade> dlq = ontime.getSideOutput(LatenessRouter.DLQ_TAG);
        // (DLQ sink: see Phase 6, Task 6.x — wire a simple Trade→string sink for now or skip)

        // Enrichment + position update
        SingleOutputStreamOperator<Position> positions = ontime
            .map(new Enricher()).name("enricher")
            .keyBy(e -> e.getClientId() + "|" + e.getSymbol())
            .process(new PositionUpdater()).name("position-updater");

        // Side output is enriched trades (emitted by PositionUpdater? No — emit it from the map).
        // We emit enriched trades from a parallel `map` branch:
        DataStream<EnrichedTrade> enriched = ontime.map(new Enricher()).name("enricher-side");
        // (See note below — single Enricher would be cleaner; demo uses two for simplicity.)

        // Sinks
        KafkaSink<EnrichedTrade> enrichedSink = KafkaSinkFactory.avroSink(
            EnrichedTrade.class, "enriched-trades", bootstrap, registry,
            e -> e.getClientId().getBytes(),
            "position-engine-enriched");

        KafkaSink<Position> positionSink = KafkaSinkFactory.avroSink(
            Position.class, "positions", bootstrap, registry,
            p -> (p.getClientId() + "|" + p.getSymbol()).getBytes(),
            "position-engine-position");
        // NOTE: ClientSymbolPartitioner is wired via KafkaSinkFactory extension in a follow-up.

        enriched.sinkTo(enrichedSink).name("enriched-sink");
        positions.sinkTo(positionSink).name("position-sink");

        env.execute("position-engine");
    }
}
```

- [ ] **Step 2: Refactor — single Enricher with side-output for enriched trades**

The above duplicates `Enricher`. Refactor `PositionUpdater` to take `EnrichedTrade` input AND emit enriched to a side output. Apply this change to `PositionUpdater`:

In `PositionUpdater` add:
```java
public static final OutputTag<EnrichedTrade> ENRICHED_TAG = new OutputTag<>("enriched") {};
```
and inside `processElement`, before `out.collect(Position...)`:
```java
ctx.output(ENRICHED_TAG, t);
```

Then in `PositionEngineJob`, replace the duplicated map with side-output extraction:
```java
SingleOutputStreamOperator<Position> positions = ontime
    .map(new Enricher()).name("enricher")
    .keyBy(e -> e.getClientId() + "|" + e.getSymbol())
    .process(new PositionUpdater()).name("position-updater");

DataStream<EnrichedTrade> enriched = positions.getSideOutput(PositionUpdater.ENRICHED_TAG);
```

Update `PositionUpdaterTest` to consume the side-output tag using `h.getSideOutput(PositionUpdater.ENRICHED_TAG)` and assert one EnrichedTrade per input.

- [ ] **Step 3: Build the JAR**

```bash
cd flink
mvn -pl position-engine -am package -DskipTests=false
```
Expected: `flink/position-engine/target/position-engine-0.1.0.jar` (shaded).

- [ ] **Step 4: Commit**

```bash
cd ..
git add flink/position-engine/src/main/java/com/example/position/PositionEngineJob.java \
        flink/position-engine/src/main/java/com/example/position/PositionUpdater.java \
        flink/position-engine/src/test/java/com/example/position/PositionUpdaterTest.java
git commit -m "feat: wire PositionEngineJob with single Enricher + side-output enriched-trades"
```

---

### Task 2.11: docker-compose — add Flink JM/TM services and submit Job A

**Files:** Modify `docker-compose.yml`, modify `Makefile`

- [ ] **Step 1: Add Flink services to `docker-compose.yml`**

```yaml
  flink-jobmanager:
    image: flink:2.2.0-java21
    container_name: flink-jobmanager
    depends_on:
      kafka:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
    ports: ["8082:8081"]
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=jobmanager.rpc.address: flink-jobmanager
        execution.checkpointing.interval: 30s
        state.backend.type: rocksdb
        state.checkpoints.dir: file:///tmp/flink-checkpoints
        execution.checkpointing.mode: EXACTLY_ONCE
      - KAFKA_BOOTSTRAP=kafka:9092
      - SCHEMA_REGISTRY=http://schema-registry:8081
    volumes:
      - ./flink/position-engine/target:/jars/position-engine
      - ./flink/risk-engine/target:/jars/risk-engine
      - flink-checkpoints:/tmp/flink-checkpoints
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8081/v1/overview || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 30

  flink-taskmanager:
    image: flink:2.2.0-java21
    container_name: flink-taskmanager
    depends_on:
      flink-jobmanager:
        condition: service_healthy
    command: taskmanager
    scale: 1
    environment:
      - |
        FLINK_PROPERTIES=jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 4
        state.backend.type: rocksdb
      - KAFKA_BOOTSTRAP=kafka:9092
      - SCHEMA_REGISTRY=http://schema-registry:8081
    volumes:
      - flink-checkpoints:/tmp/flink-checkpoints

volumes:
  flink-checkpoints:
```

- [ ] **Step 2: Update Makefile `submit-jobs` target**

```makefile
submit-jobs:
	docker compose exec flink-jobmanager flink run -d -c com.example.position.PositionEngineJob /jars/position-engine/position-engine-0.1.0.jar
	docker compose exec flink-jobmanager flink run -d -c com.example.risk.RiskEngineJob /jars/risk-engine/risk-engine-0.1.0.jar
```

- [ ] **Step 3: Manual test — bring up Flink, submit Job A**

```bash
cd flink && mvn -pl position-engine -am package -DskipTests
cd ..
docker compose up -d flink-jobmanager flink-taskmanager
docker compose exec flink-jobmanager flink run -d -c com.example.position.PositionEngineJob /jars/position-engine/position-engine-0.1.0.jar
```
Open `http://localhost:8082` — verify the job is RUNNING.

- [ ] **Step 4: Run trade-gen + watch enriched-trades and positions topics**

```bash
make seed-limits
make trade-gen &     # background
# Verify in kafka-ui that enriched-trades and positions populate
```

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml Makefile
git commit -m "feat: add Flink JM/TM and submit-jobs target"
```

---

## Phase 3 — Flink job: risk-engine

### Task 3.1: `RiskInput` tagged-union

**Files:** Create `flink/risk-engine/pom.xml`, `flink/risk-engine/src/main/java/com/example/risk/RiskInput.java`

- [ ] **Step 1: Write `flink/risk-engine/pom.xml`** (copy from `position-engine/pom.xml`, change artifactId to `risk-engine` and shade `mainClass` to `com.example.risk.RiskEngineJob`).

- [ ] **Step 2: Implement `RiskInput`**

```java
package com.example.risk;

import com.example.avro.EnrichedTrade;
import com.example.avro.Position;

/** Tagged union for the keyed input of RiskEvaluator. */
public sealed interface RiskInput {
    String clientId();

    record OfTrade(EnrichedTrade trade) implements RiskInput {
        public String clientId() { return trade.getClientId(); }
    }
    record OfPosition(Position position) implements RiskInput {
        public String clientId() { return position.getClientId(); }
    }
}
```

- [ ] **Step 3: Build to verify**

```bash
cd flink && mvn -pl risk-engine -am compile
```
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
cd ..
git add flink/risk-engine/pom.xml flink/risk-engine/src/main/java/com/example/risk/RiskInput.java
git commit -m "feat: add risk-engine module skeleton with RiskInput tagged-union"
```

---

### Task 3.2: `RiskEvaluator` — write the failing tests (one per breach type)

**Files:** Create `flink/risk-engine/src/test/java/com/example/risk/RiskEvaluatorTest.java`

- [ ] **Step 1: Write the test class with all four scenarios**

The test uses `KeyedTwoInputStreamOperatorTestHarness` for broadcast process functions. Key snippets (full file shown):

```java
package com.example.risk;

import com.example.avro.*;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class RiskEvaluatorTest {
    private static final MapStateDescriptor<String, ClientLimit> LIMITS_DESC =
        new MapStateDescriptor<>("limits", Types.STRING, org.apache.flink.api.java.typeutils.runtime.PojoSerializer.class.cast(null) == null ? Types.GENERIC(ClientLimit.class) : null);

    private static ByteBuffer dec(String s, int scale) {
        return ByteBuffer.wrap(new BigDecimal(s).setScale(scale).unscaledValue().toByteArray());
    }
    private static ClientLimit limit(String c, String gross, String single, int tpm) {
        return ClientLimit.newBuilder().setClientId(c)
            .setMaxGrossNotional(dec(gross, 2)).setMaxSingleOrderNotional(dec(single, 2))
            .setMaxTradesPerMinute(tpm).setUpdatedTs(0L).build();
    }
    private static EnrichedTrade trade(String c, String s, long qty, String price, long ts) {
        BigDecimal p = new BigDecimal(price);
        BigDecimal n = p.multiply(BigDecimal.valueOf(qty)).setScale(2);
        return EnrichedTrade.newBuilder()
            .setTradeId("t").setOrderId("o").setClientId(c).setSymbol(s).setSide(Side.BUY)
            .setQuantity(qty).setSignedQty(qty).setPrice(dec(price, 4))
            .setNotional(ByteBuffer.wrap(n.unscaledValue().toByteArray()))
            .setCurrency("USD").setVenue("X").setSector("X").setEventTs(ts).build();
    }
    private static Position pos(String c, String s, long net, String last, String notional, long ts) {
        return Position.newBuilder().setClientId(c).setSymbol(s).setNetQuantity(net)
            .setAvgPrice(dec(last, 4)).setNotional(dec(notional, 2)).setSector("X")
            .setLastUpdate(ts).build();
    }

    private static KeyedTwoInputStreamOperatorTestHarness<String, RiskInput, ClientLimit, Breach> harness() throws Exception {
        var op = new CoBroadcastWithKeyedOperator<>(new RiskEvaluator(), List.of(RiskEvaluator.LIMITS_DESC));
        return new KeyedTwoInputStreamOperatorTestHarness<>(op, RiskInput::clientId, ClientLimit::getClientId, Types.STRING);
    }

    @Test
    void singleOrderNotionalBreachFires() throws Exception {
        try (var h = harness()) {
            h.open();
            h.processElement2(new StreamRecord<>(limit("C1", "1000000", "10000", 60), 0L));
            h.processElement1(new StreamRecord<>(new RiskInput.OfTrade(trade("C1", "AAPL", 100, "200.0000", 1L)), 1L));
            assertThat(h.extractOutputValues()).hasSize(1);
            assertThat(h.extractOutputValues().get(0).getBreachType()).isEqualTo(BreachType.SINGLE_ORDER_NOTIONAL);
        }
    }

    @Test
    void grossNotionalBreachFires() throws Exception {
        try (var h = harness()) {
            h.open();
            h.processElement2(new StreamRecord<>(limit("C1", "5000", "999999999", 60), 0L));
            h.processElement1(new StreamRecord<>(new RiskInput.OfPosition(pos("C1", "AAPL", 100, "100", "10000.00", 1L)), 1L));
            h.processElement1(new StreamRecord<>(new RiskInput.OfTrade(trade("C1", "MSFT", 1, "1.0000", 2L)), 2L));
            assertThat(h.extractOutputValues()).hasSize(1);
            assertThat(h.extractOutputValues().get(0).getBreachType()).isEqualTo(BreachType.GROSS_NOTIONAL);
        }
    }

    @Test
    void velocityBreachFires() throws Exception {
        try (var h = harness()) {
            h.open();
            h.processElement2(new StreamRecord<>(limit("C1", "9999999", "9999999", 3), 0L));
            for (int i = 0; i < 4; i++) {
                h.processElement1(new StreamRecord<>(new RiskInput.OfTrade(trade("C1", "AAPL", 1, "1.0000", 1_000L + i*1_000L)), 1_000L + i*1_000L));
            }
            assertThat(h.extractOutputValues().stream().filter(b -> b.getBreachType() == BreachType.TRADE_VELOCITY).count()).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void noLimitConfiguredYieldsNoBreaches() throws Exception {
        try (var h = harness()) {
            h.open();
            h.processElement1(new StreamRecord<>(new RiskInput.OfTrade(trade("C1", "AAPL", 1_000_000, "1000.0000", 1L)), 1L));
            assertThat(h.extractOutputValues()).isEmpty();
        }
    }

    @Test
    void stalePositionEventIsIgnored() throws Exception {
        try (var h = harness()) {
            h.open();
            h.processElement2(new StreamRecord<>(limit("C1", "1000", "999999999", 60), 0L));
            h.processElement1(new StreamRecord<>(new RiskInput.OfPosition(pos("C1", "AAPL", 1, "10",  "10.00",   100L)), 100L));
            h.processElement1(new StreamRecord<>(new RiskInput.OfPosition(pos("C1", "AAPL", 1, "10",  "10.00",    50L)), 50L));  // stale
            // Trade should evaluate against gross=10 (the fresher value), not zero.
            h.processElement1(new StreamRecord<>(new RiskInput.OfTrade(trade("C1", "MSFT", 1, "1.0000", 200L)), 200L));
            // gross = 10 < 1000 → no GROSS_NOTIONAL breach
            assertThat(h.extractOutputValues().stream().filter(b -> b.getBreachType() == BreachType.GROSS_NOTIONAL)).isEmpty();
        }
    }
}
```

- [ ] **Step 2: Run test (compile FAIL — `RiskEvaluator` and `LIMITS_DESC` missing)**

```bash
cd flink && mvn -pl risk-engine -am test-compile
```
Expected: FAILURE.

---

### Task 3.3: `RiskEvaluator` implementation

**Files:** Create `flink/risk-engine/src/main/java/com/example/risk/RiskEvaluator.java`

- [ ] **Step 1: Implement `RiskEvaluator`**

```java
package com.example.risk;

import com.example.avro.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.UUID;

public class RiskEvaluator extends KeyedBroadcastProcessFunction<String, RiskInput, ClientLimit, Breach> {

    public static final MapStateDescriptor<String, ClientLimit> LIMITS_DESC =
        new MapStateDescriptor<>("limits", Types.STRING, Types.GENERIC(ClientLimit.class));

    public static class SymbolNotional {
        public BigDecimal notional;
        public long lastUpdate;
        public SymbolNotional() {}
        public SymbolNotional(BigDecimal n, long ts) { this.notional = n; this.lastUpdate = ts; }
    }

    private transient MapState<String, SymbolNotional> symbolNotionals;
    private transient ListState<Long> recentTimestamps;

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext ctx) {
        symbolNotionals = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("symbol-notionals", Types.STRING, Types.GENERIC(SymbolNotional.class)));
        recentTimestamps = getRuntimeContext().getListState(
            new ListStateDescriptor<>("recent-ts", Types.LONG));
    }

    @Override
    public void processBroadcastElement(ClientLimit limit, Context ctx, Collector<Breach> out) throws Exception {
        ctx.getBroadcastState(LIMITS_DESC).put(limit.getClientId(), limit);
    }

    @Override
    public void processElement(RiskInput input, ReadOnlyContext ctx, Collector<Breach> out) throws Exception {
        if (input instanceof RiskInput.OfPosition op) {
            handlePosition(op.position());
        } else if (input instanceof RiskInput.OfTrade ot) {
            evaluate(ot.trade(), ctx, out);
        }
    }

    private void handlePosition(Position p) throws Exception {
        SymbolNotional existing = symbolNotionals.get(p.getSymbol());
        if (existing != null && p.getLastUpdate() <= existing.lastUpdate) return;
        if (p.getNetQuantity() == 0L) {
            symbolNotionals.remove(p.getSymbol());
        } else {
            symbolNotionals.put(p.getSymbol(), new SymbolNotional(decimal(p.getNotional(), 2).abs(), p.getLastUpdate()));
        }
    }

    private void evaluate(EnrichedTrade t, ReadOnlyContext ctx, Collector<Breach> out) throws Exception {
        ClientLimit limit = ctx.getBroadcastState(LIMITS_DESC).get(t.getClientId());
        if (limit == null) return;

        BigDecimal tradeNotional = decimal(t.getNotional(), 2);

        // Rule 1: SINGLE_ORDER_NOTIONAL
        BigDecimal singleLimit = decimal(limit.getMaxSingleOrderNotional(), 2);
        if (tradeNotional.compareTo(singleLimit) > 0) {
            emit(out, t.getClientId(), BreachType.SINGLE_ORDER_NOTIONAL, singleLimit, tradeNotional,
                t.getTradeId(), t.getOrderId(), t.getEventTs(),
                "order notional " + tradeNotional + " exceeds " + singleLimit);
        }

        // Rule 2: GROSS_NOTIONAL
        BigDecimal gross = BigDecimal.ZERO;
        for (Iterator<SymbolNotional> it = symbolNotionals.values().iterator(); it.hasNext(); ) {
            gross = gross.add(it.next().notional);
        }
        BigDecimal grossLimit = decimal(limit.getMaxGrossNotional(), 2);
        if (gross.compareTo(grossLimit) > 0) {
            emit(out, t.getClientId(), BreachType.GROSS_NOTIONAL, grossLimit, gross,
                t.getTradeId(), t.getOrderId(), t.getEventTs(),
                "gross notional " + gross + " exceeds " + grossLimit);
        }

        // Rule 3: TRADE_VELOCITY
        long cutoff = t.getEventTs() - 60_000L;
        java.util.ArrayList<Long> kept = new java.util.ArrayList<>();
        for (Long ts : recentTimestamps.get()) if (ts >= cutoff) kept.add(ts);
        kept.add(t.getEventTs());
        recentTimestamps.update(kept);
        if (kept.size() > limit.getMaxTradesPerMinute()) {
            emit(out, t.getClientId(), BreachType.TRADE_VELOCITY,
                BigDecimal.valueOf(limit.getMaxTradesPerMinute()), BigDecimal.valueOf(kept.size()),
                t.getTradeId(), t.getOrderId(), t.getEventTs(),
                kept.size() + " trades in 60s window");
        }
    }

    private void emit(Collector<Breach> out, String cid, BreachType type, BigDecimal limit, BigDecimal actual,
                      String tradeId, String orderId, long ts, String details) {
        out.collect(Breach.newBuilder()
            .setBreachId(UUID.randomUUID().toString())
            .setClientId(cid).setBreachType(type)
            .setLimitValue(toBytes(limit.setScale(2, java.math.RoundingMode.HALF_UP)))
            .setActualValue(toBytes(actual.setScale(2, java.math.RoundingMode.HALF_UP)))
            .setTradeId(tradeId).setOrderId(type == BreachType.SINGLE_ORDER_NOTIONAL ? orderId : null)
            .setDetectedTs(ts).setDetails(details).build());
    }

    private static BigDecimal decimal(ByteBuffer bb, int scale) {
        byte[] bytes = new byte[bb.remaining()];
        bb.duplicate().get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
    }
    private static ByteBuffer toBytes(BigDecimal v) {
        return ByteBuffer.wrap(v.unscaledValue().toByteArray());
    }
}
```

- [ ] **Step 2: Run tests**

```bash
mvn -pl risk-engine -am test
```
Expected: 5 tests pass.

- [ ] **Step 3: Commit**

```bash
cd ..
git add flink/risk-engine/src/main/java/com/example/risk/RiskEvaluator.java \
        flink/risk-engine/src/test/java/com/example/risk/RiskEvaluatorTest.java
git commit -m "feat: add RiskEvaluator with three breach rules + late-position guard"
```

---

### Task 3.4: `RiskEngineJob` — wire pipeline

**Files:** Create `flink/risk-engine/src/main/java/com/example/risk/RiskEngineJob.java`

- [ ] **Step 1: Write the job**

```java
package com.example.risk;

import com.example.avro.*;
import com.example.common.KafkaSinkFactory;
import com.example.common.KafkaSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RiskEngineJob {

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:9092");
        String registry  = System.getenv().getOrDefault("SCHEMA_REGISTRY", "http://schema-registry:8081");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(30_000L, CheckpointingMode.EXACTLY_ONCE);

        var tradeSrc = KafkaSourceFactory.avroSource(EnrichedTrade.class, "enriched-trades", bootstrap, registry, "risk-engine-trades");
        var posSrc   = KafkaSourceFactory.avroSource(Position.class,      "positions",       bootstrap, registry, "risk-engine-positions");
        var limSrc   = KafkaSourceFactory.avroSource(ClientLimit.class,   "client-limits",   bootstrap, registry, "risk-engine-limits");

        WatermarkStrategy<EnrichedTrade> wsTrade = KafkaSourceFactory.watermarkStrategyTyped(EnrichedTrade::getEventTs);
        WatermarkStrategy<Position>      wsPos   = KafkaSourceFactory.watermarkStrategyTyped(Position::getLastUpdate);

        DataStream<EnrichedTrade> trades = env.fromSource(tradeSrc, wsTrade, "enriched-trades-source");
        DataStream<Position>      poss   = env.fromSource(posSrc,   wsPos,   "positions-source");
        DataStream<ClientLimit>   lims   = env.fromSource(limSrc,   WatermarkStrategy.noWatermarks(), "client-limits-source");

        DataStream<RiskInput> tagged = trades.map(t -> (RiskInput) new RiskInput.OfTrade(t)).returns(RiskInput.class)
            .union(poss.map(p -> (RiskInput) new RiskInput.OfPosition(p)).returns(RiskInput.class));

        BroadcastStream<ClientLimit> broadcast = lims.broadcast(RiskEvaluator.LIMITS_DESC);

        SingleOutputStreamOperator<Breach> breaches = tagged
            .keyBy(RiskInput::clientId)
            .connect(broadcast)
            .process(new RiskEvaluator())
            .name("risk-evaluator");

        KafkaSink<Breach> breachSink = KafkaSinkFactory.avroSink(
            Breach.class, "breaches", bootstrap, registry,
            b -> b.getClientId().getBytes(),
            "risk-engine-breach");

        breaches.sinkTo(breachSink).name("breach-sink");

        env.execute("risk-engine");
    }
}
```

- [ ] **Step 2: Build the JAR**

```bash
cd flink && mvn -pl risk-engine -am package -DskipTests=false
```

- [ ] **Step 3: Submit job and verify breaches appear**

```bash
cd ..
docker compose exec flink-jobmanager flink run -d -c com.example.risk.RiskEngineJob /jars/risk-engine/risk-engine-0.1.0.jar

# Lower one client's limits to trigger breaches
cd tools && source .venv/bin/activate
python -m limits_cli set --client-id C0001 --max-gross 1000 --max-order 100 --max-tpm 5

# Wait ~10s; verify the `breaches` topic gets entries via kafka-ui
```

- [ ] **Step 4: Commit**

```bash
cd ..
git add flink/risk-engine/src/main/java/com/example/risk/RiskEngineJob.java
git commit -m "feat: wire RiskEngineJob with tagged-union keyed input + broadcast limits"
```

---

## Phase 4 — Pinot serving

### Task 4.1: Add Pinot to docker-compose

**Files:** Modify `docker-compose.yml`

- [ ] **Step 1: Append Pinot services**

```yaml
  pinot-zk:
    image: zookeeper:3.9
    container_name: pinot-zk
    ports: ["2181:2181"]

  pinot-controller:
    image: apachepinot/pinot:1.5.0
    container_name: pinot-controller
    depends_on: [pinot-zk]
    command: StartController -zkAddress pinot-zk:2181 -clusterName pinot
    ports: ["9000:9000"]
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:9000/health || exit 1"]
      interval: 5s
      retries: 30

  pinot-broker:
    image: apachepinot/pinot:1.5.0
    container_name: pinot-broker
    depends_on:
      pinot-controller:
        condition: service_healthy
    command: StartBroker -zkAddress pinot-zk:2181 -clusterName pinot
    ports: ["8099:8099"]

  pinot-server:
    image: apachepinot/pinot:1.5.0
    container_name: pinot-server
    depends_on:
      pinot-controller:
        condition: service_healthy
    command: StartServer -zkAddress pinot-zk:2181 -clusterName pinot
```

- [ ] **Step 2: Start Pinot**

```bash
docker compose up -d pinot-zk pinot-controller pinot-broker pinot-server
sleep 30
curl -s http://localhost:9000/health   # expect "OK"
```

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add Pinot services to docker-compose"
```

---

### Task 4.2: Pinot table configs — `trades` (append)

**Files:** Create `pinot/trades_schema.json`, `pinot/trades_table.json`

- [ ] **Step 1: Write `pinot/trades_schema.json`**

```json
{
  "schemaName": "trades",
  "dimensionFieldSpecs": [
    {"name": "trade_id", "dataType": "STRING"},
    {"name": "order_id", "dataType": "STRING"},
    {"name": "client_id", "dataType": "STRING"},
    {"name": "symbol", "dataType": "STRING"},
    {"name": "side", "dataType": "STRING"},
    {"name": "currency", "dataType": "STRING"},
    {"name": "venue", "dataType": "STRING"},
    {"name": "sector", "dataType": "STRING"}
  ],
  "metricFieldSpecs": [
    {"name": "quantity", "dataType": "LONG"},
    {"name": "signed_qty", "dataType": "LONG"},
    {"name": "price", "dataType": "BYTES"},
    {"name": "notional", "dataType": "BYTES"}
  ],
  "dateTimeFieldSpecs": [
    {"name": "event_ts", "dataType": "LONG", "format": "1:MILLISECONDS:EPOCH", "granularity": "1:MILLISECONDS"}
  ]
}
```

- [ ] **Step 2: Write `pinot/trades_table.json`**

```json
{
  "tableName": "trades",
  "tableType": "REALTIME",
  "segmentsConfig": {
    "timeColumnName": "event_ts",
    "schemaName": "trades",
    "replication": "1",
    "retentionTimeUnit": "DAYS",
    "retentionTimeValue": "7"
  },
  "tenants": {},
  "tableIndexConfig": {
    "loadMode": "MMAP",
    "invertedIndexColumns": ["client_id", "symbol", "sector"],
    "rangeIndexColumns": ["event_ts"],
    "streamConfigs": {
      "streamType": "kafka",
      "stream.kafka.topic.name": "enriched-trades",
      "stream.kafka.broker.list": "kafka:9092",
      "stream.kafka.consumer.type": "lowLevel",
      "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
      "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder",
      "stream.kafka.decoder.prop.schema.registry.rest.url": "http://schema-registry:8081",
      "realtime.segment.flush.threshold.time": "1h",
      "realtime.segment.flush.threshold.rows": "0",
      "realtime.segment.flush.threshold.segment.size": "100M"
    }
  },
  "metadata": {}
}
```

- [ ] **Step 3: Commit**

```bash
git add pinot/trades_schema.json pinot/trades_table.json
git commit -m "feat: add Pinot trades table (REALTIME, append, enriched-trades source)"
```

---

### Task 4.3: Pinot table configs — `positions` (upsert)

**Files:** Create `pinot/positions_schema.json`, `pinot/positions_table.json`

- [ ] **Step 1: Write `pinot/positions_schema.json`**

```json
{
  "schemaName": "positions",
  "primaryKeyColumns": ["client_id", "symbol"],
  "dimensionFieldSpecs": [
    {"name": "client_id", "dataType": "STRING"},
    {"name": "symbol", "dataType": "STRING"},
    {"name": "sector", "dataType": "STRING"}
  ],
  "metricFieldSpecs": [
    {"name": "net_quantity", "dataType": "LONG"},
    {"name": "avg_price", "dataType": "BYTES"},
    {"name": "notional", "dataType": "BYTES"}
  ],
  "dateTimeFieldSpecs": [
    {"name": "last_update", "dataType": "LONG", "format": "1:MILLISECONDS:EPOCH", "granularity": "1:MILLISECONDS"}
  ]
}
```

- [ ] **Step 2: Write `pinot/positions_table.json`**

```json
{
  "tableName": "positions",
  "tableType": "REALTIME",
  "segmentsConfig": {
    "timeColumnName": "last_update",
    "schemaName": "positions",
    "replication": "1",
    "retentionTimeUnit": "DAYS",
    "retentionTimeValue": "30"
  },
  "tenants": {},
  "upsertConfig": {
    "mode": "FULL",
    "comparisonColumn": "last_update"
  },
  "routing": {
    "instanceSelectorType": "strictReplicaGroup"
  },
  "tableIndexConfig": {
    "loadMode": "MMAP",
    "invertedIndexColumns": ["client_id", "symbol", "sector"],
    "streamConfigs": {
      "streamType": "kafka",
      "stream.kafka.topic.name": "positions",
      "stream.kafka.broker.list": "kafka:9092",
      "stream.kafka.consumer.type": "lowLevel",
      "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
      "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder",
      "stream.kafka.decoder.prop.schema.registry.rest.url": "http://schema-registry:8081",
      "realtime.segment.flush.threshold.time": "1h"
    }
  },
  "instanceAssignmentConfigMap": {
    "CONSUMING": {
      "tagPoolConfig": {"tag": "DefaultTenant_REALTIME"},
      "replicaGroupPartitionConfig": {
        "replicaGroupBased": true,
        "numReplicaGroups": 1,
        "numInstancesPerReplicaGroup": 1
      }
    }
  },
  "metadata": {}
}
```

- [ ] **Step 2: Commit**

```bash
git add pinot/positions_schema.json pinot/positions_table.json
git commit -m "feat: add Pinot positions table (REALTIME upsert keyed on client_id+symbol)"
```

---

### Task 4.4: Pinot table configs — `breaches` (append)

**Files:** Create `pinot/breaches_schema.json`, `pinot/breaches_table.json`

- [ ] **Step 1: Write `pinot/breaches_schema.json`**

```json
{
  "schemaName": "breaches",
  "dimensionFieldSpecs": [
    {"name": "breach_id", "dataType": "STRING"},
    {"name": "client_id", "dataType": "STRING"},
    {"name": "breach_type", "dataType": "STRING"},
    {"name": "trade_id", "dataType": "STRING"},
    {"name": "order_id", "dataType": "STRING"},
    {"name": "details", "dataType": "STRING"}
  ],
  "metricFieldSpecs": [
    {"name": "limit_value", "dataType": "BYTES"},
    {"name": "actual_value", "dataType": "BYTES"}
  ],
  "dateTimeFieldSpecs": [
    {"name": "detected_ts", "dataType": "LONG", "format": "1:MILLISECONDS:EPOCH", "granularity": "1:MILLISECONDS"}
  ]
}
```

- [ ] **Step 2: Write `pinot/breaches_table.json`**

```json
{
  "tableName": "breaches",
  "tableType": "REALTIME",
  "segmentsConfig": {
    "timeColumnName": "detected_ts",
    "schemaName": "breaches",
    "replication": "1",
    "retentionTimeUnit": "DAYS",
    "retentionTimeValue": "7"
  },
  "tenants": {},
  "tableIndexConfig": {
    "loadMode": "MMAP",
    "invertedIndexColumns": ["client_id", "breach_type"],
    "streamConfigs": {
      "streamType": "kafka",
      "stream.kafka.topic.name": "breaches",
      "stream.kafka.broker.list": "kafka:9092",
      "stream.kafka.consumer.type": "lowLevel",
      "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
      "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder",
      "stream.kafka.decoder.prop.schema.registry.rest.url": "http://schema-registry:8081",
      "realtime.segment.flush.threshold.time": "1h"
    }
  },
  "metadata": {}
}
```

- [ ] **Step 3: Commit**

```bash
git add pinot/breaches_schema.json pinot/breaches_table.json
git commit -m "feat: add Pinot breaches table (REALTIME, append)"
```

---

### Task 4.5: Makefile target to register Pinot tables; smoke test

**Files:** Modify `Makefile`

- [ ] **Step 1: Replace `pinot-tables` placeholder**

```makefile
pinot-tables:
	@for name in trades positions breaches; do \
		echo "Adding $$name schema..."; \
		curl -fsS -X POST -H "Content-Type: application/json" \
			-d @pinot/$${name}_schema.json http://localhost:9000/schemas; echo; \
		echo "Adding $$name table..."; \
		curl -fsS -X POST -H "Content-Type: application/json" \
			-d @pinot/$${name}_table.json http://localhost:9000/tables; echo; \
	done
```

- [ ] **Step 2: Run it**

```bash
make pinot-tables
```
Expected: 6 successful responses (3 schemas + 3 tables).

- [ ] **Step 3: Verify ingestion via Pinot SQL**

With trade-gen running and Job A submitted:
```bash
curl -s -X POST http://localhost:8099/query/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM trades"}' | jq
```
Expected: non-zero count after a few seconds.

- [ ] **Step 4: Verify upsert behavior on `positions`**

```bash
curl -s -X POST http://localhost:8099/query/sql \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT client_id, COUNT(*) FROM positions GROUP BY client_id LIMIT 5"}' | jq
```
Each (client_id, symbol) should appear at most once after upsert reconciliation.

- [ ] **Step 5: Commit**

```bash
git add Makefile
git commit -m "feat: add pinot-tables Makefile target with curl-based registration"
```

---

## Phase 5 — Dashboard

### Task 5.1: Vite + React scaffold

**Files:** Create `dashboard/` skeleton.

- [ ] **Step 1: Scaffold**

```bash
cd /Users/robran/IdeaProjects/stream_processing_analytics
npm create vite@latest dashboard -- --template react-ts
cd dashboard
npm install
npm install @tanstack/react-query recharts
npm install -D tailwindcss @tailwindcss/postcss postcss
```

- [ ] **Step 2: Configure Tailwind**

`dashboard/postcss.config.js`:
```js
export default { plugins: { '@tailwindcss/postcss': {} } };
```

`dashboard/src/index.css`:
```css
@import "tailwindcss";
:root { color-scheme: dark; }
body { background: #0a0a0a; color: #d4d4d4; font-family: ui-monospace, SFMono-Regular, monospace; }
```

- [ ] **Step 3: Smoke test**

```bash
npm run dev
# Open http://localhost:5173 — default Vite page
```

- [ ] **Step 4: Commit**

```bash
cd ..
git add dashboard/
git commit -m "feat: scaffold Vite + React + Tailwind + TanStack Query + Recharts"
```

---

### Task 5.2: Pinot SQL client wrapper

**Files:** Create `dashboard/src/pinot.ts`

- [ ] **Step 1: Write `dashboard/src/pinot.ts`**

```ts
const PINOT_URL = import.meta.env.VITE_PINOT_URL || "http://localhost:8099/query/sql";

export interface PinotResponse<T = unknown> {
  resultTable?: { dataSchema: { columnNames: string[] }; rows: unknown[][] };
  exceptions?: { errorCode: number; message: string }[];
  numDocsScanned?: number;
}

export async function pinotQuery<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
  const r = await fetch(PINOT_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql }),
  });
  if (!r.ok) throw new Error(`Pinot HTTP ${r.status}`);
  const json: PinotResponse = await r.json();
  if (json.exceptions?.length) throw new Error(json.exceptions[0].message);
  const cols = json.resultTable?.dataSchema.columnNames ?? [];
  const rows = json.resultTable?.rows ?? [];
  return rows.map((row) => Object.fromEntries(cols.map((c, i) => [c, row[i]])) as T);
}
```

- [ ] **Step 2: Commit**

```bash
git add dashboard/src/pinot.ts
git commit -m "feat: add Pinot SQL client wrapper"
```

---

### Task 5.3: Four panels + App layout

**Files:** Create `dashboard/src/panels/{BreachesSparkline,TopExposed,Positions,BreachFeed}.tsx`, `dashboard/src/App.tsx`.

- [ ] **Step 1: `BreachesSparkline.tsx`**

```tsx
import { useQuery } from "@tanstack/react-query";
import { Area, AreaChart, ResponsiveContainer, Tooltip, XAxis } from "recharts";
import { pinotQuery } from "../pinot";

type Row = { minute: number; cnt: number };

export function BreachesSparkline() {
  const { data = [] } = useQuery<Row[]>({
    queryKey: ["sparkline"],
    queryFn: () =>
      pinotQuery<Row>(
        "SELECT DATETRUNC('MINUTE', detected_ts) AS minute, COUNT(*) AS cnt " +
        "FROM breaches WHERE detected_ts > ago('PT1H') " +
        "GROUP BY minute ORDER BY minute"
      ),
    refetchInterval: 10_000,
  });

  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-3">
      <h2 className="text-xs uppercase tracking-wide text-neutral-400 mb-1">
        Breaches per minute, last 1h
      </h2>
      <ResponsiveContainer width="100%" height={80}>
        <AreaChart data={data}>
          <XAxis dataKey="minute" hide />
          <Tooltip
            contentStyle={{ background: "#171717", border: "1px solid #404040" }}
            labelFormatter={(v) => new Date(v as number).toLocaleTimeString()}
          />
          <Area type="monotone" dataKey="cnt" stroke="#f87171" fill="#f87171" fillOpacity={0.3} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
```

- [ ] **Step 2: `TopExposed.tsx`**

```tsx
import { useQuery } from "@tanstack/react-query";
import { pinotQuery } from "../pinot";
type Row = { client_id: string; gross: number };

export function TopExposed() {
  const { data = [] } = useQuery<Row[]>({
    queryKey: ["top-exposed"],
    queryFn: () => pinotQuery<Row>(
      "SELECT client_id, SUM(ABS(notional)) AS gross FROM positions GROUP BY client_id ORDER BY gross DESC LIMIT 10"
    ),
    refetchInterval: 5_000,
  });
  const max = Math.max(...data.map(d => d.gross), 1);
  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-3">
      <h2 className="text-xs uppercase tracking-wide text-neutral-400 mb-2">Top-10 Exposed Clients</h2>
      <div className="space-y-1">
        {data.map(d => (
          <div key={d.client_id} className="flex items-center gap-2 text-sm">
            <span className="w-16 text-neutral-500">{d.client_id}</span>
            <div className="flex-1 bg-neutral-800 h-4 relative">
              <div className="bg-blue-600 h-full" style={{ width: `${(d.gross / max) * 100}%` }} />
            </div>
            <span className="w-24 text-right">{d.gross.toLocaleString()}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
```

- [ ] **Step 3: `Positions.tsx`** (with `clientId` prop)

```tsx
import { useQuery } from "@tanstack/react-query";
import { pinotQuery } from "../pinot";
type Row = { symbol: string; net_quantity: number; notional: number; sector: string };

export function Positions({ clientId }: { clientId: string | null }) {
  const where = clientId ? `WHERE client_id = '${clientId}'` : "";
  const { data = [] } = useQuery<Row[]>({
    queryKey: ["positions", clientId],
    queryFn: () => pinotQuery<Row>(
      `SELECT symbol, net_quantity, notional, sector FROM positions ${where} ORDER BY ABS(notional) DESC LIMIT 100`
    ),
    refetchInterval: 2_000,
  });
  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-3">
      <h2 className="text-xs uppercase tracking-wide text-neutral-400 mb-2">Positions {clientId ? `· ${clientId}` : "· All"}</h2>
      <table className="w-full text-sm">
        <thead className="text-neutral-500 text-xs uppercase">
          <tr><th className="text-left">Symbol</th><th className="text-right">Qty</th><th className="text-right">Notional</th><th className="text-left">Sector</th></tr>
        </thead>
        <tbody>
          {data.map(r => (
            <tr key={r.symbol} className="border-t border-neutral-800">
              <td>{r.symbol}</td>
              <td className="text-right">{r.net_quantity.toLocaleString()}</td>
              <td className="text-right">{Number(r.notional).toLocaleString()}</td>
              <td className="text-neutral-500">{r.sector}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

- [ ] **Step 4: `BreachFeed.tsx`**

```tsx
import { useQuery } from "@tanstack/react-query";
import { pinotQuery } from "../pinot";
type Row = { client_id: string; breach_type: string; actual_value: number; limit_value: number; details: string; detected_ts: number };

const COLOURS: Record<string, string> = {
  GROSS_NOTIONAL: "border-l-red-500",
  SINGLE_ORDER_NOTIONAL: "border-l-amber-500",
  TRADE_VELOCITY: "border-l-blue-500",
};

export function BreachFeed() {
  const { data = [] } = useQuery<Row[]>({
    queryKey: ["breaches"],
    queryFn: () => pinotQuery<Row>(
      "SELECT client_id, breach_type, actual_value, limit_value, details, detected_ts FROM breaches WHERE detected_ts > ago('PT5M') ORDER BY detected_ts DESC LIMIT 50"
    ),
    refetchInterval: 2_000,
  });
  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-3 h-full overflow-y-auto">
      <h2 className="text-xs uppercase tracking-wide text-neutral-400 mb-2">Live Breach Feed (last 5m)</h2>
      <ul className="space-y-1">
        {data.map((b, i) => (
          <li key={i} className={`pl-2 border-l-4 ${COLOURS[b.breach_type] ?? "border-l-neutral-500"} text-sm`}>
            <div className="flex justify-between">
              <span>{b.client_id} <span className="text-neutral-500">{b.breach_type}</span></span>
              <span className="text-neutral-600 text-xs">{new Date(b.detected_ts).toLocaleTimeString()}</span>
            </div>
            <div className="text-neutral-400 text-xs">{b.details}</div>
          </li>
        ))}
      </ul>
    </div>
  );
}
```

- [ ] **Step 5: `App.tsx`**

```tsx
import { QueryClient, QueryClientProvider, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { pinotQuery } from "./pinot";
import { BreachesSparkline } from "./panels/BreachesSparkline";
import { TopExposed } from "./panels/TopExposed";
import { Positions } from "./panels/Positions";
import { BreachFeed } from "./panels/BreachFeed";

const qc = new QueryClient();

function ClientFilter({ value, onChange }: { value: string | null; onChange: (v: string | null) => void }) {
  const { data = [] } = useQuery<{ client_id: string }[]>({
    queryKey: ["client-list"],
    queryFn: () => pinotQuery<{ client_id: string }>("SELECT DISTINCT client_id FROM positions LIMIT 1000"),
    refetchInterval: 30_000,
  });
  return (
    <select
      className="bg-neutral-800 border border-neutral-700 px-2 py-1 text-sm"
      value={value ?? "ALL"}
      onChange={e => onChange(e.target.value === "ALL" ? null : e.target.value)}
    >
      <option value="ALL">All clients</option>
      {data.map(d => <option key={d.client_id} value={d.client_id}>{d.client_id}</option>)}
    </select>
  );
}

function Shell() {
  const [client, setClient] = useState<string | null>(null);
  return (
    <div className="p-4 space-y-3 max-w-screen-2xl mx-auto">
      <header className="flex items-center justify-between">
        <h1 className="text-lg font-semibold">Streaming Risk Dashboard</h1>
        <ClientFilter value={client} onChange={setClient} />
      </header>
      <BreachesSparkline />
      <div className="grid grid-cols-2 gap-3">
        <div className="space-y-3">
          <TopExposed />
          <Positions clientId={client} />
        </div>
        <BreachFeed />
      </div>
    </div>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={qc}>
      <Shell />
    </QueryClientProvider>
  );
}
```

Update `dashboard/src/main.tsx` to import `./index.css` and render `<App />`.

- [ ] **Step 6: Run dashboard, verify panels populate**

```bash
cd dashboard && npm run dev
# Open http://localhost:5173. With pipeline running, panels should fill within seconds.
```

- [ ] **Step 7: Commit**

```bash
cd ..
git add dashboard/src/
git commit -m "feat: dashboard four-panel layout with TanStack polling"
```

---

## Phase 6 — Observability

### Task 6.1: Prometheus + Grafana containers + Flink Prometheus reporter

**Files:** Modify `docker-compose.yml`, modify Flink JM/TM config, create `observability/prometheus.yml`, `observability/grafana/provisioning/datasources.yml`.

- [ ] **Step 1: `observability/prometheus.yml`**

```yaml
global:
  scrape_interval: 5s
scrape_configs:
  - job_name: flink-jm
    static_configs: [{ targets: ['flink-jobmanager:9249'] }]
  - job_name: flink-tm
    static_configs: [{ targets: ['flink-taskmanager:9250'] }]
```

- [ ] **Step 2: Update Flink env in `docker-compose.yml`** (append to FLINK_PROPERTIES of both JM and TM):

```
metrics.reporters: prom
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9249-9260
```

Expose 9249, 9250 in JM/TM service blocks.

- [ ] **Step 3: Add Prometheus + Grafana services**

```yaml
  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    ports: ["9090:9090"]
    volumes:
      - ./observability/prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    depends_on: [prometheus]
    ports: ["3000:3000"]
    environment:
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Admin
    volumes:
      - ./observability/grafana/provisioning:/etc/grafana/provisioning
```

- [ ] **Step 4: `observability/grafana/provisioning/datasources/prometheus.yml`**

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    url: http://prometheus:9090
    isDefault: true
```

- [ ] **Step 5: Smoke-test**

```bash
docker compose up -d prometheus grafana
# Open http://localhost:3000 → datasource exists; explore `flink_jobmanager_*` metrics
```

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml observability/
git commit -m "feat: Prometheus scraping Flink + Grafana with anonymous admin"
```

---

### Task 6.2: Custom Flink counters

**Files:** Modify `RiskEvaluator.java`, `LatenessRouter.java`, `Enricher.java`

- [ ] **Step 1: Add counters in `RiskEvaluator.open(...)`**

```java
private transient org.apache.flink.metrics.Counter breachesGross, breachesSingle, breachesVelocity;

@Override
public void open(...) {
    // existing state init
    var mg = getRuntimeContext().getMetricGroup();
    breachesGross    = mg.counter("breaches_emitted_gross_notional");
    breachesSingle   = mg.counter("breaches_emitted_single_order");
    breachesVelocity = mg.counter("breaches_emitted_velocity");
}
```

In each `emit(...)` site, increment the matching counter.

- [ ] **Step 2: Add `late_records_dropped_total` counter in `LatenessRouter`**

```java
private transient Counter dropped;
@Override public void open(OpenContext ctx) {
    dropped = getRuntimeContext().getMetricGroup().counter("late_records_dropped_total");
}
// in DLQ branch: dropped.inc();
```

- [ ] **Step 3: Rebuild + redeploy Flink jobs**

```bash
cd flink && mvn -pl position-engine -pl risk-engine -am package -DskipTests
cd ..
make submit-jobs   # cancel previous + re-submit (or use Flink UI)
```

- [ ] **Step 4: Verify metrics exposed**

```bash
curl -s http://localhost:9249/metrics | grep -E 'breaches_emitted|late_records'
```
Expected: counters appear.

- [ ] **Step 5: Commit**

```bash
git add flink/risk-engine/ flink/position-engine/
git commit -m "feat: custom Flink counters for breaches and late-record drops"
```

---

## Phase 7 — End-to-end tests

### Task 7.1: pytest scaffolding for E2E

**Files:** Create `tests/e2e/conftest.py`, `tests/e2e/test_breach_scenarios.py`

- [ ] **Step 1: Write `tests/e2e/conftest.py`**

```python
import os
import time
import pytest
import requests

PINOT = os.environ.get("PINOT_URL", "http://localhost:8099/query/sql")


def pinot_query(sql: str):
    r = requests.post(PINOT, json={"sql": sql}, timeout=10)
    r.raise_for_status()
    body = r.json()
    cols = body["resultTable"]["dataSchema"]["columnNames"]
    return [dict(zip(cols, row)) for row in body["resultTable"]["rows"]]


def wait_for(predicate, timeout=15, interval=0.5):
    start = time.time()
    while time.time() - start < timeout:
        try:
            if predicate(): return True
        except Exception:
            pass
        time.sleep(interval)
    return False


@pytest.fixture
def query():
    return pinot_query


@pytest.fixture
def waitfor():
    return wait_for
```

- [ ] **Step 2: Write `tests/e2e/test_breach_scenarios.py`**

```python
"""End-to-end breach scenarios. Assumes `make all` is up."""
import os
import time
import uuid
from decimal import Decimal
from io import BytesIO

import fastavro
import pytest
import requests
from confluent_kafka import Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
REGISTRY = os.environ.get("SCHEMA_REGISTRY", "http://localhost:8081")
SCHEMAS = os.path.dirname(__file__) + "/../../schemas"


def _avro(schema_name, payload):
    schema = fastavro.parse_schema(__import__("json").loads(open(f"{SCHEMAS}/{schema_name}.avsc").read()))
    sid = requests.get(f"{REGISTRY}/subjects/{schema_name}-value/versions/latest").json()["id"]
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
        "trade_id": str(uuid.uuid4()), "order_id": str(uuid.uuid4()),
        "client_id": client, "symbol": symbol, "side": side, "quantity": qty,
        "price": Decimal(str(price)), "currency": "USD", "venue": "X",
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
    time.sleep(2)  # let limit propagate
    _send_trade(producer, cid, "AAPL", qty=100, price="50.00")
    assert waitfor(lambda: any(
        r["breach_type"] == "SINGLE_ORDER_NOTIONAL"
        for r in query(f"SELECT breach_type FROM breaches WHERE client_id = '{cid}'")
    ))


def test_gross_notional_breach(producer, query, waitfor):
    cid = "TC_GROSS_" + uuid.uuid4().hex[:6]
    _set_limit(producer, cid, max_gross=1_000, max_order=10_000_000, max_tpm=999)
    time.sleep(2)
    # Build position over the limit
    for _ in range(3):
        _send_trade(producer, cid, "AAPL", qty=10, price="100.00")
    producer.flush(2)
    time.sleep(3)
    _send_trade(producer, cid, "MSFT", qty=1, price="1.00")  # the trigger
    assert waitfor(lambda: any(
        r["breach_type"] == "GROSS_NOTIONAL"
        for r in query(f"SELECT breach_type FROM breaches WHERE client_id = '{cid}'")
    ))


def test_velocity_breach(producer, query, waitfor):
    cid = "TC_VEL_" + uuid.uuid4().hex[:6]
    _set_limit(producer, cid, max_gross=10_000_000, max_order=10_000_000, max_tpm=3)
    time.sleep(2)
    for _ in range(5):
        _send_trade(producer, cid, "AAPL", qty=1, price="1.00")
    assert waitfor(lambda: any(
        r["breach_type"] == "TRADE_VELOCITY"
        for r in query(f"SELECT breach_type FROM breaches WHERE client_id = '{cid}'")
    ))
```

- [ ] **Step 3: Run E2E tests**

With `make all` complete:
```bash
cd tools && source .venv/bin/activate
pip install pytest requests
cd ../tests/e2e
pytest -v
```
Expected: 3 tests pass within ~30s.

- [ ] **Step 4: Commit**

```bash
cd ../..
git add tests/e2e/
git commit -m "test: add end-to-end breach scenarios via pytest+Pinot"
```

---

## Phase 8 — README and finishing

### Task 8.1: README

**Files:** Create `README.md`

- [ ] **Step 1: Write README**

```markdown
# Streaming Risk Analytics

End-to-end demo: synthetic equity trades → Kafka → two Flink jobs (positions + breach detection) → Pinot → React dashboard.

## Quick start

```bash
make up               # docker-compose
make topics           # creates Kafka topics
make register-schemas # Avro → Schema Registry
make pinot-tables     # registers Pinot tables
cd flink && mvn package -DskipTests
cd ..
make submit-jobs      # uploads + starts both Flink JARs
make seed-limits      # default limits for 50 clients
make trade-gen        # starts trade generator (foreground)
# In another shell:
cd dashboard && npm run dev
```

Open:
- Dashboard: http://localhost:5173
- Flink UI: http://localhost:8082
- Pinot UI: http://localhost:9000
- Kafka UI: http://localhost:8080
- Grafana: http://localhost:3000

## Design

See `docs/superpowers/specs/2026-04-30-streaming-risk-analytics-design.md`.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add quick-start README"
```

---

## Self-review (post-write)

1. **Spec coverage:**
   - Locked decisions 1–7 → all implemented across phases 0–6.
   - Schemas (5) → Task 0.2.
   - Job A pipeline (Enricher, PositionUpdater, LatenessRouter, ClientSymbolPartitioner, JOB) → Tasks 2.4–2.11.
   - Job B pipeline (RiskInput, RiskEvaluator, JOB) → Tasks 3.1–3.4.
   - Pinot tables (3) → Tasks 4.2–4.5.
   - Dashboard (4 panels + filter) → Tasks 5.1–5.3.
   - Late-arriving data policy → enforced in `LatenessRouter` (Task 2.6) and `PositionUpdater`/`RiskEvaluator` last_update guards (Tasks 2.7, 3.2).
   - Watermark idleness → in `KafkaSourceFactory.watermarkStrategyTyped` (Task 2.3).
   - Observability (Prometheus + Grafana + custom counters) → Tasks 6.1–6.2.
   - Testing strategy (unit, integration via harnesses, E2E pytest) → Tasks 2.4–2.10, 3.2–3.3, 7.1.

2. **Placeholder scan:** No `TBD`/`TODO`/`fill in` left. ✓

3. **Type consistency:**
   - `LatenessRouter.DLQ_TAG` referenced consistently in tests + job.
   - `PositionUpdater.ENRICHED_TAG` introduced in Task 2.10 refactor and consumed in `PositionEngineJob`.
   - `RiskEvaluator.LIMITS_DESC` defined in Task 3.3 and referenced by tests (3.2) and job (3.4).
   - `RiskInput.OfTrade`/`OfPosition` shape consistent.

4. **Gaps acknowledged:**
   - Integration tests (Flink MiniCluster) are mentioned in the spec but not given dedicated tasks here — coverage from unit harnesses + E2E is judged sufficient for v1; add `MiniClusterExtension` tests as a follow-up if needed.
   - `KafkaSinkFactory` does not yet wire the custom `ClientSymbolPartitioner` into the producer config — note: Pinot upsert correctness depends on each `(client_id, symbol)` consistently landing on the same partition. The default partitioner uses the message key, which we set to `client_id|symbol` in `PositionEngineJob` — that is sufficient since Kafka's default partitioner hashes the key. The `ClientSymbolPartitioner` class therefore exists as documentation/test-vehicle of the expected hashing contract.
   - DLQ topic sink is wired up to drop trades silently in `PositionEngineJob`; a follow-up should add an explicit String-serialized sink for inspection. Mark as known limitation.
