# Streaming Risk Analytics — Design Spec

**Date:** 2026-04-30
**Status:** Draft — ready for user review

## Locked Decisions

| # | Decision | Choice |
|---|---|---|
| 1 | Deployment target | **Hybrid** — `docker-compose` locally, production-shaped code (Avro, schema registry, checkpoints, exactly-once, observability) so it can be lifted into K8s later |
| 2 | Risk analytics scope | **Positions/exposure + limit breaches** (max gross notional, max single-order notional, max trade velocity) |
| 3 | Trade data shape | **Equities-style**, Avro-serialized, registered in Confluent Schema Registry |
| 4 | Limits configuration | **Kafka `client-limits` topic + Flink broadcast state** — limits update live, no restart |
| 5 | Pinot serving | **Three REALTIME tables** including a `positions` upsert table; **custom React dashboard** + Pinot query console |
| 6 | Flink job language | **Java + Maven, DataStream API** |
| 7 | Topology | **Two Flink jobs** — `position-engine` and `risk-engine`, integrated via Kafka |

## Architecture

```
┌──────────────┐                                                   ┌─────────────┐
│ trade-gen    │──► trades ──┐                                     │  dashboard  │
│ (Python)     │             │                                     │  (React)    │
└──────────────┘             ▼                                     └──────▲──────┘
                       ┌──────────┐                                       │ SQL
                       │ Job A    │── enriched-trades ──┬──┐              │
                       │ position │── positions ──────┬─┼──┼──► Pinot ────┘
                       │ engine   │                   │ │  │   (REALTIME
                       └──────────┘                   │ │  │    + upsert)
                                                      │ │  │
┌──────────────┐                                      │ │  │
│ limits-cli   │── client-limits ──┐                  │ │  │
│ (Python)     │                   ▼                  ▼ ▼  │
└──────────────┘             ┌────────────┐                │
                             │ Job B      │── breaches ────┘
                             │ risk       │
                             │ engine     │
                             └────────────┘
                  reads: enriched-trades, positions, client-limits
```

### Components

- **Kafka (KRaft, single broker locally)** — backbone. Topics: `trades`, `client-limits`, `enriched-trades`, `positions`, `breaches`, `dlq`. Confluent Schema Registry alongside.
- **Trade generator** (Python, `confluent-kafka` + `fastavro`) — synthetic equity trades for ~50 clients × ~500 symbols, configurable rate.
- **Limits CLI** (Python) — publishes/updates limits per client to `client-limits`.
- **Flink Job A — `position-engine`** (Java) — enriches trades, computes running net position per `(client_id, symbol)` in keyed state, emits enriched trades and position upserts.
- **Flink Job B — `risk-engine`** (Java) — consumes `enriched-trades` and `positions` (re-keyed by `client_id`), joined with broadcast `client-limits`; detects three breach types and emits breach events.
- **Pinot** — three REALTIME tables: `trades` (append), `positions` (upsert keyed on `(client_id, symbol)`), `breaches` (append).
- **Dashboard** (React + Vite) — four panels (breaches/min sparkline, top-N exposures, positions table, live breach feed). Polls Pinot SQL broker on staggered cadences (2–10s).

### Data flow (happy path)
1. Generator publishes a trade to `trades`.
2. Job A reads, enriches (adds `notional`, `sector`), updates per-key position state, emits to `enriched-trades` and `positions`.
3. Job B reads `enriched-trades` and `positions`, joined with broadcast `client-limits`, evaluates rules, emits any `breaches`.
4. Pinot ingests all three output topics.
5. Dashboard SQL-queries Pinot.

**Target end-to-end latency:** trade → dashboard visible in **< 3s** locally.

## Schemas (Avro)

All Kafka payloads are Avro, registered in Confluent Schema Registry. `BACKWARD` compatibility required — new fields must have defaults.

### `trade.avsc` — topic `trades`, key `client_id`
```
trade_id:  string                    # UUID, unique per fill
order_id:  string                    # parent order ID (one order, many fills)
client_id: string                    # e.g. "C0042"
symbol:    string                    # e.g. "AAPL"
side:      enum {BUY, SELL}
quantity:  long
price:     bytes (decimal, 18,4)
currency:  string                    # ISO 4217, default "USD"
venue:     string
event_ts:  long (timestamp-millis)
```

### `enriched_trade.avsc` — topic `enriched-trades`, key `client_id`
Trade + `notional: bytes(decimal)`, `sector: string`, `signed_qty: long` (negative for SELL). `order_id` carried through.

### `client_limit.avsc` — topic `client-limits`, key `client_id`, **compacted**
```
client_id:                 string
max_gross_notional:        bytes (decimal, 18,2)
max_single_order_notional: bytes (decimal, 18,2)
max_trades_per_minute:     int
updated_ts:                long (timestamp-millis)
```
Compaction → restart replays current limit set.

### `position.avsc` — topic `positions`, key `client_id|symbol`
```
client_id:    string
symbol:       string
net_quantity: long
avg_price:    bytes (decimal, 18,4)
notional:     bytes (decimal, 18,2)
sector:       string
last_update:  long (timestamp-millis)
```
Pinot upsert-keyed on `(client_id, symbol)`.

### `breach.avsc` — topic `breaches`, key `client_id`
```
breach_id:   string
client_id:   string
breach_type: enum {GROSS_NOTIONAL, SINGLE_ORDER_NOTIONAL, TRADE_VELOCITY}
limit_value: bytes (decimal)
actual_value:bytes (decimal)
trade_id:    string (nullable)        # the triggering fill
order_id:    string (nullable)        # the parent order (set for SINGLE_ORDER_NOTIONAL)
detected_ts: long (timestamp-millis)
details:     string
```

Decimals stay as Avro `bytes(decimal)` — financial precision matters.

## Job A — position-engine

**Purpose:** ground-truth running positions per `(client_id, symbol)`, emit enriched trades for downstream risk evaluation.

### Pipeline (Flink DataStream API)
```
KafkaSource<Trade>(trades, group=position-engine)
  → assignTimestampsAndWatermarks(forBoundedOutOfOrderness(5s).withIdleness(30s))
  → process(LatenessRouter)    // > 60s late → DLQ; otherwise → main
  → map(Enricher)              // add notional, sector, signed_qty
  → keyBy(client_id, symbol)
  → process(PositionUpdater)   // KeyedProcessFunction
  → sideOutput(enrichedTrades) → KafkaSink(enriched-trades, exactly-once)
  → mainOutput(positions)      → KafkaSink(positions, exactly-once)
```

### `Enricher` (stateless map)
- `notional = quantity * price`
- `signed_qty = side == BUY ? +qty : -qty`
- `sector` from a static `Map<String,String>` (resource `sectors.json`, ~500 entries) — unknown symbol → `sector="UNKNOWN"` and emit a DLQ record.

### `PositionUpdater` (KeyedProcessFunction)
- Keyed state: `ValueState<PositionState> { net_quantity, weighted_cost_basis, last_price, last_event_ts, last_update }`.
- On each enriched trade:
  - `net_quantity += signed_qty` — **always** applied (sum is commutative; late trades still update total).
  - Weighted-average cost basis updated **on adds only**; reductions don't change basis. **Demo-grade simplification accepted:** behavior is approximate when a position flips long↔short or when adds arrive late. Documented as a known limitation.
  - `last_price` and `last_event_ts` — **only updated if `trade.event_ts > state.last_event_ts`**. Prevents a late trade from regressing the displayed price.
  - `last_price` = price from the most recent fill **for this client+symbol** (no market-data feed; documented limitation).
  - `notional = net_quantity * last_price`
  - `last_update = max(state.last_update, trade.event_ts)` — monotonic, used as the Pinot upsert comparison column.
  - Emit `Position` to main output (Pinot upsert keeps latest).
  - Emit enriched trade to side output.
- No state TTL — positions are durable.

### Runtime
- Checkpointing: every 30s, `EXACTLY_ONCE`, RocksDB state backend, checkpoints to `./checkpoints/position-engine/` (volume-mounted locally; S3/GCS in prod).
- Parallelism: 2 locally.
- Kafka source partitioned by `client_id`; finer key `(client_id, symbol)` for state.

## Job B — risk-engine

**Purpose:** evaluate every enriched trade against the latest per-client limits, emit breach events. Stateful: current limits (broadcast), per-symbol notional per client (from `positions`), 60s velocity window.

### Inputs
- `enriched-trades` — keyed by `client_id` (event-time, 5s OOO tolerance)
- `positions` — keyed by `client_id` (the per-`(client,symbol)` upserts from Job A; Job B re-keys to client level)
- `client-limits` — broadcast (compacted topic, replays current state on bootstrap)

### Pipeline (Flink DataStream API)
```
WS = forBoundedOutOfOrderness(5s).withIdleness(30s)

trades   = KafkaSource<EnrichedTrade>(enriched-trades).assignTimestampsAndWatermarks(WS)
                                                       .process(LatenessRouter)   // >60s late → DLQ
                                                       .map(toRiskInput.ofTrade) .keyBy(client_id)
positions= KafkaSource<Position>(positions)            .assignTimestampsAndWatermarks(WS)
                                                       .map(toRiskInput.ofPosition).keyBy(client_id)

keyedUnion = trades.union(positions)                   // single keyed stream of RiskInput

limits   = KafkaSource<ClientLimit>(client-limits)     .broadcast(LIMITS_DESC)

keyedUnion.connect(limits)
  .process(RiskEvaluator)                              // KeyedBroadcastProcessFunction
  .sinkTo(KafkaSink<Breach>(breaches, EXACTLY_ONCE))
```

### `RiskEvaluator` (KeyedBroadcastProcessFunction)

**Broadcast state:** `MapState<String client_id, ClientLimit>`. Updated in `processBroadcastElement`.

**Keyed state per `client_id`:**
- `MapState<String symbol, SymbolNotional { notional, last_update }>` — last-known absolute notional per symbol with its source `last_update`. Used for monotonic merge: a Position event is **applied only if `position.last_update > existing.last_update`**, so a late position never overwrites a fresher one.
- `ListState<Long> recentTradeTimestamps` — sliding 60s window of trade `event_ts` values.

**`processElement(RiskInput input, ...)`**

If `input` is a `Position`:
- If `position.last_update <= existing.last_update` → drop (late update; staler than what we have).
- Else `symbolNotionals.put(symbol, { |position.notional|, position.last_update })` (or remove if `net_quantity == 0`).
- No emission. State updated for next trade evaluation.

If `input` is an `EnrichedTrade`:
- `limit = broadcastState.get(client_id)` — null → no limits configured, skip evaluation.
- Compute `grossNotional = Σ symbolNotionals.values()` (small map, ~tens of symbols).
- **Rule 1 — `SINGLE_ORDER_NOTIONAL`**: `if (trade.notional > limit.max_single_order_notional) emit Breach(...)`. Carries `trade_id` and `order_id`.
- **Rule 2 — `GROSS_NOTIONAL`**: `if (grossNotional > limit.max_gross_notional) emit Breach(...)`. Position-derived, authoritative.
- **Rule 3 — `TRADE_VELOCITY`**:
  - `recentTradeTimestamps.add(trade.event_ts)`
  - Evict entries older than `event_ts - 60_000ms` (lazy, on each event).
  - `if (count > limit.max_trades_per_minute) emit Breach(...)` with `details="N trades in 60s"`.

**Breach repetition policy:** **fire on every triggering event.** No suppression — keep the engine simple and honest; downstream (dashboard or a future filter job) handles dedup.

**Cleanup timer:** event-time timer at `event_ts + 60_000ms` evicts stale velocity entries even when a client goes quiet, preventing unbounded list growth.

**Race-condition note:** because `positions` and `enriched-trades` arrive on separate Kafka partitions (both keyed by `client_id`, so same task — but separate sources), a trade may be evaluated *just before* its corresponding position update lands. The grossNotional reflects state-as-of-last-position, not state-after-this-trade. For a demo this lag is sub-second and acceptable. A stricter version would have Job A emit `(EnrichedTrade, Position)` paired in a single message.

### Runtime
- Parallelism 2, `EXACTLY_ONCE` checkpoints every 30s, RocksDB backend, checkpoint dir `./checkpoints/risk-engine/`.

## Pinot tables

Three REALTIME tables, schemas + table configs as JSON under `pinot/`.

### `trades` (REALTIME, append)
- Source topic: `enriched-trades` (not raw — pre-enriched gives `notional`, `sector` for free).
- `timeColumn`: `event_ts`.
- Segment flush: 1h. Retention: 7 days.
- Indexes: inverted on `client_id`, `symbol`, `sector`; range on `event_ts`.

### `positions` (REALTIME, **upsert**)
- Source topic: `positions`.
- **Primary key:** `(client_id, symbol)`.
- `timeColumn` and upsert comparison column: `last_update` (newer wins).
- Indexes: inverted on `client_id`, `symbol`, `sector`.
- **Required topic config:** `positions` topic partitioned by hash of `client_id|symbol` (custom Kafka partitioner in Job A). 4 partitions locally. Pinot table uses `RoutingConfig: strictReplicaGroup`.

### `breaches` (REALTIME, append)
- Source topic: `breaches`.
- `timeColumn`: `detected_ts`.
- Indexes: inverted on `client_id`, `breach_type`.

### Pinot deployment (local)
Single ZK, Controller, Broker, Server, Minion — all docker containers. No HA. Kafka runs in KRaft mode (no Kafka ZK; Pinot's ZK is separate).

### Canonical dashboard queries
```sql
-- Current positions for a client
SELECT symbol, net_quantity, notional, sector
FROM positions WHERE client_id = ? ORDER BY ABS(notional) DESC;

-- Top-10 exposed clients
SELECT client_id, SUM(ABS(notional)) AS gross
FROM positions GROUP BY client_id ORDER BY gross DESC LIMIT 10;

-- Recent breaches (last 5 min)
SELECT client_id, breach_type, actual_value, limit_value, details, detected_ts
FROM breaches WHERE detected_ts > ago('PT5M') ORDER BY detected_ts DESC LIMIT 50;

-- Trade velocity (last 1 min)
SELECT client_id, COUNT(*) AS trades_per_min
FROM trades WHERE event_ts > ago('PT1M') GROUP BY client_id ORDER BY trades_per_min DESC LIMIT 10;
```

## Dashboard

**Stack:** React + Vite + TypeScript, Tailwind, TanStack Query for polling, Recharts for charts. **No backend** — browser queries Pinot SQL endpoint directly (`http://localhost:8099/query/sql`). Acceptable for local demo; prod would front with an auth proxy.

### Layout — single page, four panels
```
┌────────────────────────────────────────────────────────────────────┐
│  Streaming Risk Dashboard         [client_id ▼ All]  [● Live]      │
├────────────────────────────────────────────────────────────────────┤
│  PANEL D — Breaches per minute, last 1h (sparkline, full-width)    │
├──────────────────────────────────┬─────────────────────────────────┤
│  PANEL A — Top-10 Exposed Clients│  PANEL C — Live Breach Feed    │
│  (horizontal bar, gross notional)│  (rolling list, last 50,        │
│                                  │   colour by breach_type,        │
├──────────────────────────────────┤   newest first, 5min window)    │
│  PANEL B — Positions             │                                 │
│  (table — symbol, net_qty,       │                                 │
│   notional, sector; client       │                                 │
│   filter from header)            │                                 │
└──────────────────────────────────┴─────────────────────────────────┘
```

### Panels & queries

| Panel | Poll | Query |
|-------|------|-------|
| **D — Breaches/min, last 1h** | 10s | `SELECT DATETRUNC('MINUTE', detected_ts) AS minute, COUNT(*) FROM breaches WHERE detected_ts > ago('PT1H') GROUP BY minute ORDER BY minute` (sparkline, Recharts area chart) |
| **A — Top-10 exposed clients** | 5s | `SELECT client_id, SUM(ABS(notional)) AS gross FROM positions GROUP BY client_id ORDER BY gross DESC LIMIT 10` |
| **B — Positions** | 2s | `SELECT symbol, net_quantity, notional, sector FROM positions WHERE client_id = ? ORDER BY ABS(notional) DESC` (filter `?` = "All" → omit WHERE) |
| **C — Breach feed** | 2s | `SELECT client_id, breach_type, actual_value, limit_value, details, detected_ts FROM breaches WHERE detected_ts > ago('PT5M') ORDER BY detected_ts DESC LIMIT 50` |

### Client-filter dropdown
Populated from `SELECT DISTINCT client_id FROM positions LIMIT 1000` on load; refreshed every 30s.

### Visual treatment
Dark mode, monospace numerics. Breach colours: `GROSS_NOTIONAL` red, `SINGLE_ORDER_NOTIONAL` amber, `TRADE_VELOCITY` blue. "Trading-screen serious."

### Future push channel (out of scope for v1)
If polling becomes insufficient, add a small SSE gateway that consumes `breaches` directly from Kafka and pushes to the browser — bypassing Pinot for the alert path.

## Local dev environment

### Versions (locked)
| Component | Version | Notes |
|-----------|---------|-------|
| Java | **21** | Flink runtime + build target |
| Apache Kafka | **4.2.0** | KRaft mode (no ZK) |
| Apache Flink | **2.2.0** | DataStream API |
| Apache Pinot | **1.5.0** | REALTIME tables + upsert |
| Confluent Schema Registry | **CP 8.x** | Aligned with Kafka 4.x |
| React / Vite / TS | latest | Dashboard |

### `docker-compose.yml` services
| Service | Image | Ports | Purpose |
|---|---|---|---|
| `kafka` | `apache/kafka:4.2.0` (KRaft) | 9092, 9094 | Single broker |
| `schema-registry` | `confluentinc/cp-schema-registry:8.x` | 8081 | Avro schemas |
| `kafka-ui` | `provectuslabs/kafka-ui:latest` | 8080 | Topic inspector |
| `flink-jobmanager` | `flink:2.2.0-java21` | 8082 | JM + web UI |
| `flink-taskmanager` | `flink:2.2.0-java21` | — | 2 task slots |
| `pinot-zk` | `zookeeper:3.9` | 2181 | Pinot's ZK |
| `pinot-controller` | `apachepinot/pinot:1.5.0` | 9000 | Pinot UI |
| `pinot-broker` | `apachepinot/pinot:1.5.0` | 8099 | SQL endpoint |
| `pinot-server` | `apachepinot/pinot:1.5.0` | — | Segment storage |
| `dashboard` | local Vite dev (or built nginx image) | 5173 | UI |

### Makefile targets
- `make up` — start docker-compose
- `make down` — tear down (preserves volumes)
- `make clean` — tear down and wipe volumes
- `make topics` — create Kafka topics with correct partitioning/compaction
- `make register-schemas` — POST `.avsc` files to Schema Registry
- `make submit-jobs` — upload + start both Flink JARs via JM REST
- `make pinot-tables` — POST schemas + table configs to Pinot controller
- `make seed-limits` — publish a default per-client limit set
- `make trade-gen` — start the Python trade generator
- `make all` — runs the above in order, idempotent

## Project layout
```
stream_processing_analytics/
├── docker-compose.yml
├── Makefile
├── README.md
├── docs/superpowers/specs/2026-04-30-streaming-risk-analytics-design.md
├── schemas/                              # Avro — single source of truth
│   ├── trade.avsc
│   ├── enriched_trade.avsc
│   ├── client_limit.avsc
│   ├── position.avsc
│   └── breach.avsc
├── flink/
│   ├── pom.xml                           # parent POM, three modules
│   ├── common/                           # generated Avro + shared utils
│   ├── position-engine/
│   │   └── src/main/java/com/example/position/
│   │       ├── PositionEngineJob.java
│   │       ├── Enricher.java
│   │       ├── PositionUpdater.java
│   │       └── ClientSymbolPartitioner.java
│   └── risk-engine/
│       └── src/main/java/com/example/risk/
│           ├── RiskEngineJob.java
│           ├── RiskEvaluator.java
│           └── RiskInput.java            # tagged-union (Trade | Position)
├── pinot/
│   ├── trades_schema.json + trades_table.json
│   ├── positions_schema.json + positions_table.json
│   └── breaches_schema.json + breaches_table.json
├── tools/                                # Python helpers
│   ├── pyproject.toml
│   ├── trade_gen/
│   │   ├── __main__.py
│   │   ├── generator.py
│   │   └── sectors.json                  # also baked into Flink resources
│   └── limits_cli/
│       └── __main__.py
├── dashboard/
│   ├── package.json
│   ├── src/{App.tsx, pinot.ts, panels/{TopExposed,Positions,BreachFeed,BreachesSparkline}.tsx}
│   └── ...
└── tests/
    ├── flink/                            # JUnit + Flink MiniCluster
    └── e2e/                              # pytest hitting docker-compose
```

**Build:** `mvn -pl flink/position-engine,flink/risk-engine -am package` produces two shaded JARs. Flink JM REST API uploads them.
**Schema source-of-truth:** `schemas/*.avsc` — `avro-maven-plugin` generates Java; `tools/` reads same files via `fastavro`. No drift.

## Late-arriving data — policy

**Watermark strategy (every Kafka source):** `forBoundedOutOfOrderness(5s).withIdleness(30s)`.
- 5s out-of-order tolerance — typical inter-arrival skew on a single broker.
- 30s idleness — a partition with no traffic is excluded from watermark calculation, so a quiet client doesn't stall global event-time logic.

**Bounded lateness window: 60s.**
- Events arriving ≤ 60s late are processed normally (state updates, breach evaluation, emission).
- Events arriving > 60s late are routed to `dlq` by `LatenessRouter` rather than processed. Prevents an unbounded "very-late" tail from rewriting positions hours after the fact.
- 60s matches the velocity-window length, so no in-flight breach evaluation depends on data older than the lateness window.

**Per-component handling:**

| Component | Late behavior |
|---|---|
| Job A `PositionUpdater` — `net_quantity` | Always applied (commutative). |
| Job A — `last_price`, `last_event_ts` | Updated only if `event_ts > last_event_ts`. Late trade does not regress price. |
| Job A — cost basis | Demo-grade: applied in arrival order. Documented limitation. |
| Job A — `last_update` (emitted) | `max(prev, event_ts)`. Monotonic — Pinot upsert relies on this. |
| Job B — `MapState<symbol, notional>` | Per-symbol `last_update` guard; staler position events are dropped. |
| Job B — velocity window | Late trade evaluated against its own backward-looking 60s window. Breach `detected_ts` (wall clock) may follow `event_ts` (trade time) by seconds; this is explicit. |
| Pinot `positions` (upsert) | Comparison column `last_update` rejects stale upserts. |
| Pinot `trades`, `breaches` (append) | Late events insert into time-correct buckets. `WHERE event_ts > ago(...)` filters work correctly. |

**Trade dedup:** out of scope for v1 — assumes Kafka exactly-once producer + Flink exactly-once checkpoints prevent duplicate `trade_id`s end-to-end. A future hardening would key by `trade_id` in `PositionUpdater` and reject duplicates explicitly.

## Testing strategy

### Unit (JUnit 5, Flink test harnesses)
- `Enricher` — pure function tests on representative trades.
- `PositionUpdater` — `KeyedOneInputStreamOperatorTestHarness`. Covers buy-then-sell-then-flip-sign, late trade does not regress `last_price`, unknown-symbol DLQ.
- `RiskEvaluator` — `KeyedTwoInputStreamOperatorTestHarness` (broadcast variant). Covers each breach type, "no limit set → no eval", late-position-rejected-by-last_update guard.

### Integration (Flink MiniCluster)
- One test per job: `MiniClusterWithClientResource`, in-memory source/sink. Catches checkpoint-barrier and watermark generation issues unit harnesses miss.

### E2E (pytest against docker-compose)
- `make all` brings stack up; tests publish trades + limits via `confluent-kafka`, poll Pinot SQL endpoint, assert within a 10s deadline.
- Three scenarios:
  1. Single client crosses gross notional → exactly one `GROSS_NOTIONAL` breach per crossing trade.
  2. Oversized order → exactly one `SINGLE_ORDER_NOTIONAL` breach.
  3. Burst of N+1 trades in 60s → at least one `TRADE_VELOCITY` breach.

Test data uses a fixed seed for reproducibility — no Faker randomness.

## Observability & error handling

### Metrics
- Built-in Flink metrics via Prometheus reporter (`flink-metrics-prometheus`). JM `:9249`, TM `:9250`.
- Custom counters: `breaches_emitted_total{type}`, `dlq_records_total{reason}`, `unknown_symbols_total`, `late_records_dropped_total`.

### Stack (minimal)
- `prometheus` container scrapes Flink + Kafka JMX-exporter sidecars.
- `grafana` with one pre-provisioned dashboard:
  - Flink: records-in/out per operator, checkpoint duration, backpressure.
  - Kafka: per-topic message rate, consumer lag for `position-engine` and `risk-engine` groups.
  - App: breach rate by type, DLQ rate, late-records-dropped.

### Logs
Stdout per container; `docker compose logs -f` is the local viewer. No log aggregation locally.

### Health checks
docker-compose `healthcheck` for Kafka (port probe), Schema Registry (`/subjects`), Flink JM (`/v1/overview`), Pinot Controller (`/health`).

### Error handling
- Avro decode failures → `dlq` topic with `{topic, partition, offset, raw_bytes_b64, exception}`. Job continues.
- Unknown symbol → record processed with `sector="UNKNOWN"` AND a `dlq` warning; trades not dropped.
- Schema-incompatible producer → Schema Registry rejects on publish.
- Pinot ingestion lag — surfaces in Grafana; dashboard shows stale-data banner using `MAX(last_update) vs now()`.
- Records > 60s late → `dlq` via `LatenessRouter`.

## Out of scope (v1)
- Authn/authz on Kafka, Schema Registry, Pinot, dashboard.
- Multi-region / DR.
- Backfill from historical trade files.
- Market-data feed (last-fill price stands in for current price).
- Alert routing (Slack/PagerDuty).
- Trade-id deduplication (relies on exactly-once end-to-end).
- Proper short-sale cost-basis accounting.
