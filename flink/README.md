# flink/

Maven multi-module project containing the two Flink jobs that drive the pipeline.

## Modules

| Module | Purpose |
|--------|---------|
| `common/` | Avro codegen + shared utilities (`SectorLookup`, `KafkaSourceFactory`, `KafkaSinkFactory`, `Serializable*` interfaces). Built first; depended on by both engines. |
| `position-engine/` | Job A — reads `trades`, computes running positions per `(client_id, symbol)`, emits `enriched-trades` and `positions`. |
| `risk-engine/` | Job B — reads `enriched-trades` + `positions` + broadcast `client-limits`, evaluates three breach rules, emits `breaches`. |

## Build

```bash
mvn -pl position-engine -pl risk-engine -am package
```

Produces shaded uber-JARs at:
- `position-engine/target/position-engine-0.1.0.jar`
- `risk-engine/target/risk-engine-0.1.0.jar`

These are bind-mounted into the `flink-jobmanager` container at `/jars/` and submitted via `make submit-jobs`.

## Versions (pinned in `pom.xml`)

- Java 21 source/target
- Apache Flink **2.2.0** (DataStream API)
- `flink-connector-kafka` **4.0.1-2.0** (separate versioning from Flink core)
- Avro **1.11.4** (with `enableDecimalLogicalType=true` so getters return `BigDecimal`/`Instant`)

## State backend

RocksDB. Checkpoints every 30s in `EXACTLY_ONCE` mode, written to in-container `/tmp/flink-checkpoints` (ephemeral on local restart — see `docker-compose.yml` notes).

## Tests

```bash
mvn test                       # all modules
mvn -pl position-engine test   # one module
```

Tests use Flink's `*OperatorTestHarness` from `flink-test-utils` to exercise process functions in-process (no MiniCluster needed).

## Conventions

- Lambdas captured by Flink's stream graph **must be `Serializable`**. The `common/` module provides `SerializableKeyExtractor` and `SerializableToLong` for that purpose.
- Decimals: `BigDecimal` for math, `bytes(decimal)` on the wire.
- Timestamps: `Instant` for the API, `.toEpochMilli()` for window/timer math (which expects long ms).
