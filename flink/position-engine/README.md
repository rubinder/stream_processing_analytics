# flink/position-engine/

**Job A.** Single responsibility: maintain ground-truth positions per `(client_id, symbol)`.

## Pipeline

```
KafkaSource<Trade>(trades)
  → assignTimestampsAndWatermarks(boundedOOO 5s, idleness 30s)
  → process(LatenessRouter)            // > 60s late → DLQ side output
  → map(Enricher)                      // add notional, sector, signed_qty
  → keyBy(client_id|symbol)
  → process(PositionUpdater)           // KeyedProcessFunction with ValueState
      ├─ side output: EnrichedTrade  → KafkaSink(enriched-trades)
      └─ main output: Position       → KafkaSink(positions)
```

## Source files (`src/main/java/com/example/position/`)

| Class | Role |
|-------|------|
| `PositionEngineJob` | `main` — wires sources, operators, sinks. |
| `Enricher` | `MapFunction<Trade, EnrichedTrade>`. Adds `signed_qty`, `notional = quantity × price`, `sector` from `SectorLookup`. |
| `LatenessRouter` | `ProcessFunction<Trade, Trade>`. Routes events arriving > 60 s after the watermark to a DLQ side output (`DLQ_TAG`). |
| `PositionUpdater` | `KeyedProcessFunction<String, EnrichedTrade, Position>`. Holds a `ValueState<State>` per key; emits a `Position` record on every input + the original `EnrichedTrade` to a side output (`ENRICHED_TAG`). |
| `ClientSymbolPartitioner` | Custom Kafka partitioner; ensures Pinot upsert ordering by hashing the composite `(client_id, symbol)` key. Currently retained as documentation/test vehicle — the default key-hash partitioner gives the same behavior since the message key is already `client_id|symbol`. |

## State (`PositionUpdater.State`)

- `netQuantity` — running signed sum (BUY +qty, SELL −qty). Always applied (commutative).
- `weightedCostBasis` — VWAP on adds (BUYs only); reductions don't change basis. Demo-grade; not a proper short-sale accounting.
- `lastPrice` / `lastEventTsMs` — only updated when `eventTs > lastEventTsMs` so a late trade can't regress the displayed price.
- `lastUpdateMs` — `max(prev, eventTs)` — monotonic; used as Pinot's upsert comparison column.

## Output: `Position`

Each input emits exactly one `Position` record (latest-wins via Pinot upsert). Includes both the precise `notional` (BYTES decimal) and a `notional_double` (DOUBLE) companion that Pinot can `SUM`/`ABS`/`ORDER BY`.

## Runtime

- Parallelism: 2
- Checkpoints: every 30 s, EXACTLY_ONCE, RocksDB backend
- Source consumer group: `position-engine`

## Tests

`src/test/java/com/example/position/` — uses `KeyedOneInputStreamOperatorTestHarness` for `PositionUpdater` and `OneInputStreamOperatorTestHarness` for the simpler operators. Run via `mvn -pl position-engine -am test`.
