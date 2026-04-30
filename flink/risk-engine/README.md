# flink/risk-engine/

**Job B.** Evaluates each enriched trade against the latest per-client limits and emits breach events.

## Pipeline

```
trades    = KafkaSource<EnrichedTrade>(enriched-trades) .map(toRiskInput.ofTrade)    .keyBy(client_id)
positions = KafkaSource<Position>(positions)            .map(toRiskInput.ofPosition) .keyBy(client_id)
limits    = KafkaSource<ClientLimit>(client-limits)                                    .broadcast(LIMITS_DESC)

trades.union(positions)                              // single keyed RiskInput stream
       .keyBy(RiskInput::clientId)
       .connect(limits)
       .process(RiskEvaluator)                       // KeyedBroadcastProcessFunction
       .sinkTo(KafkaSink<Breach>(breaches))
```

## Source files (`src/main/java/com/example/risk/`)

| Class | Role |
|-------|------|
| `RiskEngineJob` | `main` — wires sources, broadcast, and sink. |
| `RiskInput` | `sealed interface` — tagged union over `OfTrade(EnrichedTrade)` and `OfPosition(Position)`. Lets two keyed streams flow through one keyed process function. |
| `RiskEvaluator` | `KeyedBroadcastProcessFunction<String, RiskInput, ClientLimit, Breach>`. Holds keyed state (per-symbol notional with last_update guard, 60 s velocity window) + broadcast state (`MapState<client_id, ClientLimit>`). Emits a `Breach` per triggering trade for each rule that fires. |

## Three breach rules

1. **`SINGLE_ORDER_NOTIONAL`** — per-event check: `trade.notional > limit.max_single_order_notional`. Carries `trade_id` + `order_id`.
2. **`GROSS_NOTIONAL`** — sum of `|notional|` across all symbols held by the client (rebuilt each event from the `MapState<symbol, SymbolNotional>` populated from the `positions` stream, with monotonic `last_update` rejection of stale updates). Compared to `limit.max_gross_notional`.
3. **`TRADE_VELOCITY`** — sliding 60 s window of trade `event_ts` values in a `ListState<Long>`. Evicts older entries lazily; if `count > limit.max_trades_per_minute`, fires.

Breach repetition policy: **fire on every triggering event**. Dedup is the dashboard's (or a future filter job's) job.

## Late-position guard

When a `Position` event arrives, it's only applied if its `last_update` is strictly newer than the last applied for that symbol. Prevents an out-of-order position update from reverting state.

## Output: `Breach`

Avro `Breach` record with `breach_type` enum, `actual_value`, `limit_value`, `trade_id` / `order_id` (when relevant), and a human-readable `details` string already containing the formatted decimals (so the dashboard can display without decoding BYTES).

## Runtime

- Parallelism: 2
- Checkpoints: every 30 s, EXACTLY_ONCE, RocksDB backend
- Source consumer groups: `risk-engine-trades`, `risk-engine-positions`, `risk-engine-limits`

## Tests

`src/test/java/com/example/risk/RiskEvaluatorTest.java` — uses `KeyedTwoInputStreamOperatorTestHarness` (the broadcast-state variant) to drive position events + trades + limits and assert breach outputs. Five scenarios cover each rule, the no-limit-set case, and the late-position-rejected guard. Run via `mvn -pl risk-engine -am test`.
