# com.example.risk

Source of `risk-engine`. See `../../../../../../README.md` for the full module overview and pipeline diagram. Quick reference:

- **`RiskEngineJob`** — entrypoint; wires three sources (one broadcast) and a sink.
- **`RiskInput`** — `sealed interface` over `OfTrade(EnrichedTrade)` and `OfPosition(Position)`. Allows the union of two keyed streams to flow through a single keyed broadcast process function.
- **`RiskEvaluator`** — `KeyedBroadcastProcessFunction`. Maintains per-client keyed state (`MapState<symbol, SymbolNotional>` with last-update guard, plus `ListState<Long>` for the velocity window) and reads broadcast `MapState<client_id, ClientLimit>`. Emits a `Breach` per triggering trade for each of the three rules (`SINGLE_ORDER_NOTIONAL`, `GROSS_NOTIONAL`, `TRADE_VELOCITY`).
