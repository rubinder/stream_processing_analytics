# com.example.position

Source of `position-engine`. See `../../../../../../README.md` for the full module overview and pipeline diagram. Quick reference:

- **`PositionEngineJob`** — entrypoint; constructs sources, operators, sinks.
- **`Enricher`** — stateless `MapFunction<Trade, EnrichedTrade>`.
- **`LatenessRouter`** — `ProcessFunction<Trade, Trade>` routing > 60 s-late events to `DLQ_TAG` side output.
- **`PositionUpdater`** — `KeyedProcessFunction` with the per-`(client, symbol)` `ValueState`. Emits `Position` (main) and `EnrichedTrade` (`ENRICHED_TAG` side output).
- **`ClientSymbolPartitioner`** — `KafkaPartitioner<Position>` for the `positions` topic (composite-key hash).
