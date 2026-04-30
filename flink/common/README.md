# flink/common/

Shared module: Avro codegen + cross-cutting Flink utilities. Both `position-engine` and `risk-engine` depend on this.

## Build

`mvn -pl common compile` runs the `avro-maven-plugin` against `../../schemas/*.avsc` and writes Java classes into `src/main/java/com/example/avro/` (gitignored — regenerated on every build).

## Hand-written code

Under `src/main/java/com/example/common/`:

| Class | Purpose |
|-------|---------|
| `SectorLookup` | Loads `sectors.json` from the classpath at static init. `sectorFor(symbol)` returns the equities sector or `"UNKNOWN"`. |
| `KafkaSourceFactory` | Builds typed `KafkaSource<T>` for Avro records via Schema Registry; provides a `watermarkStrategyTyped` helper with bounded-OOO + idleness defaults from the spec. |
| `KafkaSinkFactory` | Builds `KafkaSink<T>` with EXACTLY_ONCE delivery and Schema-Registry Avro serialization. Uses the inner `KeySerializationSchema` to wrap a serializable key extractor. |
| `SerializableKeyExtractor<T>` | `Function<T, byte[]> & Serializable`. Required because Flink's stream graph must serialize all closures. |
| `SerializableToLong<T>` | `ToLongFunction<T> & Serializable`. Same reason — used as the timestamp extractor in `WatermarkStrategy`. |

## Resources

`src/main/resources/sectors.json` — symbol → sector map (~30 representative tickers across 6 sectors). Mirrored from `tools/trade_gen/sectors.json` so Flink and the trade generator agree on the mapping.

## Tests

`SectorLookupTest` exercises the resource load + lookup behavior. Run with `mvn -pl common test`.
