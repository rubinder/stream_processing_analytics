# com.example.common

Shared utilities for both Flink jobs. See `../../../../../../README.md` for the module overview.

| Class | Purpose |
|-------|---------|
| `SectorLookup` | Static map symbol → sector. Loaded once from `/sectors.json` on class init; `sectorFor` returns `"UNKNOWN"` for misses. |
| `KafkaSourceFactory` | `avroSource(...)` builds a typed `KafkaSource<T extends SpecificRecord>` configured with Confluent Schema-Registry Avro deserialization, earliest-offsets bootstrap, and the consumer group passed by the caller. `watermarkStrategyTyped` returns the standard `forBoundedOutOfOrderness(5s).withIdleness(30s)` strategy with a caller-supplied serializable timestamp extractor. |
| `KafkaSinkFactory` | `avroSink(...)` builds a `KafkaSink<T>` with EXACTLY_ONCE delivery, a 5-min transaction timeout, and Schema-Registry Avro serialization. Wraps the caller's `SerializableKeyExtractor<T>` in an inner `KeySerializationSchema`. |
| `SerializableKeyExtractor<T>` | `Function<T, byte[]> & Serializable`. Required because Flink's stream graph must serialize every closure. |
| `SerializableToLong<T>` | `ToLongFunction<T> & Serializable`. Used as the timestamp extractor in `WatermarkStrategy`. |
