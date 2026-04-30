# schemas/

Avro schema definitions — **single source of truth** for the wire format used everywhere in the pipeline.

## Files

| File | Subject in Schema Registry | Topic |
|------|---------------------------|-------|
| `trade.avsc` | `trades-value` | `trades` (input from generator) |
| `enriched_trade.avsc` | `enriched-trades-value` | `enriched-trades` (Job A output) |
| `client_limit.avsc` | `client-limits-value` | `client-limits` (compacted, broadcast input) |
| `position.avsc` | `positions-value` | `positions` (Job A output, Pinot upsert) |
| `breach.avsc` | `breaches-value` | `breaches` (Job B output) |

## Why a single source of truth?

- **Java**: `flink/common/pom.xml` runs `avro-maven-plugin` against this directory, generating `Trade.java`, `EnrichedTrade.java`, etc. into `flink/common/src/main/java/com/example/avro/` (gitignored, regenerated on every build).
- **Python**: `tools/` and `tests/e2e/` read these `.avsc` files directly via `fastavro`.
- **Pinot**: schemas are reflected (separately) under `pinot/*_schema.json`.
- **Schema Registry**: `make register-schemas` POSTs each file as a versioned subject so producers/consumers can fetch by ID.

If you change a field here, also update `pinot/<table>_schema.json` if Pinot needs to read it, then `make register-schemas` to push the new version.

## Compatibility

Schema Registry is configured for `BACKWARD` compatibility per subject (the default). New fields require a `default` value. Removing fields requires defaults on the consumer side that's still reading old records.

## Notable design choices

- **Decimals as `bytes(decimal, precision, scale)`** for financial precision (no float drift).
- **Timestamps as `long(timestamp-millis)`** — Avro logical type. With `enableDecimalLogicalType=true` in the codegen config, Java getters return `BigDecimal` / `Instant` directly.
- `position.avsc` carries both `notional` (precise `bytes(decimal)`) and `notional_double` (DOUBLE companion). Pinot's Avro decoder doesn't apply decimal logical type to BYTES columns, so the DOUBLE companion is what dashboards aggregate / sort on. Same for `avg_price` / `avg_price_double`.
- `enriched_trade.side` is a plain string (not the enum `Side` from `trade.avsc`) because Schema Registry parses each subject independently and can't resolve cross-file enum references.
