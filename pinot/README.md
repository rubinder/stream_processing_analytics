# pinot/

JSON configs for the three Apache Pinot REALTIME tables that serve the dashboard.

## Files

| Schema | Table config | Source topic | Mode |
|--------|--------------|--------------|------|
| `trades_schema.json` | `trades_table.json` | `enriched-trades` | append, 7-day retention |
| `positions_schema.json` | `positions_table.json` | `positions` | **upsert** (primary key `(client_id, symbol)`, comparison column `last_update`), 30-day retention |
| `breaches_schema.json` | `breaches_table.json` | `breaches` | append, 7-day retention |

## Registration

```bash
make pinot-tables
```

POSTs each `*_schema.json` to `http://localhost:9000/schemas` and each `*_table.json` to `http://localhost:9000/tables`. The order matters within a pair (schema first, then table).

## Notable design choices

- **Trades source = `enriched-trades`** (not raw `trades`) so the table has `notional` and `sector` already computed.
- **Upsert routing**: `positions_table.json` uses `routing.instanceSelectorType=strictReplicaGroup`, which Pinot requires for upsert/dedup tables. With a single-server local cluster, Pinot picks defaults and assigns all 4 partitions to the one server.
- **Decoder**: all three tables use `KafkaConfluentSchemaRegistryAvroMessageDecoder`, fetching schemas at runtime from `http://schema-registry:8081`.
- **`BYTES` columns for decimals**: Pinot's Avro decoder reads `bytes(decimal)` as raw bytes (no scale applied). This means `SUM`/`ABS`/`ORDER BY` on those columns either errors or yields nonsense. The `positions` table includes companion `notional_double` / `avg_price_double` (DOUBLE) columns — populated in Flink via `BigDecimal.doubleValue()` — that Pinot can aggregate natively. The dashboard's Top-10 Exposed and Positions panels query the `_double` columns.

## Indexes

- `trades`: inverted on `client_id`, `symbol`, `sector`; range on `event_ts`.
- `positions`: inverted on `client_id`, `symbol`, `sector`.
- `breaches`: inverted on `client_id`, `breach_type`.

## Pinot UI

`http://localhost:9000` for table state, segment status, and the SQL query console.
