# trade_gen

Synthetic equity-trade generator. Writes Avro-encoded `Trade` records to the `trades` Kafka topic via the Confluent wire format.

## Files

- `__main__.py` — Click CLI; loads the Avro schema from `../../schemas/trade.avsc`, fetches the Schema-Registry ID for `trades-value`, then emits trades at the requested rate.
- `generator.py` — `TradeFactory` dataclass. Deterministic for a given `seed` (except `event_ts`). Picks client / symbol / side / quantity from a seeded `random.Random`, walks price within ±2 % of a fixed per-symbol band, occasionally reuses an open `order_id` to simulate multi-fill orders.
- `sectors.json` — symbol → sector map (mirrored to `flink/common/src/main/resources/sectors.json`).
- `test_generator.py` — pytest covering required fields and seed-determinism.

## CLI

```bash
python -m trade_gen \
  --bootstrap localhost:9094 \
  --registry  http://localhost:8081 \
  --rate      20         \
  --clients   50         \
  --seed      42         \
  --duration  0          # 0 = run forever
```

Logs `sent N trades` to stderr every 100 messages. `producer.flush()` runs on shutdown.

## Tests

```bash
cd tools && source .venv/bin/activate
pytest trade_gen/test_generator.py -v
```
