# limits_cli

Publishes `ClientLimit` records to the compacted `client-limits` Kafka topic. Each record sets `(max_gross_notional, max_single_order_notional, max_trades_per_minute)` for a client; the topic's compaction means latest-per-client wins, and the risk-engine job replays current limits from earliest on bootstrap.

## Files

- `__main__.py` — Click CLI with two subcommands.

## Subcommands

```bash
# Seed default limits for C0000..C{N-1}
python -m limits_cli seed --clients 50

# Override one client's limits
python -m limits_cli set \
  --client-id C0042 \
  --max-gross 5000000 \
  --max-order 100000 \
  --max-tpm   30
```

Defaults: `max_gross=1_000_000`, `max_order=100_000`, `max_tpm=30`.

## Encoding

Same Confluent wire format as `trade_gen` — magic byte `0x00`, 4-byte schema ID for subject `client-limits-value`, then Avro body via `fastavro.schemaless_writer`.

## Make target

`make seed-limits` runs `seed --clients 50` against the local stack.
