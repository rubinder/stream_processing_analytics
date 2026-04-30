# tools/

Python utilities for driving the pipeline end-to-end. Both packages are installed together via the project-level `pyproject.toml`.

## Setup (once)

```bash
cd tools
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Packages

| Package | Entrypoint | Purpose |
|---------|-----------|---------|
| `trade_gen/` | `python -m trade_gen [--rate N --clients N --duration SECONDS]` | Streams Avro-encoded synthetic equity trades to the `trades` Kafka topic. Deterministic given a seed (except `event_ts`, which is wall-clock). |
| `limits_cli/` | `python -m limits_cli [seed --clients N \| set --client-id C0001 --max-gross … --max-order … --max-tpm …]` | Publishes `ClientLimit` records to the compacted `client-limits` topic. `seed` writes defaults for N clients; `set` overrides one. |

## Make targets

```bash
make seed-limits   # python -m limits_cli seed --clients 50
make trade-gen     # python -m trade_gen --rate 20  (foreground)
```

## Avro encoding

Both CLIs use the **Confluent wire format**:

```
magic byte 0x00 | 4-byte big-endian schema_id | Avro body
```

The schema ID is fetched at runtime from Schema Registry (subject `<topic>-value`, e.g. `trades-value`, `client-limits-value`). The Avro body is produced via `fastavro.schemaless_writer` against the schema loaded from `../schemas/*.avsc`.

## Common pitfalls

- The Schema-Registry subject is named after the **topic**, not the schema file (e.g., `trades-value`, not `trade-value`). The mapping is in each CLI.
- Run inside the venv (`source .venv/bin/activate`); otherwise `confluent-kafka` / `fastavro` won't be importable.
