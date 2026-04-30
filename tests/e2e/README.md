# tests/e2e/

End-to-end pytest suite that drives the full local stack and asserts the pipeline produces the expected breaches.

## Files

- `conftest.py` — `pinot_query` helper (wraps `POST /query/sql`), `wait_for(predicate, timeout)` polling helper, and pytest fixtures `query` and `waitfor`.
- `test_breach_scenarios.py` — three scenarios, one per breach type:
  - `test_single_order_notional_breach` — one oversized order → `SINGLE_ORDER_NOTIONAL` breach lands in Pinot.
  - `test_gross_notional_breach` — accumulate position above limit → next trade fires `GROSS_NOTIONAL`.
  - `test_velocity_breach` — burst of N+1 trades in 60 s → `TRADE_VELOCITY` breach.

Each test uses a unique `client_id` (e.g. `TC_VEL_<hex>`) so the assertions don't collide with the seeded clients or other tests.

## Prereqs

The full stack must be up and the pipeline running:

```bash
make up
make topics && make register-schemas && make pinot-tables
mvn -f flink package -DskipTests
make submit-jobs
make seed-limits
make trade-gen   # in another terminal (foreground)
```

## Run

```bash
cd tools && source .venv/bin/activate
cd ../tests/e2e
pytest -v
```

Expected runtime: ~10–30 s per scenario depending on Pinot ingestion lag.

## Encoding details

Tests publish Avro via the same Confluent wire format as the `trade_gen` / `limits_cli` CLIs — schema fetched from Schema Registry by topic-derived subject (e.g., `trades-value`, `client-limits-value`).
