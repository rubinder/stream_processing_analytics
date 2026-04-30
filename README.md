# Streaming Risk Analytics

End-to-end demo: synthetic equity trades → Kafka → two Flink jobs (positions + breach detection) → Pinot → React dashboard.

**Stack:** Apache Kafka 4.2.0 (KRaft), Apache Flink 2.2.0 (DataStream API), Apache Pinot 1.5.0, Confluent Schema Registry CP 8.x, Java 21, Python 3.12+, React + Vite + TypeScript + Tailwind.

## Quick start

Prerequisites: Docker Desktop, Java 21+, Maven 3.9+, Node 20+, Python 3.12+.

```bash
# 1. Start the stack
make up

# 2. Create topics + register schemas (first time only)
make topics
make register-schemas

# 3. Build Flink JARs
cd flink && mvn -pl position-engine -pl risk-engine -am package -DskipTests
cd ..

# 4. Submit Flink jobs
make submit-jobs

# 5. Register Pinot tables (first time only)
make pinot-tables

# 6. Seed default per-client limits
make seed-limits

# 7. Start the trade generator (this runs in the foreground)
make trade-gen

# 8. In another shell — start the dashboard
cd dashboard && npm install && npm run dev
```

Open in your browser:
- **Dashboard:** http://localhost:5173
- **Flink UI:** http://localhost:8082
- **Pinot UI:** http://localhost:9000
- **Kafka UI:** http://localhost:8080
- **Grafana:** http://localhost:3000

Stop the stack: `make down`. Wipe state: `make clean`.

## Layout

- `schemas/` — Avro schemas (single source of truth)
- `flink/` — Two Flink jobs: `position-engine` and `risk-engine`, plus `common` module
- `tools/` — Python utilities: `trade_gen` (synthetic trade publisher) and `limits_cli`
- `pinot/` — Pinot table & schema configs (3 REALTIME tables)
- `dashboard/` — React/Vite/Tailwind/TanStack Query/Recharts frontend
- `tests/e2e/` — pytest E2E breach scenarios
- `observability/` — Prometheus + Grafana provisioning
- `docs/superpowers/` — design spec and implementation plan

## Design

See `docs/superpowers/specs/2026-04-30-streaming-risk-analytics-design.md` for the full design (architecture, schemas, late-data policy, observability, testing strategy).

## Running E2E tests

After `make all` completes:

```bash
cd tools && source .venv/bin/activate
cd ../tests/e2e
pytest -v
```

## Development

- Run Flink unit tests: `cd flink && mvn test`
- Run Python tests: `cd tools && source .venv/bin/activate && pytest`
- Build dashboard: `cd dashboard && npm run build`
