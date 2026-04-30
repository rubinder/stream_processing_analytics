# observability/

Configuration for the Prometheus + Grafana side of the stack. Both services run as containers in `docker-compose.yml`; this directory holds the bind-mounted config they read at startup.

## Files

- `prometheus.yml` — Prometheus scrape config. Two jobs:
  - `flink-jm` → `flink-jobmanager:9249`
  - `flink-tm` → `flink-taskmanager:9250`
  Both ports expose Flink's built-in `PrometheusReporter` metrics endpoint, enabled via `metrics.reporters: prom` in each Flink container's `FLINK_PROPERTIES`.
- `grafana/provisioning/datasources/prometheus.yml` — auto-registers the local Prometheus as the default Grafana datasource at startup.

## Open

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (anonymous Admin enabled — `GF_AUTH_ANONYMOUS_ENABLED=true` in compose)

## Custom counters exposed by the jobs

- `risk-engine`:
  - `breaches_emitted_gross_notional`
  - `breaches_emitted_single_order`
  - `breaches_emitted_velocity`
- `position-engine`:
  - `late_records_dropped_total` (`LatenessRouter` DLQ counter)

These appear under the `flink_taskmanager_job_task_operator_*` metric prefix that the Prometheus reporter applies. Search in Grafana / Prometheus with e.g. `flink_taskmanager_job_task_operator_breaches_emitted_velocity`.

## What's not provisioned

No pre-built Grafana dashboards (yet) — Grafana comes up empty. Build dashboards interactively, or drop JSON exports under `grafana/provisioning/dashboards/` for auto-import.
