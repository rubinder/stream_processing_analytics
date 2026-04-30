# observability/grafana/

Bind-mounted into the `grafana` container at `/etc/grafana/provisioning`. Grafana auto-loads everything under here at startup.

## Layout

- `provisioning/datasources/prometheus.yml` — declares the local Prometheus as the default datasource.
- `provisioning/dashboards/` — empty. Drop Grafana dashboard JSON files here (with a small `provider.yml`) to auto-import them on container restart.

For the local demo, the absence of pre-built dashboards is intentional — explore Flink/Kafka metrics interactively. Reference: `flink_taskmanager_job_task_operator_*` is where Flink's built-in operator counters land, and our custom counters (e.g. `breaches_emitted_*`, `late_records_dropped_total`) follow the same prefix.
