# Grafana provisioning

Files and directories under here are auto-loaded by Grafana on startup, per its [provisioning conventions](https://grafana.com/docs/grafana/latest/administration/provisioning/).

- `datasources/` — datasource definitions (one YAML per file). Currently has `prometheus.yml`.
- `dashboards/` *(create as needed)* — dashboard JSON exports plus a `provider.yml` that points Grafana at the directory. Empty by default.

Reload after edits: `docker-compose restart grafana`.
