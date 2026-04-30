# Grafana datasources

`prometheus.yml` declares the local Prometheus (running at `http://prometheus:9090` inside the docker network) as the default Grafana datasource.

If you add another datasource (e.g., a Loki for logs, or a separate Prometheus for app metrics), drop another YAML in this directory — Grafana picks them all up on startup.
