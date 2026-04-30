SHELL := /usr/bin/env bash
.PHONY: up down clean topics register-schemas pinot-tables submit-jobs seed-limits trade-gen all

KAFKA := docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092

up:
	docker-compose up -d
	@echo "Waiting for services..."
	@sleep 5

down:
	docker-compose down

clean:
	docker-compose down -v
	rm -rf checkpoints/ volumes/

topics:
	$(KAFKA) --create --if-not-exists --topic trades            --partitions 4 --replication-factor 1
	$(KAFKA) --create --if-not-exists --topic enriched-trades   --partitions 4 --replication-factor 1
	$(KAFKA) --create --if-not-exists --topic positions         --partitions 4 --replication-factor 1
	$(KAFKA) --create --if-not-exists --topic breaches          --partitions 4 --replication-factor 1
	$(KAFKA) --create --if-not-exists --topic dlq               --partitions 1 --replication-factor 1
	$(KAFKA) --create --if-not-exists --topic client-limits     --partitions 4 --replication-factor 1 \
		--config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.01 --config segment.ms=60000
	$(KAFKA) --list

register-schemas:
	@for f in schemas/*.avsc; do \
		subject=$$(basename $$f .avsc); \
		case $$subject in \
			trade) topic=trades ;; \
			enriched_trade) topic=enriched-trades ;; \
			client_limit) topic=client-limits ;; \
			position) topic=positions ;; \
			breach) topic=breaches ;; \
		esac; \
		echo "Registering $$f for $$topic-value..."; \
		jq -Rs '{schema: .}' $$f | \
			curl -fsS -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
			--data @- http://localhost:8081/subjects/$$topic-value/versions; \
		echo; \
	done

pinot-tables:
	@for name in trades positions breaches; do \
		echo "Adding $$name schema..."; \
		curl -fsS -X POST -H "Content-Type: application/json" \
			-d @pinot/$${name}_schema.json http://localhost:9000/schemas; echo; \
		echo "Adding $$name table..."; \
		curl -fsS -X POST -H "Content-Type: application/json" \
			-d @pinot/$${name}_table.json http://localhost:9000/tables; echo; \
	done

submit-jobs:
	docker-compose exec -T flink-jobmanager flink run -d -c com.example.position.PositionEngineJob /jars/position-engine/position-engine-0.1.0.jar
	docker-compose exec -T flink-jobmanager flink run -d -c com.example.risk.RiskEngineJob /jars/risk-engine/risk-engine-0.1.0.jar || echo "(risk-engine not built yet — that's OK)"

seed-limits:
	cd tools && . .venv/bin/activate && python -m limits_cli seed --clients 50

trade-gen:
	cd tools && . .venv/bin/activate && python -m trade_gen --rate 20

all: up topics register-schemas pinot-tables submit-jobs seed-limits trade-gen
