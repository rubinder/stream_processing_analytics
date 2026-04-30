SHELL := /usr/bin/env bash
.PHONY: up down clean topics register-schemas pinot-tables submit-jobs seed-limits trade-gen all

KAFKA := docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092

up:
	docker compose up -d
	@echo "Waiting for services..."
	@sleep 5

down:
	docker compose down

clean:
	docker compose down -v
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
	@echo "Implemented in Phase 4"

submit-jobs:
	@echo "Implemented in Phase 5"

seed-limits:
	@echo "Implemented in Phase 2"

trade-gen:
	@echo "Implemented in Phase 2"

all: up topics register-schemas pinot-tables submit-jobs seed-limits trade-gen
