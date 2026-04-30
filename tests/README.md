# tests/

Black-box tests that go beyond what the per-module unit tests cover.

## Subdirectories

- `e2e/` — pytest scenarios that drive the full `docker-compose` stack: produce trades + limits, wait for breaches to land in Pinot, assert correctness.

Unit tests for individual Flink operators live next to their source under `flink/<module>/src/test/java/...`. Unit tests for the Python `TradeFactory` live next to its source under `tools/trade_gen/test_generator.py`.
