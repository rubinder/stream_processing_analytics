# Tests for com.example.position

Unit tests exercise each operator in isolation using Flink's test harnesses (no MiniCluster).

| Test | Harness | Coverage |
|------|---------|----------|
| `EnricherTest` | (plain JUnit, no harness) | sign of `signed_qty` for BUY/SELL, `notional = qty × price`, sector lookup including `UNKNOWN` |
| `LatenessRouterTest` | `OneInputStreamOperatorTestHarness` | on-time event passes to main output; > 60 s-late event routed to `DLQ_TAG` |
| `PositionUpdaterTest` | `KeyedOneInputStreamOperatorTestHarness` | net-quantity accumulation, late-trade does NOT regress `last_price`, `last_update` is monotonic, side-output emits one `EnrichedTrade` per input |
| `ClientSymbolPartitionerTest` | (plain JUnit) | same `(client, symbol)` always lands on the same partition; different pairs spread across partitions |

Run all: `mvn -pl position-engine -am test`. Run one: `mvn -pl position-engine -am test -Dtest=PositionUpdaterTest`.
