# Tests for com.example.risk

`RiskEvaluatorTest` uses `KeyedTwoInputStreamOperatorTestHarness` (the broadcast-state variant) to exercise `RiskEvaluator`.

Scenarios:

| Test | Asserts |
|------|---------|
| `singleOrderNotionalBreachFires` | a single oversized trade emits exactly one `SINGLE_ORDER_NOTIONAL` breach |
| `grossNotionalBreachFires` | a position event populates `MapState<symbol, SymbolNotional>`; a subsequent trade triggers `GROSS_NOTIONAL` because the position's notional exceeds the limit |
| `velocityBreachFires` | 4 trades within 60 s under a limit of 3 emits at least one `TRADE_VELOCITY` breach |
| `noLimitConfiguredYieldsNoBreaches` | absent client-limit broadcast → no evaluation, no emissions |
| `stalePositionEventIsIgnored` | a position event with an older `last_update` than what's stored is rejected; the fresher value remains in state |

Run: `mvn -pl risk-engine -am test`.
