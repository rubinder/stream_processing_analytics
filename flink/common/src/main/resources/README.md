# Classpath resources

`sectors.json` — symbol → equities-sector map. Loaded by `SectorLookup` at class init. The same file lives at `tools/trade_gen/sectors.json` so the generator and Flink agree; if you update one, mirror to the other.

Anything under `src/main/resources/` ends up on the classpath of the shaded JAR and is reachable as `getResourceAsStream("/<filename>")`.
