# dashboard/src/panels/

One component per dashboard panel. Each is a self-contained `useQuery({ refetchInterval })` hook that hits the Vite `/pinot` proxy and renders the result.

| File | Panel | Refresh | Pinot SQL |
|------|-------|---------|-----------|
| `BreachesSparkline.tsx` | Breaches per minute, last 1 h | 10 s | `SELECT DATETRUNC('MINUTE', detected_ts) AS minute, COUNT(*) AS cnt FROM breaches WHERE detected_ts > ago('PT1H') GROUP BY minute ORDER BY minute` |
| `TopExposed.tsx` | Top-10 Exposed Clients | 5 s | `SELECT client_id, SUM(ABS(notional_double)) AS gross FROM positions GROUP BY client_id ORDER BY gross DESC LIMIT 10` |
| `Positions.tsx` | Positions table (filterable by client) | 2 s | `SELECT symbol, net_quantity, notional_double, sector FROM positions [WHERE client_id = ?] ORDER BY ABS(notional_double) DESC LIMIT 100` |
| `BreachFeed.tsx` | Live Breach Feed (last 5 min) | 2 s | `SELECT client_id, breach_type, actual_value, limit_value, details, detected_ts FROM breaches WHERE detected_ts > ago('PT5M') ORDER BY detected_ts DESC LIMIT 50` |

## Why `notional_double`?

Pinot's Avro decoder reads `bytes(decimal)` as raw bytes; `SUM`/`ABS`/`ORDER BY` on those columns either errors or yields garbage. The position-engine emits a `notional_double` (and `avg_price_double`) DOUBLE companion alongside each `Position` record so dashboards can aggregate / sort natively. The original BYTES column is preserved for precision.
