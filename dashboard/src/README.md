# dashboard/src/

React + TypeScript source for the Streaming Risk Dashboard.

## Files

- `main.tsx` — Vite entrypoint; mounts `<App />` into `#root` with React `StrictMode`.
- `App.tsx` — top-level shell. Sets up `QueryClientProvider`, the `ClientFilter` dropdown, and the four-panel grid (sparkline, top-exposed, positions, breach feed).
- `pinot.ts` — thin wrapper around `fetch(POST)`. Sends SQL to the Vite dev-server proxy at `/pinot` (which forwards to `http://localhost:8099/query/sql`). Decodes Pinot's columnar response into row objects.
- `index.css` — Tailwind v4 import plus minimal dark-mode body defaults.
- `panels/` — one component per dashboard panel.
- `assets/` — Vite scaffold leftovers (the React logo). Not used by the app.

## Pinot SQL via Vite proxy

The browser can't fetch `http://localhost:8099` directly because Pinot doesn't return CORS headers. `vite.config.ts` adds a `/pinot` → `http://localhost:8099/query/sql` proxy, so the dashboard issues same-origin POSTs and Vite forwards them server-side.

If you change `vite.config.ts`, restart `npm run dev`.

## Querying conventions

- Decimal columns are queried via their DOUBLE companions (`notional_double`, `avg_price_double`) — Pinot can't aggregate / order BYTES decimals.
- Polling cadences (per panel): sparkline 10 s, top-exposed 5 s, positions / breach-feed 2 s. Set on the `useQuery({ refetchInterval })`.
