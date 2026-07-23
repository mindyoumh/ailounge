# `app/api/stats/` — Sidebar Quick Stats API

Single `GET` endpoint consumed by the Sidebar's Quick Stats panel. Returns aggregated counts and ingestion timing.

## Route

| Method | Path | File | Description |
|--------|------|------|-------------|
| `GET` | `/api/stats` | `route.ts` | Aggregate counts + last ingest timestamp |

## Response Shape

```json
{
  "totalItems": 42,
  "lastIngest": "2026-06-24T12:00:00.000Z",
  "itemsToday": 7,
  "itemsThisWeek": 23,
  "errors": 0,
  "stackTotal": 14,
  "stackHigh": 1,
  "stackMedium": 3,
  "stackLow": 10
}
```

| Field | Type | Source |
|-------|------|--------|
| `totalItems` | `number` | `SELECT COUNT(*) FROM feed_items WHERE source != 'manual'` |
| `lastIngest` | `string\|null` | `getLastGlobalIngestion()` — reads `ingest:last_run:all` from `kv_store` |
| `itemsToday` | `number` | `getItemsToday()` — filtered by `fetched_at` between today midnight and tomorrow midnight (JS `Date`) |
| `itemsThisWeek` | `number` | `getItemsThisWeek()` — filtered by `fetched_at >= 7 days ago` (JS `Date`) |
| `errors` | `number` | `getIngestionStatus()` count where `status === 'error'` |
| `stackTotal` | `number` | `SELECT COUNT(*) FROM watchlist_items` |
| `stackHigh` | `number` | Count of `watchlist_items` with `risk_level='high'` |
| `stackMedium` | `number` | Count of `watchlist_items` with `risk_level='medium'` |
| `stackLow` | `number` | Count of `watchlist_items` with `risk_level='low'` |

## Data Flow

```
Sidebar (client)
  └─ useEffect → fetch("/api/stats")
                    └─ GET /api/stats
                         ├─ supabase.from("feed_items").select("*", { count: "exact", head: true })
                         ├─ supabase.from("watchlist_items").select("*", { count: "exact", head: true })
                         ├─ supabase.from("watchlist_items").select("risk_level")
                         ├─ getLastGlobalIngestion()
                         ├─ getItemsToday()
                         ├─ getItemsThisWeek()
                         └─ getIngestionStatus().filter(s => s.status === "error")
```

## Consumed By

- **Sidebar**: Quick Stats panel (4 rows: total items, last ingest, today, this week)
- **Homepage**: `StackSummary` widget (shows stack total + high/medium/low risk counts, links to `/watchlist`)

## Re-exported Analytics Functions

All queries delegate to `src/lib/analytics.ts`:
- `getLastGlobalIngestion()` — key `ingest:last_run:all`
- `getItemsToday()` — date filter
- `getItemsThisWeek()` — 7-day window
- `getIngestionStatus()` — per-source status from `kv_store`
