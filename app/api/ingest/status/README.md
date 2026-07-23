# `app/api/ingest/status/` — Ingestion Status API

Single `GET` endpoint returning per-ingester status + summary.

## Route

| Method | Path | File | Description |
|--------|------|------|-------------|
| `GET` | `/api/ingest/status` | `route.ts` | Per-ingester status with summary |

## Response Shape

```json
{
  "sources": [
    { "source": "hn", "lastRun": "2026-07-10T12:00:00.000Z", "status": "ok", "count": 12, "elapsedMs": 3456 },
    { "source": "rss", "lastRun": "2026-07-10T12:00:00.000Z", "status": "ok", "count": 5, "elapsedMs": 1234 },
    { "source": "github_trending", "lastRun": "2026-07-10T12:00:00.000Z", "status": "ok", "count": 3, "elapsedMs": 5678 },
    { "source": "repo_radar", "lastRun": "2026-07-10T12:00:00.000Z", "status": "ok", "count": 0, "elapsedMs": 9876 }
  ],
  "summary": {
    "status": "ok",
    "lastRun": "2026-07-10T12:00:00.000Z",
    "errors": 0,
    "total": 4
  }
}
```

| Field | Source |
|-------|--------|
| `sources` | `getIngestionStatus()` — reads `ingest:*` keys from `kv_store` |
| `summary.status` | `kv_store` key `ingest:status:all` |
| `summary.lastRun` | `kv_store` key `ingest:last_run:all` |
| `summary.errors` | Count of sources with `status === 'error'` |

## Consumed By

- **IngestHealth** component (`components/briefing/ingest-health.tsx`) — homepage per-ingester health widget
