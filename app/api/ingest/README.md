# `app/api/ingest/` — Ingestion API Routes

Two route files: trigger ingestion and query status.

## Trigger — `route.ts`

### `POST /api/ingest`

Calls `runAll({ closeDb: false })` from `src/ingesters/run-all.ts` and returns JSON results.

Called by the `IngestButton` component (`components/engineering-intelligence/ingest-button.tsx`).

**Response:**
```json
{
  "results": {
    "hn": { "ok": true, "inserted": 5, "elapsed": 1234 },
    "github_trending": { "ok": true, "inserted": 3, "elapsed": 567 },
    "rss": { "ok": true, "inserted": 12, "elapsed": 890 },
    "repo_radar": { "ok": true, "inserted": 0, "elapsed": 9876 }
  },
  "allOk": true
}
```

JWT auth required. **Error** `500`: ingestion failure with error message.

## Status — `status/route.ts`

### `GET /api/ingest/status`

Returns per-ingester status + aggregate summary. No auth required (public).

See [`status/README.md`](./status/README.md) for full docs.
