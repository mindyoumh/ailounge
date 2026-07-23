# `app/api/watchlist/[id]/cve/` — CVE Refresh Endpoint

Manual vulnerability re-check for a single watchlist item. Rendered at `POST /api/watchlist/{id}/cve`.

## `POST /api/watchlist/[id]/cve`

Re-queries the [OSV.dev API](https://osv.dev) for the item's package and ecosystem, stores the result as a JSON payload in `known_vulns`, and auto-bumps `risk_level` based on highest severity.

**Response (200):**

```json
{
  "ok": true,
  "cves": [
    {
      "id": "CVE-2024-1234",
      "summary": "Buffer overflow in...",
      "severity": "HIGH",
      "aliases": ["GHSA-xxxx-xxxx"],
      "published": "2024-01-15T00:00:00Z"
    }
  ],
  "item": { "id": 1, "name": "Next.js", "known_vulns": "{...}", "risk_level": "medium", ... }
}
```

**Responses:**

| Status | Body |
|--------|------|
| 200 | `{ ok, cves, item }` |
| 404 | `{ "error": "Item not found" }` |

**Stored payload shape** (value of `known_vulns` column):

```json
{
  "lastChecked": "2026-07-14T04:10:00.000Z",
  "totalCount": 3,
  "highestSeverity": "HIGH",
  "summaryText": "3 CVEs found (2 high, 1 moderate)",
  "cves": [...]
}
```

**Risk level auto-bump:**
- `CRITICAL` or `HIGH` → `risk_level: "high"`
- `MODERATE` → `risk_level: "medium"`
- `LOW` → no risk_level change

**Consumed by:** `checkCve(id)` in `app/watchlist/page.tsx` (Refresh button in expanded panel).
