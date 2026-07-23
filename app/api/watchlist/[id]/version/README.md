# `app/api/watchlist/[id]/version/` — Version Fetch Endpoint

Manual latest-version lookup for a single watchlist item. Rendered at `POST /api/watchlist/{id}/version`.

## `POST /api/watchlist/[id]/version`

Queries the correct package registry (npm, PyPI, Go, NuGet, crates.io, RubyGems) for the latest version and updates `latest_version`.

**Response (200):**

```json
{
  "ok": true,
  "version": "20.1.0",
  "item": { "id": 1, "name": "Next.js", "latest_version": "20.1.0", ... }
}
```

**Responses:**

| Status | Body |
|--------|------|
| 200 | `{ ok, version, item }` |
| 404 | `{ "error": "Item not found" }` |
| 502 | `{ "error": "Could not fetch version" }` |

**Registry support (7):**

| Ecosystem | API |
|-----------|-----|
| npm | `https://registry.npmjs.org/{name}/latest` |
| PyPI | `https://pypi.org/pypi/{name}/json` |
| Go | `https://proxy.golang.org/{name}/@latest` |
| NuGet | `https://api.nuget.org/v3-flatcontainer/{name}/index.json` |
| crates.io | `https://crates.io/api/v1/crates/{name}` |
| RubyGems | `https://rubygems.org/api/v1/gems/{name}.json` |

Name is run through `toRegistryName()` from `src/lib/package-name-map.ts` for correct registry lookups (e.g., "Next.js" → "next").

Also updates `updated_at` to the current timestamp.

**Consumed by:** `checkVersion(id)` in `app/watchlist/page.tsx` (Fetch button in expanded panel).
