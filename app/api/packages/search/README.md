# `app/api/packages/search/` — Package Search API

Debounced autocomplete endpoint for the Stack Watchlist add-item form. Rendered at `GET /api/packages/search`.

## `GET /api/packages/search?q=<query>`

Search packages across a curated list (~140 entries) with npm registry fallback.

**Query params:**

| Param | Required | Description |
|-------|----------|-------------|
| `q` | Yes | Search query (min 1 char, case-insensitive substring) |

**Response:**

```json
{
  "results": [
    { "name": "Tailwind CSS", "ecosystem": "npm", "source": "curated" },
    { "name": "Django", "ecosystem": "PyPI", "source": "curated" },
    { "name": "some-npm-package", "ecosystem": "npm", "source": "npm" }
  ]
}
```

**Search strategy:**

1. **Curated list** (`src/config/package-suggestions.ts`) — 140+ entries across npm, PyPI, crates.io, Go, Maven, NuGet, RubyGems, Packagist, and infra tools. Filtered by case-insensitive substring match.
2. **npm registry fallback** — if curated results < 8, queries `https://registry.npmjs.org/-/v1/search?text=...` (3s timeout). Results are deduplicated against curated entries.
3. **Merge** — curated first, then npm, max 10 results total.

**Edges:**
- Empty query → `{ results: [] }`
- npm API failure → silently skipped (curated results still returned)
- 3s timeout on npm fetch to avoid hanging the search UX

**Consumed by:** `PackageSearchInput` component in `app/watchlist/page.tsx` (200ms debounce, click-to-select sets name + ecosystem).
