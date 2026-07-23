# `app/api/watchlist/` — Watchlist API Routes

CRUD API for Stack Watchlist version/risk tracking. Six route files.

---

## List & Create — `route.ts`

### `GET /api/watchlist`

List all watchlist items ordered by `category ASC, name ASC`.

**Response:**

```json
{
  "items": [
    { "id": 1, "name": "Next.js", "category": "framework", "ecosystem": "npm", "risk_level": "low", ... }
  ]
}
```

### `POST /api/watchlist`

Add a new watchlist item. Auto-detects ecosystem, fetches latest version, and checks vulnerabilities.

**Request body:**

```json
{
  "name": "string (required, unique)",
  "category": "string (default: null)",
  "ecosystem": "string (default: auto-detected via detectEcosystem())",
  "installed_version": "string | null",
  "latest_version": "string | null",
  "risk_level": "'low' | 'medium' | 'high' (default: 'low')",
  "risk_reason": "string | null",
  "upgrade_notes": "string | null",
  "known_vulns": "string | null",
  "migration_link": "string | null"
}
```

**Responses:**

| Status | Body |
|--------|------|
| 201 | `{ "ok": true, "id": 3 }` |
| 400 | `{ "error": "name is required" }` or `{ "error": "risk_level must be low, medium, or high" }` |
| 409 | `{ "error": "Item with this name already exists" }` |

UNIQUE constraint on `name`. Invalid risk_level returns 400.

**Side effects (fire-and-forget after insert):**

1. `ecosystem` auto-detection via `detectEcosystem(name)` if not provided
2. `fetchLatestVersion(name, ecosystem)` — queries npm/PyPI/Go/NuGet/crates.io/RubyGems registry, updates `latest_version`
3. `checkVulnerabilities(name, ecosystem)` — queries OSV.dev API, stores structured CVE payload in `known_vulns`, auto-bumps `risk_level` based on severity
4. `retroactivelyScore({ name, category })` — rescans existing `feed_items` for matches, updates relevance scores

---

## Update & Delete — `[id]/route.ts`

### `PATCH /api/watchlist/[id]`

Update specific fields. Only these 10 fields are accepted:

```
name, category, ecosystem, installed_version, latest_version,
risk_level, risk_reason, upgrade_notes, known_vulns, migration_link
```

**Request body** (any subset):

```json
{
  "installed_version": "19.2.1",
  "latest_version": "20.0.0",
  "risk_level": "high"
}
```

Auto-sets `updated_at = new Date().toISOString()` (JavaScript `Date`) on every update.

Risk level is validated: `low`, `medium`, or `high` only.

**Responses:**

| Status | Body |
|--------|------|
| 200 | `{ "ok": true, "item": { ...updated item... } }` |
| 400 | `{ "error": "No fields to update" }` or invalid risk_level |
| 404 | `{ "error": "Item not found" }` |

### `DELETE /api/watchlist/[id]`

Remove a watchlist item. **Requires `lead` role** — guarded by `requireRole(request, ["lead"])`. Non-lead users receive 403.

**Responses:**

| Status | Body |
|--------|------|
| 200 | `{ "ok": true }` |
| 403 | `{ "error": "Forbidden — requires one of roles: lead" }` |
| 404 | `{ "error": "Item not found" }` |

---

## CVE Refresh — `[id]/cve/route.ts`

### `POST /api/watchlist/[id]/cve`

Re-checks vulnerabilities via OSV.dev API. Updates `known_vulns` (JSON payload with `lastChecked`, `totalCount`, `highestSeverity`, `summaryText`, `cves[]`) and auto-bumps `risk_level`.

**Response:**

```json
{
  "ok": true,
  "cves": [{ "id": "CVE-2024-1234", "summary": "...", "severity": "HIGH", ... }],
  "item": { ...full updated item... }
}
```

**Responses:** 200 (success), 404 (not found).

---

## Version Fetch — `[id]/version/route.ts`

### `POST /api/watchlist/[id]/version`

Fetches the latest version from the correct package registry. Updates `latest_version` and `updated_at`.

**Response:**

```json
{
  "ok": true,
  "version": "20.1.0",
  "item": { ...full updated item... }
}
```

**Responses:** 200 (success), 404 (not found), 502 (registry fetch failed).

Registry support: npm, PyPI, Go, NuGet, crates.io, RubyGems. Name is normalized via `toRegistryName()` in `src/lib/package-name-map.ts`.

---

## PDF Export — `export/route.ts`

### `GET /api/watchlist/export`

Generate and download a PDF report of all watchlist items. Uses `pdf-lib` for PDF generation — no HTML-to-PDF conversion.

**Response:** Binary PDF download with `Content-Type: application/pdf` and `Content-Disposition: attachment` filename `stack-watchlist-{date}.pdf`.

**Table layout (5 columns):**

| Column | Content |
|--------|---------|
| Name | Item name |
| Category | Item category |
| Version | `installed → latest` with semver drift label |
| Vulns | Parsed CVE count from `known_vulns` JSON, or `—` if none |
| Risk | Risk level (green/amber/red) |

**Responses:**

| Status | Body |
|--------|------|
| 200 | Binary PDF |
| 404 | `{ "error": "Watchlist is empty" }` |

**Error handling:** Empty watchlist returns 404.

**Unicode sanitization (commit `7dda602`):** All text passed to `pdf.drawText()` is run through `sanitize()` which strips non-Windows-1252 characters to prevent PDF generation failures. Common replacements: `→` → `->`, `—` → `-`, `…` → `...`, smart quotes → straight quotes. Non-ANSI characters are replaced with `?`.
