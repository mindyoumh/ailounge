# `app/watchlist/` — Stack Watchlist

Client-side page for tracking versions, risks, and resources across your tech stack. Rendered at `/watchlist`.

## Page — `page.tsx`

A **client component** with expandable rows, inline editing, a sortable/searchable table, and package search combobox.

### Search

A search input at the top filters items by `name`, `category`, or `upgrade_notes` (case-insensitive substring). Shows `N of M` count when search is active.

### Table Columns (sortable by clicking header)

| Column | Content | Editable? |
|--------|---------|-----------|
| (chevron) | Expand/collapse row | — |
| Name | Text | No |
| Category | Badge | No |
| Version Health | `installed → latest` with Drift badge (patch behind / minor behind / major update / up to date) | No (editable in expanded panel) |
| Vulns | Count badge with severity color (e.g., "3 CVEs" in rose for CRITICAL/HIGH, amber for MODERATE, muted for LOW) | No (editable in expanded panel) |
| Risk | Badge with icon + tooltip (ShieldCheck emerald / ShieldAlert amber / ShieldX rose) | No (editable in expanded panel) |
| Last Checked | Relative time (e.g., "2h ago") | No |
| Links | npm / GitHub / Docs icons (auto-detected from name, opens new tab) | No |
| Delete | Trash icon (with confirm dialog) — visible for `lead` and `dev` roles | — |

### Version Health

Semver comparison between `installed_version` and `latest_version`:

| Status | Badge | Color |
|--------|-------|-------|
| Up to date | "Up to date" | Emerald |
| Patch behind | "N patch(es) behind" | Amber |
| Minor behind | "N minor behind" | Orange |
| Major update | "Major update" | Rose |
| Unknown | `—` | Muted |

Version strings are parsed by stripping leading `v` and splitting on `.`. Missing or unparseable versions show `—`.

### Expanded Panel

Click the chevron on any row to reveal an inline editing panel:

| Field | Label | Widget | Editable? |
|-------|-------|--------|-----------|
| `installed_version` | Installed | Click-to-edit input | Yes |
| `latest_version` | Latest | Click-to-edit input + **Fetch** button | Yes |
| `ecosystem` | Ecosystem | Dropdown (8 ecosystems) | Yes |
| `known_vulns` | Known Vulns | Summary text + **Refresh** button | No (auto-refreshed) |
| `risk_reason` | Risk Reason | Click-to-edit input | Yes |
| `upgrade_notes` | Upgrade Notes | Click-to-edit input | Yes |
| `migration_link` | Migration Link | Click-to-edit input + ExternalLink icon when filled | Yes |

Each `EditableField` component: click to enter edit mode (auto-focus), Enter/Blur to save, Escape to cancel.

**Fetch button** — calls `POST /api/watchlist/{id}/version`, queries the package registry for the latest version, updates the row.
**Refresh button** — calls `POST /api/watchlist/{id}/cve`, re-queries OSV.dev for vulnerabilities, auto-bumps risk level.

### Add Item Form

Toggle-able form with:

| Field | Widget |
|-------|--------|
| Name | `PackageSearchInput` (debounced 200ms, searches curated list + npm registry, shows results in dropdown with ecosystem badge) |
| Category | Select (6 options: framework, database, infra, cloud, ai-sdk, tool) |
| Risk Level | Select (low / medium / high) |
| Risk Reason | Optional text input |

On submit, calls `POST /api/watchlist` which:
1. Auto-detects ecosystem via `detectEcosystem()`
2. Fetches latest version from the correct registry
3. Runs CVE check on OSV.dev
4. Triggers `retroactivelyScore()` to re-score existing feed items

### CRUD Operations

| Action | API Call |
|--------|----------|
| List items | `GET /api/watchlist` |
| Add item | `POST /api/watchlist` (name, category, ecosystem, risk_level, risk_reason) |
| Update field | `PATCH /api/watchlist/[id]` (whitelisted: name, category, ecosystem, installed_version, latest_version, risk_level, risk_reason, upgrade_notes, known_vulns, migration_link) |
| Delete item | `DELETE /api/watchlist/[id]` (lead/dev role only, with confirmation dialog, animated fade-out) |
| Refresh CVE | `POST /api/watchlist/[id]/cve` |
| Fetch version | `POST /api/watchlist/[id]/version` |

### Resource Links

Auto-detected based on item name. Common packages have hardcoded URLs (Next.js, React, Tailwind CSS, TypeScript, Supabase, PostgreSQL, Vite, Python, Django, Docker, Redis, Nginx, AWS, Node.js). Unrecognized names generate generic `npm`/`GitHub` URLs by slugifying the name.

### Risk Summary Footer

When items exist, at the bottom of the table:
- Total count (or `N of M` when searching)
- Per-level counts with colored dots (high=red, medium=amber, low=green)

### Empty & Error States

- **Empty**: Icon + "No items tracked yet" + "Add your first item" button
- **Empty with search**: "No items match your search."
- **Error**: Red banner with error message
- **Loading**: Animated pulse placeholders (6 rows)
- **Deleting**: Fade-out animation on deleted row
