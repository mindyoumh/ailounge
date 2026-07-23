# `/api/repo-radar` — Repo Radar API

REST API for tracking GitHub repositories. Integrates with the GitHub REST API (v3) to fetch repo metadata, releases, PRs, and issues.

## Routes

### `GET /api/repo-radar` — List tracked repos

- Returns all active (`is_active = 1`) repos ordered by stars descending
- **Response**: `{ items: RepoRadarItem[] }`

### `POST /api/repo-radar` — Add a new repo

- **Body**: `{ owner: string, repo: string }`
- **Pipeline**: Validates → checks duplicate → GitHub API (repo info, latest release, recent PRs, recent issues) → detects breaking changes & security advisories → inserts into Supabase PostgreSQL
- **Response** `201`: `{ ok: true, item: RepoRadarItem }`
- **Errors** `400`: missing owner/repo. `404`: repository not found. `409`: already tracked. `429`: rate limited

### `PATCH /api/repo-radar/[id]` — Update repo

- **Body**: partial — allowed fields: `notes`, `is_active`
- Updates `updated_at` timestamp automatically
- **Response**: `{ ok: true, item: RepoRadarItem }`
- **Error** `404`: repo not found

### `DELETE /api/repo-radar/[id]` — Remove a repo

- **Role required:** `lead` or `dev` (gated by `requireRole(request, ["lead", "dev"])`)
- Hard delete from Supabase PostgreSQL
- **Response**: `{ ok: true }`
- **Error** `404`: repo not found

### `POST /api/repo-radar/refresh` — Refresh all repos

- Calls `refreshAll()` from `src/lib/repo-radar.ts`
- Iterates all active repos, fetches fresh GitHub data for each
- Updates `kv_store` with `repo_radar:last_refresh`, `repo_radar:status`, `repo_radar:count`
- **Response**: `{ ok: true, updated: number, errors: number, results: [...] }`

## GitHub API Integration

- **Rate limiting**: uses `GH_ACCESS_TOKEN` env var for authenticated requests (5000 requests/hour). Falls back to unauthenticated (60 requests/hour) if no token set
- **405 method not allowed**: DELETE/PATCH return errors for missing items
- **User-Agent**: `my-ailounge/1.0`

## Schema

All routes use the `getDb()` singleton. The `repo_radar_items` table has 27 columns including star tracking, release metadata, PR/issue activity windows, and detection flags.
