# `src/lib/` — Shared Utilities

Shared helpers consumed by ingesters and the dashboard UI.

## Files

### `utils.ts`

CSS classname merger for Tailwind UI components.

```ts
cn("px-4", isActive && "bg-blue-500", className)
```

Uses `clsx` for conditional joining and `tailwind-merge` for conflict resolution.

---

### `db.ts`

Shared database write utility used by all ingesters.

**`IngestEntry` interface:**

```ts
interface IngestEntry {
  source: string;
  category: string;
  title: string;
  url: string;
  summary?: string;
  tags?: string;
  score?: number;
  published_at: string;
}
```

**`upsertEntry(entry)`** — Inserts a row into `feed_items`. Returns `'inserted'` or `'skipped'` (if `UNIQUE(source, url)` constraint fires).

**Relevance scoring integration:**

`upsertEntry()` calls `scoreRelevance()` from `relevance-scorer.ts` after a successful insert. If a match is found, it stores `ai_relevance_score`, `ai_relevance_label`, `ai_relevance_reason`, and `relevance_base` on the inserted row, then calls `recalcEngagementForItem()` from `engagement-scorer.ts` to apply engagement boosts.

Used by: `ingesters/manual-feeds`, `ingesters/rss`, `ingesters/hacker-news`, `ingesters/github-trending`.

---

### `relevance-scorer.ts`

Scores feed items against the user's stack watchlist to surface relevant content. Called automatically from `upsertEntry()` at ingestion time.

**`scoreRelevance(item)`** — accepts `{ title, summary?, tags?, category }`. Returns `{ score, label, reason }` or `null`.

**Scoring logic:**

| Match Type | Score | Condition |
|------------|-------|-----------|
| Exact keyword match | 80 | Watchlist item name appears in title/summary/tags |
| Partial word match | 60 | ≥50% of watchlist name words appear in text |
| Category match | 40 | Feed category matches watchlist item category |

**Caching:** Watchlist data is cached in-memory for 10 minutes (600,000ms) to avoid repeated DB queries during batch ingestion.

---

### `engagement-scorer.ts`

Boosts `ai_relevance_score` based on user engagement (pins/reads) to surface community-validated content.

**`recalcEngagementForItem(itemId)`** — recalculates score for a single item:
```
final = min(relevance_base + pins×5 + reads×1, 100)
```

Called from `upsertEntry()` (on insert) and from `retroactivelyScore()` (on retroactive update).

**`recalcAllEngagementScores()`** — recalculates for all items with non-null `relevance_base`. Called at the end of `runAll()` orchestrator.

---

### `retroactive-scorer.ts`

Re-scores existing feed items when the watchlist grows, so new watchlist entries immediately surface relevant historical content.

**`retroactivelyScore({ name, category })`** — finds feed items whose `title`, `summary`, `tags`, or `category` match the new watchlist item, runs them through `scoreRelevance()`, and updates `relevance_base`, `ai_relevance_score`, `ai_relevance_label`, `ai_relevance_reason`. Then calls `recalcEngagementForItem()` for each matched item.

Called from `POST /api/watchlist` after a successful insert.

---

### `auth-helpers.ts`

API route middleware for role-based access control. Consumed by 3 DELETE endpoints (widened from `["lead"]` to `["lead", "dev"]` in commit `68a3966`).

**`requireRole(request, roles)`** — checks the user's role against `user_roles` table via `serviceClient`. Returns `NextResponse` with error if unauthorized, or `null` if allowed:

```ts
const err = await requireRole(request, ["lead"]);
if (err) return err;
```

**Responses:**
| Status | Condition |
|--------|-----------|
| 401 | No authenticated user |
| 403 | User's role not in the allowed list |

**Usage:** `DELETE /api/watchlist/[id]`, `DELETE /api/repo-radar/[id]`, `DELETE /api/logs/[id]` — all require `lead` or `dev` role.

The `user_roles` table is populated automatically by a database trigger on signup (default: `'intern'`). Promotion to `lead` is manual via SQL. See `docs/rls-policies.sql` for the full RLS policy definitions.

---

### `cve-matcher.ts`

Queries the [OSV.dev API](https://osv.dev) for package vulnerabilities. Consumed by `POST /api/watchlist` (auto-check on add) and `POST /api/watchlist/[id]/cve` (manual refresh).

**`checkVulnerabilities(name, ecosystem)`** — sends `{ package: { name, ecosystem } }` to `POST https://api.osv.dev/v1/query`. Returns:

```ts
interface CveResult {
  cves: { id: string; summary: string; severity: string; aliases: string[]; published: string }[];
  highestSeverity: string;  // "LOW" | "MODERATE" | "HIGH" | "CRITICAL"
  totalCount: number;
  summaryText: string;      // e.g. "3 CVEs found (2 high, 1 moderate)"
}
```

**`severityToRiskLevel(severity)`** — maps OSV severity to watchlist risk_level:
- `CRITICAL`/`HIGH` → `"high"`
- `MODERATE` → `"medium"`
- `LOW` → `"low"`

**Error handling:** API failure returns `{ cves: [], highestSeverity: "LOW", totalCount: 0, summaryText: "API error" }`. Name is normalized via `toRegistryName()` from `package-name-map.ts`.

---

### `ecosystem-detector.ts`

Auto-detects the correct package registry ecosystem from a package name. Used by `POST /api/watchlist` when no explicit ecosystem is provided.

**`detectEcosystem(name)`** — returns a registry string:

| Category | Examples | Detected |
|----------|----------|----------|
| JavaScript/TypeScript | `@foo/bar`, any unknown | `"npm"` (default) |
| Python | `django`, `flask`, `fastapi`, `pandas`, `numpy`, `pytorch`, `celery`, `gunicorn`, `uvicorn`, `sqlalchemy`, `requests` | `"PyPI"` |
| Go | `cobra`, `viper`, `gorilla` | `"Go"` |
| Rust | `serde`, `tokio` | `"crates.io"` |
| Java | `spring`, `log4j`, `hibernate` | `"Maven"` |
| .NET | `asp.net`, `entityframework` | `"NuGet"` |
| Ruby | `rails`, `devise` | `"RubyGems"` |
| PHP | `laravel`, `symfony` | `"Packagist"` |

Uses a `KNOWN` lookup table (27 entries). Falls back to `"npm"` for unrecognized names.

---

### `version-fetcher.ts`

Fetches the latest version of a package from its registry. Used by `POST /api/watchlist` (auto-fetch on add) and `POST /api/watchlist/[id]/version` (manual refresh).

**`fetchLatestVersion(name, ecosystem)`** — routes to the correct registry fetcher:

| Ecosystem | Fetch method |
|-----------|-------------|
| npm | `GET https://registry.npmjs.org/{name}/latest` → `.version` |
| PyPI | `GET https://pypi.org/pypi/{name}/json` → `.info.version` |
| Go | `GET https://proxy.golang.org/{name}/@latest` → `.Version` |
| NuGet | `GET https://api.nuget.org/v3-flatcontainer/{name}/index.json` → last stable version |
| crates.io | `GET https://crates.io/api/v1/crates/{name}` → `.crate.max_version` |
| RubyGems | `GET https://rubygems.org/api/v1/gems/{name}.json` → `.version` |

Returns `string | null`. Name is normalized via `toRegistryName()` from `package-name-map.ts`.

---

### `package-name-map.ts`

Maps display names to registry-safe names. Used by `cve-matcher.ts` and `version-fetcher.ts` to ensure correct API lookups.

**`toRegistryName(displayName)`** — translates common display names:

| Display | Registry |
|---------|----------|
| `"Next.js"` | `"next"` |
| `"Tailwind CSS"` | `"tailwindcss"` |
| `"React Native"` | `"react-native"` |
| `"ASP.NET Core"` | `"aspnetcore"` |
| `"shadcn/ui"` | `"shadcn-ui"` |
| `"TanStack Query"` | `"@tanstack/react-query"` |

Contains 22 overrides. Falls back to `key` (lowercased, trimmed input) for unrecognized names.

---

### `markdown.ts`

Writes entries to `docs/feeds/*.md` files in the standard format.

**`appendToFeed(filename, title, url, publishedAt, tags)`**

- Creates the file if it doesn't exist (with header)
- Appends under the correct `## Month Year` header
- Detects and skips duplicates by checking for the exact line
- Auto-trims files at 500 lines (removes oldest entries from the bottom)

**`trimFeedFile(filePath)`** — Internal. Removes oldest entries when a markdown feed exceeds 500 lines.

Used by: `ingesters/rss` (and future auto-ingesters).

---

### `analytics.ts`

Dashboard analytics queries against **Supabase PostgreSQL** (migrated from SQLite in June 2026). All functions are now `async` and use `supabase.from().select()` instead of `db.prepare("SQL").all()`.

**Migration:** Originally synchronous SQLite queries via `getDb()`. Now each function returns a `Promise<number>` or `Promise<Row[]>` from Supabase.

| Function | Returns | SQL (Supabase pattern) | Was (SQLite) |
|----------|---------|------------------------|--------------|
| `getTotalItems()` | `Promise<number>` | `supabase.from("feed_items").select("*", { count: "exact", head: true })` | `COUNT(*) FROM feed_items` |
| `getItemsToday()` | `Promise<number>` | `.gte("fetched_at", todayISO)` with `.lte(...)` date filter | `WHERE date(fetched_at) = date('now')` |
| `getItemsThisWeek()` | `Promise<number>` | `.gte("fetched_at", weekAgoISO)` | `WHERE fetched_at >= datetime('now', '-7 days')` |
| `getItemsBySource()` | `Promise<SourceBreakdown[]>` | `supabase.from("feed_items").select("source, count:source.count()")` | `GROUP BY source ORDER BY count DESC` |
| `getItemsByCategory()` | `Promise<CategoryBreakdown[]>` | `supabase.from("feed_items").select("category, count:category.count()")` | `GROUP BY category ORDER BY count DESC` |
| `getIngestionStatus()` | `Promise<IngestionStatus[]>` | Reads all `ingest:*` keys from `kv_store` via `.in()` filter | Same logic, sync SQLite |
| `getLastGlobalIngestion()` | `Promise<string \| null>` | `.select("value").eq("key", "ingest:last_run:all").single()` | Same logic, sync SQLite |
| `getGlobalIngestionStatus()` | `Promise<string \| null>` | `.select("value").eq("key", "ingest:status:all").single()` | Same logic, sync SQLite (added in `e331bf9`) |

**IngestionStatus fields:** `source`, `lastRun`, `status`, `count`, `elapsedMs`

Consumed by API routes at `app/api/stats/route.ts` (which return JSON for the sidebar and homepage).

Used by:
- `app/api/stats/route.ts` — wraps analytics calls for client consumption
- `components/engineering-intelligence/` — dashboard widgets

---

### `repo-radar.ts`

GitHub REST API client and repo refresh logic for the Repo Radar dashboard. Uses `supabase.from()` for all DB operations (migrated from SQLite `db.prepare()` in June 2026).

**`fetchRepoInfo(owner, repo)`** — Fetches repo metadata from `GET /repos/{owner}/{repo}`. Returns description, language, stars, open issues, pushed_at.

**`fetchLatestRelease(owner, repo)`** — Fetches latest release from `GET /repos/{owner}/{repo}/releases/latest`. Returns tag name, url, body, published_at. Returns `null` if no release exists.

**`fetchRecentPRs(owner, repo, days=7)`** — Fetches recent PRs from `GET /repos/{owner}/{repo}/pulls`. Returns `{ opened, merged }` counts within the time window.

**`fetchRecentIssues(owner, repo, days=7)`** — Fetches recent issues from `GET /repos/{owner}/{repo}/issues`. Returns count of non-PR issues created within the time window.

**`detectBreakingChanges(body)`** — Scans release body for breaking change keywords (`"BREAKING CHANGE"`, `"migration required"`, `"deprecated"`, etc.). Returns the first matching sentence or `null`.

**`detectSecurityAdvisory(body)`** — Scans release body for security keywords (`"CVE"`, `"vulnerability"`, `"security advisory"`, etc.). Returns the first matching sentence or `null`.

**`refreshSingleRepo(item)`** — Full refresh pipeline for one repo: fetches info + release + PRs + issues, detects breaking/security, updates Supabase row via `supabase.from("repo_radar_items").update()`.

**`refreshAll()`** — Iterates all active repos, calls `refreshSingleRepo()` on each, records summary in `kv_store` (via Supabase upsert). Returns `{ updated, errors, results }`.

**Error handling:** All GitHub API calls share `githubFetch()` which uses `GH_ACCESS_TOKEN` env var for authentication (reduces rate limiting). Throws on 403 (rate limit) and non-ok statuses.

Used by:
- `app/api/repo-radar/` — add and refresh endpoints
- `src/ingesters/repo-radar/` — scheduled refresh via orchestrator

---

### `log-parser.ts`

CSV log parser for the Log Analysis Dashboard. Parses Acuity and Zoho CSV exports, classifies rows as errors or successes, groups errors into patterns by normalized message, and detects statistical anomaly spikes.

**Entry point:**

```ts
parseLogCsv(csvText: string, filename: string): AnalysisResult
```

Uses `csv-parse/sync` to parse the CSV. Source detection: `filename.startsWith("acuity")` → Acuity, otherwise Zoho.

**Column detection** — maps CSV header names by regex, picks first match:

| Column Role   | Matches (case-insensitive)                              |
|---------------|--------------------------------------------------------|
| `timestamp`   | `created_at`, `timestamp`, `date`, `datetime`, `time`   |
| `content`     | `content`, `message`, `description`, `status`, `result` |
| `action`      | `action`, `type`, `operation`, `event`                  |
| `method`      | `method`, `function`, `endpoint`, `api`                 |
| `error_code`  | `error_code`, `status_code`, `code`, `error`, `http_status` |
| `response`    | `response`, `result`, `output`, `body`                   |

**Error classification** — a row is an error unless:
- `content` or `error_code` column contains a success indicator (`"success"`, `"ok"`, `"true"`, `"200"`, `"201"`, `"204"`)

**Response parsing** — the `response` column is parsed for error details through a priority chain:
1. `JSON.parse` — extracts `code`, `message`, `error` fields from standard JSON responses (Zoho style)
2. Python dict Format A — `{'status_code': N, 'message': '...', 'error': '...'}` with escaped single quotes
3. Python dict Format B — `{'error': '...'}` single-key dict (sync_session style) with `\n`-embedded content

**Pattern normalization** — error messages are normalized to a `pattern_key` by replacing variable content with `{var}`:

| Pattern | Example |
|---------|---------|
| UUIDs | `a1b2c3d4-...` → `{var}` |
| ISO timestamps | `2024-01-15T10:30:00Z` → `{var}` |
| Emails | `user@example.com` → `{var}` |
| Phone numbers | `+1234567890` → `{var}` |
| Long numbers (6+ digits) | `1234567` → `{var}` |
| Single-quoted strings | `'some value'` → `{var}` |
| Double-quoted strings | `"some value"` → `{var}` |

**Pattern grouping** — normalized error messages are grouped by `pattern_key`. Each group yields:

```ts
{ pattern_key: string;    // normalized key with {var} placeholders
  sample_message: string; // first raw message seen
  count: number;          // occurrence count
  first_seen: string;     // earliest timestamp
  last_seen: string;      // latest timestamp
  severity: string; }     // "high" (>5% of errors), "medium" (>1%), "low"
```

**Anomaly detection** — daily error counts are bucketed by date. Mean and standard deviation are computed. Days where `count > mean + 2σ` are flagged:

- `deviation`: number of σ above mean
- `severity`: `"high"` (>3σ) or `"medium"` (2-3σ)

**Executive summary** — a plain-English summary is generated by categorizing patterns into semantic buckets:

| Category | Example trigger in pattern_key |
|----------|-------------------------------|
| `scheduling conflicts` | `"not an available time slot"` |
| `doctor-calendar mismatches` | `"doctor"` + `"appointment type"` |
| `user lookup failures` | `"email"` + `"not found"` |
| `conflicting appointment states` | `"both completed and cancelled"` |
| `access permission errors` | `"api access is only available on"` |
| `session conflicts` | `"existing session cancelled"` |
| `other errors` | default |

The summary describes the top category (with percentage), secondary category (if ≥5%), anomaly spike days, most affected method, and a recommended investigation direction. If no errors are found, it returns a clean bill of health.

**Interfaces:**

```ts
interface ParsedRow {
  is_error: boolean;
  method: string;
  action: string;
  content: string;
  error_type: string;
  pattern_key: string;
  error_code: string;
  raw_message: string;
  timestamp: string;
}

interface AnalysisResult {
  analysis: {
    total_rows: number;
    error_count: number;
    unique_errors: number;
    time_range_start: string;
    time_range_end: string;
    methods: string;           // JSON string of { method, count }[]
    executive_summary: string;
  };
  errors: ParsedRow[];
  patterns: { pattern_key: string; sample_message: string; count: number;
              first_seen: string; last_seen: string; severity: string }[];
  anomalies: { description: string; severity: string; detected_at: string;
               error_count: number; expected_count: number; deviation: number }[];
}
```

Used by:
- `app/api/logs/route.ts` — `POST /api/logs` upload + parse pipeline
