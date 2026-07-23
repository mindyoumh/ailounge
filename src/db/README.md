# `src/db/` — Database Layer

**Migration note:** Originally built with local SQLite (`better-sqlite3`, WAL mode, file at `data/dashboard.db`). Migrated to **Supabase PostgreSQL** in June 2026. The table schemas below are kept for reference; the actual DDL now lives in `docs/supabase-schema.sql` (run in Supabase SQL editor). Seed data is inserted via `npm run db:migrate`.

## Files

### `supabase-client.ts`

Creates and exports a singleton Supabase client using `@supabase/supabase-js`. Client initialized once from environment variables.

```ts
import { supabase } from "@/src/db/supabase-client";

const { data, error } = await supabase
  .from("feed_items")
  .select("*")
  .limit(10);
```

Requires `.env.local` with:
- `NEXT_PUBLIC_SUPABASE_URL` — your Supabase project URL
- `NEXT_PUBLIC_SUPABASE_ANON_KEY` — your Supabase anon/public key

### `browser-client.ts`

Creates a Supabase client for browser (client-side) usage using `@supabase/ssr` `createBrowserClient`. Used by login, signup, and the AuthProvider:

```ts
import { getBrowserSupabase } from "@/src/db/browser-client";

const supabase = getBrowserSupabase();
const { data, error } = await supabase.auth.signInWithPassword({ email, password });
```

### `service-client.ts`

Creates a **service role** Supabase client using `@supabase/supabase-js` with the `SUPABASE_SERVICE_ROLE_KEY`. Bypasses all Row Level Security (RLS) — used by all API routes and lib files for server-side database operations:

```ts
import { serviceClient } from "@/src/db/service-client";

const { data, error } = await serviceClient
  .from("feed_items")
  .select("*")
  .limit(10);
```

Requires `.env.local` with:
- `NEXT_PUBLIC_SUPABASE_URL` — your Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY` — your Supabase service role key (Settings → API → service_role key)

This is the **primary DB client** for route handlers, ingesters, and library code. The anon-key `supabase-client.ts` is legacy and only kept for backward compatibility.

### `server-client.ts`

Provides two factory functions for **cookie-based auth** in server environments, using `@supabase/ssr` `createServerClient`:

**`getServerClient(request)`** — for route handlers. Creates a Supabase client from the incoming `NextRequest` cookies. Returns `{ client, response }` — the `response` object must be returned from the handler to persist cookie changes:

```ts
import { getServerClient } from "@/src/db/server-client";

export async function GET(request: NextRequest) {
  const { client, response } = getServerClient(request);
  const { data: { user } } = await client.auth.getUser();
  return response; // must return response to persist cookies
}
```

**`getServerComponentClient()`** — for Server Components. Uses `next/headers` `cookies()` to read the cookie store:

```ts
import { getServerComponentClient } from "@/src/db/server-client";

const supabase = await getServerComponentClient();
const { data: { user } } = await supabase.auth.getUser();
```

### `client.ts`

Convenience re-export. Now re-exports `serviceClient` from `service-client.ts` (was `supabase` from `supabase-client.ts`). Kept as the same import path so consuming code didn't need import changes:

```ts
import { serviceClient, getDb } from "@/src/db/client";
// getDb() returns serviceClient
// closeDb() is a no-op with Supabase
```

### `schema.ts`

Previously defined all 10 table schemas + indexes + seed data (~421 lines). Now drastically simplified (~85 lines):

- **Table DDL** moved to `docs/supabase-schema.sql` — run manually in Supabase SQL editor once
- `migrate()` only handles **seed data** (initial rows for `watchlist_items`, `repo_radar_items`, `prompts`)
- All inserts use `serviceClient.from().insert()` (bypasses RLS via service role)

#### Tables (historical reference)

Same 10 tables + `user_roles` (RBAC), now served by PostgreSQL via Supabase. Column types below are the original SQLite types; PostgreSQL equivalents are in `docs/supabase-schema.sql`.

**`feed_items`** — Core ingestion store for the Developer Intelligence Feed.

| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER | Primary key, autoincrement |
| `source` | TEXT | `'manual'`, `'hn'`, `'rss'`, `'github_trending'` |
| `category` | TEXT | `'ai'`, `'nextjs'`, `'django'`, `'security'`, etc. |
| `external_id` | TEXT | Source's own ID for idempotent upserts |
| `title` | TEXT | Article/repo title |
| `url` | TEXT | Full URL |
| `summary` | TEXT | Short description |
| `tags` | TEXT | Comma-separated tags |
| `score` | INTEGER | HN points, stars, relevance rank |
| `published_at` | TEXT | ISO 8601 |
| `fetched_at` | TEXT | ISO 8601, defaults to `NOW()` |
| `ai_relevance_score` | INTEGER | Watchlist-based relevance score (0-100) |
| `ai_relevance_label` | TEXT | Matched watchlist item name |
| `ai_relevance_reason` | TEXT | Explanation of why it matched |
| `relevance_base` | INTEGER | Base score before engagement boosts |
| `ai_tldr` | TEXT | Auto-generated summary |
| ~~`is_pinned`~~ | ~~INTEGER~~ | ~~Moved to `user_feed_states`~~ |
| ~~`is_read`~~ | ~~INTEGER~~ | ~~Moved to `user_feed_states`~~ |

**Indexes:** `category`, `source`, `published_at`

**Unique constraint:** `(source, url)` prevents duplicates.

---

**`kv_store`** — Simple key-value storage for ingestion status tracking.

| Column | Type |
|--------|------|
| `key` | TEXT | Primary key |
| `value` | TEXT | Stored value |

Used by `run-all.ts` and `analytics.ts` to track per-source ingestion status (`ingest:last_run:*`, `ingest:status:*`, `ingest:count:*`, `ingest:elapsed_ms:*`).

---

**`watchlist_items`** — Stack Watchlist (Module 3).

| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER | Primary key, autoincrement |
| `name` | TEXT | Unique, e.g. `'Next.js'` |
| `category` | TEXT | `'framework'`, `'database'`, `'infra'`, `'cloud'`, `'ai-sdk'` |
| `installed_version` | TEXT | Currently deployed version |
| `latest_version` | TEXT | Latest available version |
| `ecosystem` | TEXT | Package registry: `'npm'`, `'PyPI'`, `'Go'`, `'crates.io'`, `'Maven'`, `'NuGet'`, `'RubyGems'`, `'Packagist'` (default `'npm'`) |
| `risk_level` | TEXT | `'low'`, `'medium'`, `'high'` (default `'low'`) |
| `upgrade_notes` | TEXT | Migration notes |
| `known_vulns` | TEXT | Known CVEs |
| `migration_link` | TEXT | Link to upgrade guide |
| `updated_at` | TEXT | ISO 8601, defaults to `NOW()` |

**Seed data:** 14 items on first migration (Next.js, React, Django, DRF, PostgreSQL, Redis, Docker, AWS, Celery, GitHub Actions, Sentry, OpenAI SDK, Anthropic SDK, DeepSeek SDK).

---

**`log_analyses`** — Per-upload metadata for the Log Analysis Dashboard.

| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER | Primary key, autoincrement |
| `filename` | TEXT | Uploaded CSV filename |
| `source` | TEXT | `'acuity'` or `'zoho'` |
| `uploaded_at` | TEXT | ISO 8601, defaults to `NOW()` |
| `total_rows` | INTEGER | Total parsed CSV rows |
| `error_count` | INTEGER | Rows classified as errors |
| `unique_errors` | INTEGER | Distinct error patterns |
| `time_range_start` | TEXT | Earliest timestamp in CSV |
| `time_range_end` | TEXT | Latest timestamp in CSV |
| `methods` | TEXT | JSON array of `{ method, count }` |
| `executive_summary` | TEXT | Human-readable analysis summary |

---

**`log_errors`** — Individual parsed rows from uploaded CSV.

| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER | Primary key, autoincrement |
| `analysis_id` | INTEGER | FK → `log_analyses(id) ON DELETE CASCADE` |
| `source` | TEXT | `'acuity'` or `'zoho'` |
| `method` | TEXT | Detected method/endpoint column |
| `action` | TEXT | Detected action/type column |
| `content` | TEXT | Detected content/message column |
| `pattern_key` | TEXT | Normalized error pattern (variables replaced with `{var}`) — populated by parser, used for drill-down matching |
| `error_type` | TEXT | Extracted error type/summary (max 500 chars) |
| `error_code` | TEXT | Detected status/error code column |
| `raw_message` | TEXT | Full raw error message (max 1000 chars) |
| `timestamp` | TEXT | Detected timestamp column |
| `is_error` | INTEGER | `1` for error rows, `0` for success rows |

**Index:** `analysis_id`

---

**`log_patterns`** — Grouped error patterns aggregated at parse time.

| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER | Primary key, autoincrement |
| `analysis_id` | INTEGER | FK → `log_analyses(id) ON DELETE CASCADE` |
| `source` | TEXT | `'acuity'` or `'zoho'` |
| `pattern_key` | TEXT | Normalized error type (variables replaced with `{var}`) |
| `sample_message` | TEXT | First example message (max 500 chars) |
| `count` | INTEGER | How many times this pattern appeared |
| `first_seen` | TEXT | Earliest occurrence |
| `last_seen` | TEXT | Latest occurrence |
| `severity` | TEXT | `'high'`, `'medium'`, or `'low'` |

**Index:** `analysis_id`

---

**`log_anomalies`** — Statistical anomaly spikes detected at parse time.

| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER | Primary key, autoincrement |
| `analysis_id` | INTEGER | FK → `log_analyses(id) ON DELETE CASCADE` |
| `source` | TEXT | `'acuity'` or `'zoho'` |
| `description` | TEXT | Human-readable spike description |
| `severity` | TEXT | `'high'` (>3σ) or `'medium'` (>2σ) |
| `detected_at` | TEXT | Date of the spike |
| `error_count` | INTEGER | Actual error count on that day |
| `expected_count` | REAL | Mean daily error count for baseline |
| `deviation` | REAL | Number of standard deviations above mean |

**Index:** `analysis_id`

---

**`repo_radar_items`** — Repo Radar (Module 2): tracked GitHub repositories.

| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER | Primary key, autoincrement |
| `owner` | TEXT | GitHub owner/org |
| `repo` | TEXT | Repository name |
| `full_name` | TEXT | `owner/repo`, unique |
| `description` | TEXT | GitHub repo description |
| `url` | TEXT | `https://github.com/{full_name}` |
| `language` | TEXT | Primary language (e.g. TypeScript, Python) |
| `stars` | INTEGER | Current star count |
| `stars_gained` | INTEGER | Stars since last refresh |
| `latest_release` | TEXT | Latest release tag name |
| `latest_release_url` | TEXT | URL to the release on GitHub |
| `latest_release_date` | TEXT | ISO 8601 release date |
| `latest_release_body` | TEXT | Full release body text |
| `breaking_changes` | TEXT | Detected breaking change description |
| `security_advisory` | TEXT | Detected security advisory description |
| `open_issues` | INTEGER | Total open issues |
| `open_prs` | INTEGER | Total open PRs |
| `prs_opened_7d` | INTEGER | PRs opened in last 7 days |
| `prs_merged_7d` | INTEGER | PRs merged in last 7 days |
| `issues_opened_7d` | INTEGER | Issues opened in last 7 days |
| `issue_spike` | INTEGER | `1` if issues >1.5x previous count |
| `last_activity_at` | TEXT | GitHub `pushed_at` timestamp |
| `notes` | TEXT | User notes |
| `is_active` | INTEGER | `1` active, `0` archived (default 1) |
| `last_refreshed_at` | TEXT | Last GitHub API refresh |
| `created_at` | TEXT | Defaults to `NOW()` |
| `updated_at` | TEXT | Defaults to `NOW()` |

**Seed data:** 14 repos on first migration (Next.js, Django, DRF, Celery, PostgreSQL, LangChain, OpenAI Python SDK, Anthropic SDK, shadcn/ui, Vercel AI SDK, Cal.com, Sentry, Supabase, OpenCode).

---

**`prompts`** — Prompt Library (Module 7): reusable AI/engineering prompts.

| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER | Primary key, autoincrement |
| `title` | TEXT | Prompt title (max 200 chars for community) |
| `content` | TEXT | Full prompt text (max 5000 chars for community) |
| `category` | TEXT | Category code: `code_review`, `debugging`, `architecture`, `incident_analysis`, `refactoring`, `security_audit`, `documentation`, `intern_mentoring`, `stakeholder_emails` |
| `description` | TEXT | Brief use-case description |
| `input_fields` | TEXT | JSON array of expected input field names |
| `output_description` | TEXT | Description of expected output |
| `model_recommendation` | TEXT | Recommended model(s), e.g. `Claude Sonnet 4, GPT-4o` |
| `usage_count` | INTEGER | Auto-incremented on copy (default 0) |
| `is_featured` | INTEGER | `1` if eligible for daily featured rotation |
| `source` | TEXT | `'curated'`, `'community'`, or `'ui_design'` (added via migration) |
| `external_id` | TEXT | SHA-256 hash of content for dedup (unique per source) |
| `source_url` | TEXT | Original source URL (for community/ui_design) |
| `created_at` | TEXT | Defaults to `NOW()` |
| `updated_at` | TEXT | Defaults to `NOW()` |

**Index:** `category`

**Unique constraint:** `(source, external_id)` via partial index `WHERE external_id IS NOT NULL`

**Seed data:** 13 prompts on first migration (1 featured: "Architecture Sanity Check"; 12 non-featured covering code review, security, debugging, documentation, incident analysis, refactoring, intern mentoring, stakeholder emails).

**Migration columns:** `source`, `external_id`, `source_url` are added via `ALTER TABLE` to support the 3-source system (curated, community, ui_design) without recreating the table.

---

**`user_roles`** — Role-based access control (RBAC) for the application.

| Column | Type | Notes |
|--------|------|-------|
| `user_id` | UUID | Primary key, FK → `auth.users(id) ON DELETE CASCADE` |
| `role` | TEXT | `'intern'` or `'lead'` (default `'intern'`, CHECK constraint) |
| `created_at` | TIMESTAMPTZ | Defaults to `NOW()` |

**Auto-assignment:** A `handle_new_user()` trigger (SECURITY DEFINER) inserts `(user_id, 'intern')` on every `auth.users` INSERT — no manual role setup needed for new signups.

**Manually promoting a user to lead:**
```sql
INSERT INTO user_roles (user_id, role) VALUES ('<auth-user-uuid>', 'lead')
  ON CONFLICT (user_id) DO UPDATE SET role = 'lead';
```

**RLS policies** are defined in `docs/rls-policies.sql`. Access model:
- `intern` — read + insert + update on most tables, no delete
- `lead` — full CRUD on all tables
- `anon` — public read on `feed_items`, `kv_store`, `prompts`

### `migrate.ts`

Entry point for seeding. Calls `schema.migrate()` which inserts seed data via Supabase API (no local SQLite file involved).

```bash
npm run db:migrate
# or
npx tsx src/db/migrate.ts
```

> ⚠️ Tables must already exist in Supabase. First run `docs/supabase-schema.sql` in your Supabase SQL editor, then run `npm run db:migrate` to seed.

## Environment Variables

Create `.env.local` in the project root:

```env
NEXT_PUBLIC_SUPABASE_URL=https://your-project.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
```

`NEXT_PUBLIC_*` vars are safe for client-side code. `SUPABASE_SERVICE_ROLE_KEY` is server-only — never expose it to the browser.

## Usage

```ts
import { serviceClient } from "@/src/db/client";

// Read (async)
const { data: items, error } = await serviceClient
  .from("feed_items")
  .select("*")
  .limit(10)
  .order("published_at", { ascending: false });

// Write
const { error } = await serviceClient
  .from("feed_items")
  .insert({ source: "manual", title: "...", url: "..." });

// Upsert
const { error } = await serviceClient
  .from("kv_store")
  .upsert(
    { key: "ingest:last_run:hn", value: new Date().toISOString() },
    { onConflict: "key" }
  );

// Delete
const { error } = await serviceClient
  .from("feed_items")
  .delete()
  .eq("id", 42);
```
