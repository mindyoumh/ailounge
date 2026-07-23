# `src/ingesters/` — Feed Ingestion Pipeline

Fetches, parses, and stores feed items from multiple sources. Every ingester writes to **both** Supabase PostgreSQL (via `lib/db.upsertEntry`) and markdown (via `lib/markdown.appendToFeed`).

## Architecture

```
  ┌─────────────────────┐  ┌──────────────────────┐  ┌─────────────────────┐
  │  GitHub Actions Cron│  │  GitHub Actions Cron │  │  GitHub Actions Cron│
  │  ingest-rss.yml     │  │  ingest-hn-trending  │  │  ingest-repo-radar  │
  │  every 12h          │  │  every 4h            │  │  every 6h           │
  │  INGEST_SOURCES=rss │  │  INGEST_SOURCES=hn,  │  │  INGEST_SOURCES=    │
  │                     │  │   github_trending    │  │   repo_radar        │
  └─────────┬───────────┘  └──────────┬───────────┘  └──────────┬──────────┘
            │                        │                         │
            └────────────────────────┼─────────────────────────┘
                                     ▼
                    ┌─────────────────────┐
                    │   run-all.ts         │
                    │  (orchestrator)      │
                    │  respects            │
                    │  INGEST_SOURCES env  │
                    └──────┬──────────┬────┘
                           │          │
                    ┌──────┤          ├──────┐
                    ▼      ▼          ▼      ▼
                  rss/  hacker-news  github-trending   manual-feeds
                 (DONE)   (DONE)       (DONE)       (standalone only)
                   │        │            │
                   └────────┴────────────┘
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
              lib/db.ts           lib/markdown.ts
           (Supabase upsert)      (append to .md)
```

> `manual-feeds/` is **not** part of the `run-all` orchestrator. Run it separately via `npm run ingest:manual`.

## Ingesters

### `rss/` ✅ Done

Fetches 20 RSS/Atom feed URLs, parses with a regex-based parser (no external deps), writes to Supabase + markdown.

**Source:** `'rss'`

**Feed config:** `rss/feeds.ts` — 20 feeds across 9 categories:
- AI News (OpenAI, Anthropic, Google AI)
- Next.js News (Next.js, Vercel)
- Django News (Django blog, Python blog)
- Security (GitHub Security Advisories, CVE feed)
- Cloud News (AWS, GCP)
- WordPress (WordPress news, developer blog, WooCommerce)
- Docker (Docker blog)
- DevOps (DevOps.com, The New Stack)
- GitHub (GitHub blog, GitHub Engineering)

**Date filter:** Only writes to markdown if `published_at >= 2026-01-01`. Always writes to Supabase.

```bash
npm run ingest:rss
```

### `hacker-news/` ✅ Done (`'hn'`)

Fetches top 20 stories from the [HN Algolia API](https://hn.algolia.com/api) (`search_by_date?tags=story`).

**Source:** `'hn'`

**How it works:**
- Calls `https://hn.algolia.com/api/v1/search_by_date?tags=story&hitsPerPage=20`
- Strips `Ask HN:`, `Show HN:`, `Tell HN:` prefixes from titles
- Falls back to `https://news.ycombinator.com/item?id={objectID}` when no URL
- Writes both to `05-hacker-news.md` and Supabase with score (points)

```bash
npm run ingest:hn
```

### `github-trending/` ✅ Done (`'github_trending'`)

Fetches GitHub Trending repos directly via RSS, no longer reads intermediate markdown files.

**Source:** `'github_trending'`

**How it works:**
- Fetches 3 RSS feeds from `mshibanami.github.io/GitHubTrendingRSS` (daily, weekly, monthly)
- Parses with a regex-based RSS parser (same pattern as the RSS ingester)
- Deduplicates results across the 3 feeds
- Category set to `github` for all entries
- Writes both to `04-github-trending.md` and Supabase (entries >= 2026-01-01 to markdown)

**Note:** The legacy Python scraper (`src/scraper.py`) still exists but is separate — this ingester no longer depends on it. The old `ideas/trending.md` file has been removed.

```bash
npm run ingest:trending
```

### `manual-feeds/` ✅ Done (`'manual'`, standalone)

Parses `docs/feeds/*.md` files and upserts entries into Supabase.

**Format parsed:** `- [Title](URL) | YYYY-MM-DD | tag1, tag2`

**Source:** `'manual'`

**File mapping:**
| File | Category |
|------|----------|
| `01-ai-news.md` | `ai` |
| `02-cloud-news.md` | `cloud` |
| `03-django-news.md` | `django` |
| `04-github-trending.md` | `github` |
| `05-hacker-news.md` | `hn` |
| `06-nextjs-news.md` | `nextjs` |
| `07-rumors.md` | `rumors` |
| `08-security-alerts.md` | `security` |
| `09-devops-news.md` | `devops` |
| `10-github-news.md` | `github` |

```bash
npm run ingest:manual
```

### `prompts/` ✅ Done (`'prompts'`, standalone)

Fetches and ingests prompt data from 3 sources. Standalone (not in orchestrator). Run via `npm run ingest:prompts`.

**Source:** `'prompts'`

**How it works:**

3 sub-functions chained in `main()`:

1. **`ingestCuratedExtras()`** — Inserts 14 curated engineering prompts (architecture, code review, debugging, incident analysis, etc.) with structured fields (input_fields, output_description, model_recommendation). Uses `INSERT OR IGNORE` with hash-based `external_id` for dedup.

2. **`ingestUiDesignPrompts()`** — Inserts 40 UI/UX design prompts from sources like GenDesigns, TypeUI, and iToolVerse. Re-deletes and re-inserts on each run (no dedup needed). Each includes `source_url` pointing back to the original prompt source.

3. **`ingestCommunityPrompts()`** — Fetches up to 200 entries from the [open-prompt-library](https://github.com/BELYAGOUBIABDELILAH/open-prompt-library) GitHub repo's `data/prompts.json`. Maps folder names → internal categories via `FOLDER_CATEGORY_MAP`. Deduplicates by content hash. Uses `sha256` content hashing for external_id.

```bash
npm run ingest:prompts
```

### `repo-radar/` ✅ Done (`'repo_radar'`, in orchestrator)

Refreshes all tracked repos by fetching the latest GitHub API data. Uses `GH_ACCESS_TOKEN` env var for authenticated requests (reduces rate limiting).

**Source:** `'repo_radar'`

**How it works:**
- Reads all active repos from `repo_radar_items` where `is_active = 1`
- Calls `refreshSingleRepo()` on each (from `src/lib/repo-radar.ts`)
- Records status in `kv_store` (`ingest:last_run:repo_radar`, `ingest:status:repo_radar`, `ingest:count:repo_radar`)

**Note:** This ingester refreshes existing repos — new repos are added via the `POST /api/repo-radar` endpoint.

```bash
# Run as part of orchestrator
npm run ingest

# Or standalone
npx tsx src/ingesters/repo-radar/index.ts
```

## Orchestrator

### `run-all.ts`

Runs 4 ingesters sequentially (hn, github_trending, rss, repo_radar). The prompts ingester is standalone — not included here because it manages its own data (prompts table) separate from the feed.

**Env var support:** `INGEST_SOURCES` controls which ingesters run. If set (e.g. `INGEST_SOURCES=hn,github_trending`), only those sources execute. Used by GitHub Actions cron workflows for per-source scheduling. When unset, all 4 ingesters run.

**Engagement scoring:** Only recalculated when all sources run (no `INGEST_SOURCES` filter). Per-source cron runs skip engagement recalculation.

Tracks status in `kv_store`:

| Key | Value |
|-----|-------|
| `ingest:status:*` | `'ok'` or `'error'` |
| `ingest:last_run:*` | ISO 8601 timestamp |
| `ingest:count:*` | Number of new items inserted |
| `ingest:elapsed_ms:*` | Execution time in ms |
| `ingest:status:all` | `'ok'` or `'degraded'` |

Prints a per-source summary table after execution.

**Exports `runAll()`** — accepts `{ closeDb?: boolean }` option. Called by `POST /api/ingest` for on-demand ingestion (legacy — deprecated in favor of GH Actions cron).

### GitHub Actions Cron (replaces browser IngestButton)

The old `IngestButton` component and `ingest.yml` workflow were removed. Ingestion now runs on 3 scheduled GitHub Actions workflows:

| Workflow | Schedule | `INGEST_SOURCES` |
|----------|----------|------------------|
| `.github/workflows/ingest-rss.yml` | Every 12h (`0 */12 * * *`) | `rss` |
| `.github/workflows/ingest-hn-trending.yml` | Every 4h (`0 */4 * * *`) | `hn,github_trending` |
| `.github/workflows/ingest-repo-radar.yml` | Every 6h (`0 */6 * * *`) | `repo_radar` |

Each workflow:
1. Checks out repo
2. Installs deps (`npm ci`)
3. Runs `npx tsx src/ingesters/run-all.ts` with the `INGEST_SOURCES` env var
4. Requires `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, and `GH_ACCESS_TOKEN` secrets

```bash
npm run ingest
```

## Adding a New RSS Feed Source

1. Add the URL to `rss/feeds.ts` with the correct `category` and `feedFile`
2. If the `category` doesn't exist yet, add it to the `CATEGORIES` array in `app/feed/page.tsx`
3. The RSS ingester will pick it up automatically on next run

## Adding a New Ingester Type

1. Create `src/ingesters/<name>/index.ts` exporting an async function
2. If part of orchestrator: import and add it to `runAll()` — insert a `runTracked(...)` call and add to summary loop
3. If standalone: add an npm script in `package.json` (e.g. `"ingest:prompts": "npx tsx src/ingesters/prompts/index.ts"`)
4. For standalone ingesters, call `migrate()` at the start of `main()` to ensure schema is up-to-date (see prompts ingester for example pattern)
