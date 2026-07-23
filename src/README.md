# `/src` — Data Pipeline & Core Logic

The data ingestion and storage layer for the Developer Dashboard.

## Pipeline

```
config/        — Seed data (intern tasks)
  ↓
ingesters/    — Fetch/parse feed sources → Supabase PostgreSQL + markdown (migrated from SQLite June 2026)
  ↓
lib/          — Shared utilities (DB write, markdown write, analytics queries)
  ↓
db/           — Supabase client, schema, seed data
```

## Relationship to Other Directories

| Directory | Role |
|-----------|------|
| `app/` | Next.js UI pages + API routes (consumes `src/db/` + `src/lib/` directly) |
| `components/` | Dashboard widgets (consume `src/lib/analytics.ts`) |
| `scripts/` | Standalone utilities (`clean-feed-files.ts`, `_check-db.ts`) |
| `docs/feeds/` | Markdown feed files consumed by `ingesters/manual-feeds/` and written to by all ingesters |

## Quickstart

```bash
# Prerequisites: create .env.local with NEXT_PUBLIC_SUPABASE_URL and
# NEXT_PUBLIC_SUPABASE_ANON_KEY. Run docs/supabase-schema.sql in Supabase SQL editor first.

# Seed data (tables must already exist in Supabase)
npm run db:migrate

# Run all ingesters
npm run ingest

# Run a single ingester
npm run ingest:manual     # docs/feeds/*.md → Supabase PostgreSQL
npm run ingest:rss        # RSS/Atom feeds
npm run ingest:hn         # Hacker News (HN Algolia API)
npm run ingest:trending   # GitHub Trending (RSS feeds)
npm run ingest:prompts   # Prompt Library (curated extras + community + UI design)
```

## Status

| Module | Status |
|--------|--------|
| `db/` | ✅ Done — 10 tables + `user_roles`, migration, seed data |
| `ingesters/manual-feeds/` | ✅ Done — parses markdown → Supabase PostgreSQL (standalone, not in orchestrator) |
| `ingesters/rss/` | ✅ Done — 20 RSS feeds, regex parser |
| `ingesters/hacker-news/` | ✅ Done — HN Algolia API, 20 stories |
| `ingesters/github-trending/` | ✅ Done — fetches RSS directly (3 feeds: daily/weekly/monthly) |
| `ingesters/repo-radar/` | ✅ Done — refreshes tracked repos via GitHub API |
| `ingesters/prompts/` | ✅ Done — 3 sources: curated extras (14), UI design (40), community (up to 200 from GitHub) |
| `ingesters/run-all` | ✅ Done — orchestrator with 4 ingesters, exports `runAll()`, kv_store tracking |
| `lib/` | ✅ Done — DB, markdown, analytics, log-parser, repo-radar utilities |
| `config/` | ✅ Done — intern task seed data |

```bash
# Seed repo-radar + prompts on fresh DB
npm run db:migrate  # seeds 14 repos + 13 prompts
```
