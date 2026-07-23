# Documentation Hub

Welcome to the centralized documentation directory for the MindYou AI Lounge repository.

This folder contains onboarding guides, architecture references, research materials, dashboard planning documents, AI tooling resources, and project audits.

---

## Recommended Reading Order for New Interns

1. **`getting-started/INSTRUCTIONS.md`** — Set up your dev environment (GitHub Copilot, opencode CLI, Gemini CLI).
2. **`../README.md`** (root) — Understand the project purpose and high-level structure.
3. **`../src/README.md`** — Learn the data pipeline flow (config → ingesters → lib → db).
4. **`../src/db/README.md`** — Understand the DB schema (originally SQLite, now Supabase PostgreSQL — 9 tables, columns, indexes, seed data, migration history).
5. **`../src/ingesters/README.md`** — See how feed data is fetched and stored (4 ingesters, orchestrator).
6. **`../src/lib/README.md`** — Review shared utilities (DB writes, markdown append, analytics queries).
7. **`guides/developer-dashboard.md`** — Read the dashboard spec (modules, audiences, requirements).
8. **`feeds/feeds-format-guide.md`** — Learn the markdown feed format for manual entries.
9. **`tooling/agentic-coding-attribute-matrix.md`** — Understand AI coding patterns used in this repo.
10. **`../.github/workflows/`** — Understand how scheduled ingestion runs via 3 GitHub Actions cron workflows.
11. **`post-mvp-roadmap.md`** — See the planned feature roadmap (4 tiers, 16 items).

---

## Getting Started

| Doc | Description |
|-----|-------------|
| `getting-started/INSTRUCTIONS.md` | Full developer onboarding: GitHub Copilot, opencode CLI, Gemini CLI, model selection, troubleshooting |
| `getting-started/AI-installation-tutorial.md` | Quick 10-minute setup for opencode and Gemini CLI tools |

---

## Core Architecture

| Doc | Description |
|-----|-------------|
| `../README.md` (root) | Project overview, high-level structure, scripts |
| `../src/README.md` | Data pipeline overview: config → ingesters → lib → db, directory relationships, quickstart commands, module status |
| `guides/File architecture update.md` | Repository folder reorganization plan and execution guide |
| `guides/RnD_Technical_Tasks_Overview.md` | R&D technical task inventory and planning overview |

---

## Database

| Doc | Description |
|-----|-------------|
| `../src/db/README.md` | Database layer: originally SQLite (`better-sqlite3`, WAL mode, `getDb()` singleton), now **Supabase PostgreSQL** (`@supabase/supabase-js`, `serviceClient.from()`, async queries). 10 tables + `user_feed_states` + `user_roles` (RBAC) with full column/index docs, migration history, seed data, env var setup. Includes `service-client.ts` (bypasses RLS), `server-client.ts` (cookie-based auth), `auth-helpers.ts` (role middleware) |
| `supabase-schema.sql` | PostgreSQL DDL for all 10 tables — run this in Supabase SQL editor before seeding. Source of truth for table definitions. `feed_items` now includes `ai_relevance_score`, `ai_relevance_label`, `ai_relevance_reason`, `ai_tldr` columns; `is_pinned`/`is_read` moved to `user_feed_states` table |
| `rls-policies.sql` | Row Level Security migration: `user_roles` table (`intern`/`lead`/`dev`), auto-assign trigger on signup, `is_lead()` + `is_lead_or_dev()` helpers, Auth Hook domain gate (`before_user_created`), RLS policies for all 10 tables + `user_feed_states`. Run after `supabase-schema.sql` |

---

## Feed Ingestion

| Doc | Description |
|-----|-------------|
| `../src/ingesters/README.md` | Full ingestion pipeline: architecture diagram (3 GH Actions cron + orchestrator), 4 ingesters (manual-feeds, rss, hacker-news, github-trending) with format/source/commands, RSS feed URL list, HN Algolia API details, orchestrator + `INGEST_SOURCES` env var + `kv_store` key schema, how to add a new RSS feed |
| `../src/lib/README.md` | Shared utilities: `IngestEntry` interface, `upsertEntry()` dedup logic with relevance scoring + engagement boost, `appendToFeed()` markdown writer with 500-line trim, `scoreRelevance()` watchlist-based relevance scorer, `recalcEngagementForItem()` pin/read-based score boost, `retroactivelyScore()` re-scores existing items when watchlist grows, `checkVulnerabilities()` OSV.dev CVE matcher, `detectEcosystem()` registry auto-detection, `fetchLatestVersion()` 7-registry version fetcher, `toRegistryName()` display-to-registry-name mapping, `cn()` CSS utility |
| `feeds/feeds-format-guide.md` | Standard markdown feed entry format and manual editing rules |

---

## Analytics

| Doc | Description |
|-----|-------------|
| `../src/lib/README.md` (analytics section) | 8 analytics functions (`getTotalItems`, `getItemsToday`, `getItemsThisWeek`, `getItemsBySource`, `getItemsByCategory`, `getIngestionStatus`, `getLastGlobalIngestion`, `getGlobalIngestionStatus`) with query patterns and return types. `getItemsToday`/`getItemsThisWeek` filter by `fetched_at` date range; `getLastGlobalIngestion` reads `ingest:last_run:all` from `kv_store` |

---

## Authentication

| Page | Description |
|------|-------------|
| `/login` (`app/login/`) | Sign in — email/password form + GitHub OAuth, redirect support, glassmorphism card |
| `/signup` (`app/signup/`) | Create account — email/password, domain gate (`@mindyou.com.ph` only), Auth Hook before-signup, client-side validation, redirects to /feed |
| `/auth/callback` (`app/auth/`) | OAuth callback — exchanges code for session via @supabase/ssr |

Auth is powered by Supabase Auth. Pages are exempt from middleware (see `proxy.ts` `PUBLIC_ROUTES`).

## Intern Safe Task Board

| Doc | Description |
|-----|-------------|
| `../app/intern-tasks/README.md` | Full page: category pills, difficulty/sort filters, expandable cards, static config — no DB |
| `../components/intern-tasks/README.md` | InternTaskCard component: expandable sections, category/difficulty color maps |
| `plans/intern-task-board-mvp-plan.md` | MVP plan: 8 phases, 7 components, explicit exclusions (no claiming, no completion, no auth) |

## Dashboard

| Doc | Description |
|-----|-------------|
| `guides/developer-dashboard.md` | Dashboard requirements specification: 3 audiences (lead devs, interns, team), 8 modules with feature descriptions, non-binding stack suggestions |
| `post-mvp-roadmap.md` | Post-MVP feature roadmap: 4 tiers (Automation & Intelligence, Dashboard Enhancements, Quality & Infrastructure, Polish) with 16 feature items |

---

## Watchlist

| Doc | Description |
|-----|-------------|
| `../app/watchlist/README.md` | Full page: sortable table, expanded panel, package search combobox, inline editing, version health, CVE checking |
| `../app/api/watchlist/README.md` | CRUD API: 6 routes (list+create, update+delete, CVE refresh, version fetch, PDF export) |
| `../src/lib/README.md` (cve-matcher, ecosystem-detector, version-fetcher, package-name-map sections) | Vulnerability checker, ecosystem detection, registry version fetchers, name mapping |
| `../src/config/README.md` (package-suggestions section) | ~181 curated package names for the search combobox |
| `../src/db/README.md` (watchlist_items table) | `watchlist_items` table schema: columns, `ecosystem` column, risk levels, seed data (14 items) |
| `rls-policies.sql` | RBAC setup: `user_roles` table, auto-assign trigger, `is_lead()` and `is_lead_or_dev()` helpers, RLS policies for all 10 tables + `user_feed_states` |

---

## Google Chat Automation

| Doc | Description |
|-----|-------------|
| `gchat-automation/gchat-automation-docs/architecture.md` | System architecture: Google Apps Script, Chat webhooks, time-based triggers |
| `gchat-automation/gchat-automation-docs/google-chat-tech-updates-automation.md` | Problem statement, requirements, and solution design for daily tech updates automation |
| `gchat-automation/gchat-automation-docs/deployment-guide.md` | Deployment steps for Google Chat Automation: prerequisites, setup, configuration |
| `gchat-automation/gchat-automation-docs/developer-intelligence-feed-google-api-notes.md` | Feasibility notes applying Google Chat API patterns to the Developer Intelligence Feed |
| `gchat-automation/gchat-automation-docs/progress-reporting-investigation.md` | Investigation into automatic progress report generation from daily updates |

---

## AI Tooling & Prompt Engineering

| Doc | Description |
|-----|-------------|
| `tooling/oh-my-opencode-models.md` | Agent role definitions and model selection guide for opencode (Sisyphus, Architect, etc.) |
| `tooling/reference-prompts.md` | Curated prompts for AI agents in Django/DRF/Celery workflows |
| `tooling/CONTEXT-ENGINEERING.md` | Principles and practices for designing AI-readable documentation and repository context |
| `tooling/WARP.md` | Warp.dev terminal integration and agent workflow guidance |
| `tooling/agentic-coding-attribute-matrix.md` | AI coding patterns and attributes used in the repo |

---

## Research & Analysis

| Doc | Description |
|-----|-------------|
| `research/research.md` | Research opportunities, tools, articles, and best practices for AI development |
| `research/pricing.md` | AI model pricing comparisons and cost analysis |
| `research/deepseek-gemini-claude-comparison.md` | Model capability comparison across DeepSeek, Gemini, and Claude |
| `research/vibe-coding-vs-legacy.md` | Analysis of AI-assisted coding vs traditional development approaches |

---

## Plans

| Doc | Description |
|-----|-------------|
| `plans/README.md` | Department-level problem statements and potential solutions (executive, marketing, sales, finance, HR, engineering, operations, admin, health) |
| `plans/_TEMPLATE.md` | Template for documenting new problems and solutions |
| `plans/intern-task-board-mvp-plan.md` | Intern Safe Task Board MVP plan — read-only task catalog, 8 implementation phases, explicit exclusions, component tree |

---

## Internal Audits & Roadmaps

| Doc | Description |
|-----|-------------|
| `../.github/workflows/` | 3 GitHub Actions cron workflows: `ingest-rss.yml` (12h), `ingest-hn-trending.yml` (4h), `ingest-repo-radar.yml` (6h) |
| `../supabase/functions/before-signup/` | Deno Edge Function for Auth Hook: rejects signups from non-`@mindyou.com.ph` emails before user creation |
| `../.vscode/extensions.json` | Recommends `denoland.vscode-deno` extension for Deno support in `supabase/functions/` |
| `../.vscode/settings.json` | Deno config: enables paths for `supabase/functions`, sets Deno as TS formatter |
| `internal/documentation-audit.md` | Original documentation audit: full inventory of features, pages, API routes, database tables, analytics, widgets, ingestion system, watchlist system, missing documentation |
| `internal/documentation-audit-v2.md` | Re-evaluated audit after creation of `src/*/README.md` files: updated statuses, covered gaps, duplicate recommendations removed |
| `internal/documentation-roadmap.md` | Prioritized documentation production plan: 12 proposed docs grouped by priority, with dependencies and implementation phases |

---

## Where to Add New Documentation

| If you are documenting... | Place it in... |
|---------------------------|----------------|
| **Code modules** (`src/config/`, `src/db/`, `src/ingesters/`, `src/lib/`) | A `README.md` in the same directory (e.g., `src/ingesters/README.md`) |
| **UI pages** (`app/`, `components/`) | `docs/guides/` with a descriptive name (e.g., `docs/guides/feed-page.md`) |
| **API routes** (`app/api/`) | `docs/guides/` with a descriptive name |
| **Features, dashboards, specs** (watchlist, theme, dashboard widgets) | `docs/guides/` (e.g., `docs/guides/<feature-name>.md`) |
| **Deployment or operations** | `docs/` root with a descriptive name (e.g., `docs/deployment.md`) |
| **Google Chat or other automations** | `docs/gchat-automation/gchat-automation-docs/` or a new `docs/<automation-name>/` folder |
| **Department plans** | `docs/plans/<department>/` |
| **Research, comparisons, evaluations** | `docs/research/` (e.g., `docs/research/<topic>.md`) |
| **AI prompts, model guides, context engineering** | `docs/tooling/` (e.g., `docs/tooling/<name>.md`) |
| **Documentation audits and roadmaps** | `docs/internal/` (e.g., `docs/internal/documentation-<topic>.md`) |
| **Developer onboarding, setup guides** | `docs/getting-started/` (e.g., `docs/getting-started/<guide-name>.md`) |

### Rules

1. Add a link to this index when you add a major document.
2. Prefer Markdown (`.md`) format.
3. Keep documentation concise and navigable — prefer linking to source READMEs over duplicating their content.
4. If a document belongs to a code module, put it next to the code (e.g., `src/ingesters/README.md`), not in `docs/`.
