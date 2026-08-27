# Mind You AI Council and AI Factory

[![RSS](https://github.com/mindyoumh/my-ailounge/actions/workflows/ingest-rss.yml/badge.svg)](https://github.com/mindyoumh/my-ailounge/actions/workflows/ingest-rss.yml)
[![HN + GitHub Trending](https://github.com/mindyoumh/my-ailounge/actions/workflows/ingest-hn-trending.yml/badge.svg)](https://github.com/mindyoumh/my-ailounge/actions/workflows/ingest-hn-trending.yml)
[![Repo Radar](https://github.com/mindyoumh/my-ailounge/actions/workflows/ingest-repo-radar.yml/badge.svg)](https://github.com/mindyoumh/my-ailounge/actions/workflows/ingest-repo-radar.yml)

![Next.js](https://img.shields.io/badge/Next.js-16-black?logo=next.js&logoColor=white)
![React](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=black)
![TypeScript](https://img.shields.io/badge/TypeScript-5-3178C6?logo=typescript&logoColor=white)
![Tailwind CSS](https://img.shields.io/badge/Tailwind_CSS-4-06B6D4?logo=tailwindcss&logoColor=white)
![Supabase](https://img.shields.io/badge/Supabase-PostgreSQL-3ECF8E?logo=supabase&logoColor=white)
[![Last commit](https://img.shields.io/github/last-commit/mindyoumh/my-ailounge)](https://github.com/mindyoumh/my-ailounge/commits/main)

This repository functions as the **Mind You AI Council and AI Factory**, an advanced, AI-powered internal ecosystem engineered to elevate organizational productivity by a factor of 100x. By centralizing strategic AI-driven management and operational support, it reduces manual workload, boosts efficiency, and enables seamless coordination across all technical domains.

---

## Table of Contents

- [Introduction](#introduction)
- [New Team Members - Start Here](#new-team-members---start-here)
- [Developer Intelligence Feed](#developer-intelligence-feed)
  - [Tech Stack](#tech-stack)
  - [Architecture](#architecture)
- [First-Time Setup](#first-time-setup)
- [Available Commands](#available-commands)
- [Automated Ingestion](#automated-ingestion)
- [Authentication](#authentication)
- [End-to-End Workflow](#end-to-end-workflow)
- [Dashboard Features](#dashboard-features)
- [Key Features](#key-features)
- [Project Structure](#project-structure)
- [Tooling](#tooling)
- [Pricing](#pricing)
- [Roles](#roles)
- [Research](#research)
- [Contributing](#contributing)
- [FAQs and Support](#faqs-and-support)
- [Targets for 2026](#targets-for-2026)

---

## Introduction

Welcome to the Mind You AI ecosystem, where you can supercharge your workflows with tailored agent models, advanced tools, and best-in-class productivity software. This repository provides everything needed to implement AI in your organization with structured guidance, clear references, and adaptable technologies.

---

## New Team Members - Start Here

**Welcome to the team!** If you're a new employee or intern, start with our comprehensive onboarding guide:

**→ [Developer Onboarding Guide](./docs/getting-started/INSTRUCTIONS.md)**

This guide walks you through:

- Getting GitHub Copilot through GitHub Education
- Getting Gemini Pro through Google Education
- Setting up VS Code with GitHub Copilot
- Installing and configuring OpenCode CLI
- Installing and configuring Gemini CLI
- Complete verification and testing

**Estimated time:** 2–3 hours (including approval wait times)

---

## Developer Intelligence Feed

The Developer Intelligence Feed is an engineering intelligence dashboard currently being developed within the AI Factory ecosystem. Its purpose is to aggregate and centralize high-signal technical news, discussions, trends, security updates, engineering resources, and curated feed sources into a single searchable interface.

Ingestion sources:

| Source | Script | Automated |
| --- | --- | --- |
| Hacker News | `npm run ingest:hn` | Every 4h |
| GitHub Trending | `npm run ingest:trending` | Every 4h |
| RSS Feeds | `npm run ingest:rss` | Every 12h |
| Repo Radar | part of `npm run ingest` | Every 6h |
| Prompt Library | `npm run ingest:prompts` | Manual |
| Manually Curated Feeds | `npm run ingest:manual` | Manual |

All feed data is normalized and stored in a centralized Supabase PostgreSQL database, allowing the dashboard to provide a unified view of engineering-related information. Database constraints prevent duplicate entries, so ingesters are safe to re-run.

### Tech Stack

| Layer | Technology |
| --- | --- |
| Frontend | Next.js 16, React 19, TypeScript 5, Tailwind CSS 4, Radix UI |
| Backend | Next.js Route Handlers (TypeScript) |
| Database | Supabase PostgreSQL via `@supabase/supabase-js` |
| Charts | Nivo (`@nivo/bar`, `@nivo/pie`) |
| Automation | GitHub Actions scheduled workflows |

Supporting packages include `lucide-react` (icons), `cmdk` (command palette), `sonner` (toasts), `csv-parse` (log parsing), `pdf-lib` (watchlist export), and `tsx` (TypeScript execution for ingesters).

### Architecture

```text
Feed Sources
(Manual, RSS, Hacker News, GitHub Trending, Repo Radar)
        ↓
Feed Ingesters  ←── GitHub Actions (scheduled)
        ↓
Supabase PostgreSQL
        ↓
   ┌────┴────┐
   ↓         ↓
Server     API Layer
Components (/api/feed)
   ↓         ↓
Engineering  Feed
Briefing     Dashboard
```

Server components query Supabase directly at request time. Client components fetch through the API routes.

---

## First-Time Setup

**Prerequisites:** Node.js 22 (matches CI) and a Supabase project.

Clone the repository and install dependencies:

```bash
npm install
```

Create `.env.local` in the project root:

```bash
NEXT_PUBLIC_SUPABASE_URL=https://<project-id>.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=<anon key>
SUPABASE_SERVICE_ROLE_KEY=<service role key>
```

All three are required — the app will not boot without them. Values come from Supabase Dashboard → Settings → API. `.env.local` is gitignored; never commit it. The service role key bypasses row-level security, so treat it as a production credential.

Run `docs/supabase-schema.sql` in the Supabase SQL editor to create the schema, apply `docs/rls-policies.sql` for row-level security, then seed:

```bash
npm run db:migrate
```

Populate the database and start the dashboard:

```bash
npm run ingest
npm run dev
```

| Page | URL |
| --- | --- |
| Engineering Briefing | http://localhost:3000 |
| Feed Dashboard | http://localhost:3000/feed |

> Note: The project uses Supabase PostgreSQL (migrated from SQLite). The legacy `data/` directory is gitignored and no longer used.

---

## Available Commands

### Dashboard

```bash
npm run dev      # Development server
npm run build    # Production build
npm run start    # Production server
```

### Database

```bash
npm run db:migrate    # Seed default rows (watchlist, repo radar, prompts) — schema is applied separately from docs/supabase-schema.sql
```

### Feed Ingestion

```bash
npm run ingest            # All orchestrated ingesters
npm run ingest:hn         # Hacker News only
npm run ingest:rss        # RSS feeds only
npm run ingest:trending   # GitHub Trending only
npm run ingest:manual     # Manual feed markdown (standalone)
npm run ingest:prompts    # Prompt Library
```

`npm run ingest` honours the `INGEST_SOURCES` environment variable — a comma-separated allowlist of `hn`, `github_trending`, `rss`, `repo_radar`. Unset means all four.

---

## Automated Ingestion

Three GitHub Actions workflows keep the database current without manual intervention.

| Workflow | File | Schedule | `INGEST_SOURCES` |
| --- | --- | --- | --- |
| RSS | `.github/workflows/ingest-rss.yml` | Every 12h | `rss` |
| HN + GitHub Trending | `.github/workflows/ingest-hn-trending.yml` | Every 4h | `hn,github_trending` |
| Repo Radar | `.github/workflows/ingest-repo-radar.yml` | Every 6h | `repo_radar` |

All three support `workflow_dispatch` for manual runs. Status is reflected in the badges at the top of this README.

### Required Actions secrets

Set under **Settings → Secrets and variables → Actions** (not the Dependabot tab — secrets stored there are invisible to these workflows):

| Secret | Purpose | Required |
| --- | --- | --- |
| `SUPABASE_URL` | Supabase project URL | Yes |
| `SUPABASE_SERVICE_ROLE_KEY` | RLS-bypassing key for ingestion writes | Yes |
| `GH_ACCESS_TOKEN` | Lifts Repo Radar's GitHub API quota from 60 to 5,000 req/hr | Optional |

The workflows map `SUPABASE_URL` onto the `NEXT_PUBLIC_SUPABASE_URL` environment variable that the code reads — the secret name and the variable name deliberately differ. Missing or empty secrets surface as `Missing env: NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY` at `src/db/service-client.ts`, thrown at import time before any ingestion begins.

Ingestion status per source is written to the `kv_store` table (`ingest:status:*`, `ingest:count:*`, `ingest:last_run:*`) and surfaced by the IngestHealth widget and `/api/ingest/status`.

---

## Authentication

Sign-up is restricted to `@mindyou.com.ph` email addresses, enforced in two places: client-side in `app/signup/page.tsx`, and server-side by the Supabase `before-signup` Auth Hook in `supabase/functions/before-signup/index.ts`. Route protection lives in `proxy.ts` via the `PUBLIC_ROUTES` and `PUBLIC_API_ROUTES` constants. Role checks use `requireRole` from `src/lib/auth-helpers.ts`.

---

## End-to-End Workflow

1. Run `npm run ingest`.
2. The pipeline executes each configured ingester — Hacker News, GitHub Trending, RSS, and Repo Radar.
3. Each ingester fetches new content, scores it for relevance and engagement, and upserts it into Supabase PostgreSQL.
4. Database constraints reject duplicates automatically, so only genuinely new records are inserted.
5. The Engineering Briefing page loads statistics and recent items through Server Components reading Supabase directly.
6. The Feed Dashboard retrieves records through `/api/feed`.
7. Users can search, filter by source or category, browse paginated results, mark items read, pin items, add manual entries, and delete entries.
8. Refreshing the browser reflects newly ingested content without a rebuild or restart.

---

## Dashboard Features

- Engineering Briefing dashboard with ingestion health
- Feed aggregation across four sources (Hacker News, GitHub Trending, RSS, Manual)
- Search, source filtering, category filtering, pagination
- Read/unread tracking and item pinning
- Manual feed creation and deletion
- Repo Radar for tracked repositories
- Watchlist with CVE matching, version tracking, and PDF export
- Prompt library
- Log analysis dashboard with Nivo charts
- Command palette
- Supabase PostgreSQL persistence

---

## Key Features

- **AI Role Optimization**: Empower agents to handle communication triage, infrastructure hygiene, and knowledge management efficiently.
- **Reduced Overhead**: Streamline delivery management and system planning.
- **Research-Driven Choices**: Robust market analysis to identify cost-efficient, high-performance AI models.

---

## Project Structure

| Path | Contents |
| --- | --- |
| `app/` | Next.js pages, dashboard UI, and API routes |
| `components/` | Reusable React components and UI primitives |
| `src/` | Feed ingesters, database layer, scoring, and backend utilities |
| `supabase/` | Edge functions and Supabase configuration |
| `.github/workflows/` | Scheduled ingestion workflows |
| `docs/` | Research, planning, feed definitions, and SQL schema |
| `diagrams/` | Architecture diagrams and workflow visualizations |
| `ideas/` | Brainstorming, experiments, and proposals |
| `intern-logs/` | Contributor workspaces and task tracking |
| `data/` | Legacy SQLite storage (gitignored, no longer used) |

Most directories carry their own README. Start with [`docs/README.md`](./docs/README.md) for documentation discovery, and [`AGENTS.md`](./AGENTS.md) for the agent-facing reference covering path aliases, critical constraints, and the navigation map.

---

## Tooling

### Gemini CLI

- Designed for management, planning, summaries, and coordination tasks.
- Features model auto-selection and rate-limit awareness.

### OpenCode CLI

- Engineered for structured agent execution and software development workflows.

### Installed Skills

The team currently uses OpenCode-compatible skills to accelerate engineering work:

- planning-and-task-breakdown
- ui-ux-pro-max
- github-deep-research
- caveman

These skills assist with architecture planning, dashboard design, repository research, and concise technical reasoning.

For more details, see [Tooling Documentation](./docs/research/vibe-coding-vs-legacy.md).

---

## Pricing

Explore detailed pricing analysis models, benchmarks, and strategies in the [Pricing Documentation](./docs/research/pricing.md).

---

## Roles

The AI ecosystem leverages distinct roles that include:

1. Sisyphus (Orchestrator)
2. Oracle (Architect)
3. Librarian (Researcher)

For a complete breakdown, refer to the [Role Definitions](./docs/tooling/oh-my-opencode-models.md).

---

## Research

The ecosystem fosters a research-driven workflow. Access benchmarks, live leaderboards, and plugins via the [Research Resource](./docs/research/research.md).

---

## Contributing

We welcome contributions.

Recommended workflow:

1. Create a feature branch (`feat/description` or `fix/description`).
2. Implement changes.
3. Read the sub-README before editing any module, and update it afterwards — documentation is part of the implementation.
4. Verify ingestion and dashboard functionality.
5. Run `npm run build` — it must pass before commit.
6. Submit a Pull Request and request review from the engineering team.

After schema changes, run `npm run db:migrate` then `npm run build`. After ingester changes, run `npm run ingest` then `npm run build`.

For additional discussions and proposals, see [`ideas/to-discuss.md`](./ideas/to-discuss.md).

---

## FAQs and Support

Frequently asked questions and troubleshooting guidelines are available in [`docs/tooling/WARP.md`](./docs/tooling/WARP.md).

---

## Targets for 2026

### Engineering Goals

- Automate repetitive engineering workflows.
- Expand feed coverage.
- Improve developer intelligence capabilities.
- Add automated testing and linting (neither is configured yet).

### Quality Improvements

- Enhance architecture reviews.
- Improve dashboard usability.
- Strengthen feed quality and relevance.

For complete targets, refer to [`docs/tooling/reference-prompts.md`](./docs/tooling/reference-prompts.md).

---

This repository evolves continuously through the contributions of the engineering team, interns, and AI-assisted development workflows.
