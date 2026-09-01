# Developer Dashboard & Tooling — Requirements

A **daily developer-intelligence and ingestion platform** for the lead engineer and fellow
developers. Its job: put ready tools, one-click workflows, and everyday news in one place so a
developer can pick up a task and go — and stay current without hunting across twenty tabs.

This is a **specification for developers**, not an implementation. Nothing here has been built.

> **Build ownership:** developers own all implementation. Pick a section, scope it, build it.
>
> **Suggested stack (non-binding):** Next.js (App Router) + `better-sqlite3` for local SQLite,
> Tailwind for UI. One reusable shell, multiple panels.

> **Out of scope:** business/operational dashboards (e.g. hot-leads, lead funnel, model-cost).
> Those are Mind You *business problems* and live under [`plans/<department>/`](../plans/) — not here.

---

## Overview

A single dashboard that aggregates AI/dev news, framework updates, trending repos, Hacker News,
security alerts, GitHub activity, internal tooling, learning workflows, and safe intern tasks.

Three audiences share one shell:

| Audience | What they get |
|----------|---------------|
| **Lead / senior devs** | Intelligence feed, stack watchlist, repo radar, AI tool tracker, prompt library |
| **Interns** | Safe task board, mock API/data labs, code-review simulator, research summarization |
| **The team** | Tools catalog, one-click workflows, dashboard factory, internal knowledge base |

### Build order (MVP first)

Build only this first — gives real leverage immediately while staying useful for interns:

1. **Engineering Briefing** homepage (Module 5)
2. **Developer Intelligence Feed** (Module 1) — see [build spec](#build-spec-developer-intelligence-feed) below
3. **Stack Watchlist** (Module 3)
4. **Repo Radar** (Module 2)
5. **Prompt Library** (Module 7)
6. **Intern Safe Task Board** (Module 8)

Sections below: **Modules** = the full catalog · **Build Spec** = the detailed first target with
acceptance criteria · **Recommended Additions** = proposed next-wave features.

---

## Modules

### 1. Developer Intelligence Feed

The core ingestion surface. Not generic news — categorized, filterable, de-duplicated feeds.
Detailed build spec: see [Build Spec](#build-spec-developer-intelligence-feed).

| Feed | Purpose |
|------|---------|
| AI News | Models, agents, tooling, pricing, benchmarks |
| Next.js News | Releases, RFCs, breaking changes |
| Python/Django News | Releases, security, ecosystem |
| Hacker News | Engineering discourse |
| GitHub Trending | Repos worth inspecting |
| Security Alerts | CVEs, npm/PyPI advisories |
| Cloud/AWS News | Pricing, outages, new services |
| Leaks / rumors | Clearly marked as unverified |

**Filters:** `AI` · `Next.js` · `Django` · `Security` · `Infra` · `GitHub` · `HN` · `Research` · `Rumors`

### 2. Repo Radar

Track repositories you care about.

**Watched (examples):** Next.js, Django, DRF, Celery, PostgreSQL, LangChain, OpenAI SDK,
Anthropic SDK, shadcn/ui, Vercel AI SDK, Cal.com, Sentry, Supabase, OpenCode.

**Show per repo:** latest releases, breaking changes, issue spikes, PR velocity, stars gained,
maintainer activity, security advisories.

This is your technical radar.

### 3. Stack Watchlist

Personal/company watchlist for your **real** stack: Next.js, Django, DRF, PostgreSQL, Docker,
AWS, Celery, Redis, GitHub Actions, Sentry, OpenAI / Anthropic / DeepSeek.

**Per item:** latest version, current installed version, risk level, upgrade notes,
known vulnerabilities, migration links.

Directly useful operationally.

### 4. AI Model & Tooling Tracker

Track AI tools you use or evaluate: OpenAI, Claude, DeepSeek, Gemini, Grok, Cursor, Claude Code,
Codex, OpenCode, Ollama, local models.

**Columns:** Tool · Model · Price · Context · Coding Quality · API Available · Notes

**Plus:** pricing changes, rate-limit notes, benchmark links, best use case, internal recommendation.

Helps with procurement and dev workflow decisions.

### 5. Engineering Briefing Page (homepage)

Daily generated summary — **Today's Engineering Brief**:

1. Important AI changes
2. Relevant framework updates
3. Trending repos worth checking
4. Security items
5. One recommended tool/repo
6. One intern learning task

This is the homepage.

### 6. Internal Skill Launcher

One-click buttons that leverage AI repeatedly:

- Generate feature plan
- Generate PR review checklist
- Generate test cases
- Generate API contract
- Generate migration plan
- Generate incident report
- Generate deployment checklist
- Generate intern task brief

### 7. Prompt Library

Curated internal prompt bank.

**Categories:** Code review, Debugging, Architecture, Incident analysis, Refactoring,
Security audit, Documentation, Intern mentoring, Stakeholder emails.

**Per prompt:** purpose, input fields, expected output, model recommendation.

### 8. Intern Task Board — Safe Mode (MVP)

**Status:** Planned — see [`docs/archives/2026-07-24/intern-task-board-mvp.md`](../archives/2026-07-24/intern-task-board-mvp.md)

Where interns fit. Instead of production data, give them: synthetic tasks, mock APIs, local SQLite,
generated fixtures, isolated branches, fake logs, fake incidents, code-review exercises.

**MVP scope:** A read-only catalog of tasks at `/intern-tasks`, filterable by category (synthetic-data,
mock-apis, local-db, code-review, docs-research, git-workflow) and difficulty (beginner/intermediate/advanced).
Each task is self-contained with learning objective, safe environment, expected output, and resources.
No completion tracking, no claiming, no user accounts — purely a reference board.

**Examples:**
- Build a dashboard card from fake API data
- Fix a mock N+1 query
- Write tests for a fake booking module
- Review a vulnerable PR
- Summarize today's Next.js news

They learn while producing useful support work.

### 9. Research Inbox

Save articles, repos, papers, videos, docs — prevents "interesting link rot."

**Fields:** Title, URL, Category, Summary, Priority, Saved by, Status (`unread` / `reading` /
`useful` / `discarded`).

### 10. Trend Scoring

Not all trending repos matter. Score items by: relevance to stack, stars gained, maintainer quality,
production usefulness, security risk, learning value, hype level.

**Output buckets:** Worth investigating · Ignore · Intern research task · Potential adoption · Watch only.

---

## Build Spec: Developer Intelligence Feed

> **First build target** — the canonical detailed spec for Module 1. Build this before anything else;
> the Engineering Briefing homepage is a generated view on top of this data.

**Purpose.** Give developers a single daily scan of what is happening in their world — no hunting
across sites. Read-oriented, with light manual curation.

### Sources

All ingested into **one local SQLite store**, tagged by `source`:

| # | Source | How |
|---|--------|-----|
| 1 | **GitHub trending** | Wrap the existing daily trending bot output (commit `5ce9e04 Bot auto-updated daily trends`) — do **not** rebuild the fetcher. Manual add/remove on top. |
| 2 | **AI / tech / webdev news** | Hacker News + AI/webdev/python/javascript feeds (RSS / HN Algolia API). |
| 3 | **Research papers** | arXiv + related research repos, for developer reading. |

### Requirements

- **One CRUD view** — filter by `source` / tag / date, full-text search on titles.
- **Manual curation** — add custom entry, remove noise, pin/star.
- **De-duplication** — unique on `(source, url)`.
- **Read tracking** — mark read/unread; "new since last visit".
- **Scheduled ingestion** — GitHub Action cron (same pattern as the current bot) writes to SQLite, no manual steps.

### Data model _(devs refine)_

```sql
feed_items (
  id           INTEGER PRIMARY KEY,
  source       TEXT NOT NULL,   -- 'github_trending' | 'hn' | 'rss' | 'arxiv' | 'manual'
  external_id  TEXT,            -- source's own id, for idempotent upserts
  title        TEXT NOT NULL,
  url          TEXT NOT NULL,
  summary      TEXT,
  tags         TEXT,            -- comma-separated or JSON array
  score        INTEGER,         -- HN points, stars, relevance rank
  published_at TEXT,            -- ISO 8601
  fetched_at   TEXT,            -- ISO 8601
  is_pinned    INTEGER DEFAULT 0,
  is_read      INTEGER DEFAULT 0,
  UNIQUE (source, url)
)
```

### Acceptance criteria

- [ ] All three sources visible in one filterable view.
- [ ] Manual add / remove / pin persists across reloads.
- [ ] Scheduled ingestion runs without manual steps.
- [ ] No duplicate rows per external item (enforced by `UNIQUE (source, url)`).
- [ ] Full-text title search returns matches under 200 ms on a typical day's volume.

**Notes.** One app, `source` column splits the feeds — not three half-apps. Respect API rate limits/terms.

---

## Recommended Additions

Proposed next-wave features, ranked by leverage for a lead-engineer daily-intelligence workflow.
Each: **what · why · rough effort (S/M/L)**. Build after the MVP six.

### A. Intelligence layer (highest leverage)

| Feature | What | Why | Effort |
|---------|------|-----|--------|
| **AI TL;DR per item** | One-line LLM summary + "why it matters to *our* stack" tag, generated at ingestion. | Turns a 200-item feed into a 30-second scan. The single biggest time-saver for a lead. | M |
| **Relevance scoring vs. our stack** | Auto-score every feed item against the Stack Watchlist; surface only what touches what we run. | Kills noise. A Rust CVE is irrelevant if we ship Django/Next. | M |
| **CVE → repo matcher** | Match incoming Security Alerts against actual `package.json` / `requirements.txt` / `poetry.lock` in our repos; flag "you are affected." | Converts generic advisories into actionable "patch this today." High value, hard to get elsewhere. | L |
| **Dependency drift digest** | Read-only Dependabot-style view: installed vs. latest across repos, with AI-summarized changelog diffs. | Lead needs upgrade pressure visible without opening every repo. Feeds Stack Watchlist Module 3. | L |

### B. Delivery & notifications

| Feature | What | Why | Effort |
|---------|------|-----|--------|
| **Daily digest push** | Engineering Briefing delivered to Slack/email at a set time, not pull-only. | Intelligence you have to remember to check is intelligence you skip. | S |
| **Saved searches → alerts** | Keyword/topic watches across all feeds (e.g. "Next.js RFC", "our-dep CVE") that notify on hit. | Lead can stop scanning and let the platform tap them. | M |
| **Weekly retro digest** | Auto-rollup: top items of the week, what the team saved, what's still unread. | Good for team syncs and onboarding context. | S |

### C. Collaboration (you + fellow devs)

| Feature | What | Why | Effort |
|---------|------|-----|--------|
| **Per-user state + auth** | Read/pin/save state is per developer, not global. | Multi-dev tool needs personal state or it's useless past one user. **Prerequisite for most of section C.** | M |
| **Team annotations** | Comment/tag on any feed item; "@mention" to route an item to a teammate. | Turns passive reading into shared knowledge. Lead routes learning to interns. | M |
| **"Trending in our org"** | Surface what teammates starred/saved most this week. | Crowd-sourced relevance signal beats raw upstream popularity. | S |
| **Shared tag taxonomy** | Org-wide tag vocabulary so saves are findable across people. | Prevents tag sprawl; makes Research Inbox a real knowledge base. | S |

### D. Ingestion breadth & control

| Feature | What | Why | Effort |
|---------|------|-----|--------|
| **OPML / RSS import** | Drop in any feed URL or OPML export; no code change to add a source. | Sources change constantly; hard-coding them rots fast. | S |
| **Changelog / release-note source** | First-class ingestion of GitHub Releases for Repo Radar items, with AI TL;DR. | Releases are the highest-signal source for a stack-watching lead. | M |
| **Webhook inbound** | Accept pushes (GitHub webhooks, CI events) into the feed, not just polling. | Real-time security/release events instead of waiting for the next cron. | M |
| **Noise/dup ML filter** | Cluster near-duplicate stories (same news, five outlets) into one card. | HN + RSS overlap heavily; collapsing saves scan time. | M |

### E. UX & ops

| Feature | What | Why | Effort |
|---------|------|-----|--------|
| **Global command palette** | `Cmd-K` search + jump across feeds, repos, prompts, tools. | A daily-driver tool lives or dies on speed-of-access. | S |
| **PWA / offline + keyboard-first** | Installable, offline reading queue, j/k navigation. | Lead reads on commute/phone; keyboard nav for power use. | M |
| **Ingestion health panel** | Per-source freshness, last-run status, error counts. | When a feed silently dies you want to know before you miss a CVE. | S |
| **Metrics & feedback loop** | Track which items get read/saved → tune relevance scoring over time. | Closes the loop so section A's scoring actually improves. | M |

### Suggested sequencing after MVP

1. **Per-user state + auth** (C) — unblocks collaboration and personal feeds.
2. **AI TL;DR + relevance scoring** (A) — turns volume into signal.
3. **Daily digest push + saved-search alerts** (B) — stop pulling, start getting tapped.
4. **CVE → repo matcher + dependency drift** (A) — the lead-specific killer features.
5. **Collaboration + ingestion breadth** (C, D) — scale to the whole team.
6. **UX/ops polish** (E) — make it the default tab.

---

_Business dashboards are intentionally excluded — see [`plans/`](../plans/) for Mind You business problems._
