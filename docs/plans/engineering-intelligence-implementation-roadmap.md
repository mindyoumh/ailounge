# Engineering Intelligence — Implementation Roadmap

**Date:** 2026-06-22
**Source Documents:**
- [`../research/engineering-intelligence-source-validation.md`](../research/engineering-intelligence-source-validation.md) — source-by-source validation with corrections

---

## Overview

6 new sources validated. 3 have corrected URLs vs. the original plan. 8 sources fit the existing `RSS_FEEDS` config with zero ingester changes. 1 source (NVD/CVE) requires a new ingester module.

---

## Implementation Order

```
Phase 0   Prerequisites (feed files + config maps)         Low        ~15 min
Phase 1   RSS config additions (all compatible sources)     Low        ~15 min
Phase 2   Docker infrastructure                             Medium     ~30 min
Phase 3   NVD/CVE ingester (new module)                     Medium     ~2-3 hr
Phase 4   Intern tasks + polish                             Low        ~15 min
Phase 5   Bonus sources + feed rotation                     Varies     deferred
```

Dependencies: Phase 0 → Phase 1. All other phases are independent and can be parallelised.

---

## Phase 0: Prerequisites — Feed Files & Config Maps

No code changes. Create 3 feed files and update the manual-feeds mapping table.

### Tasks

| # | Task | Detail | File(s) |
|---|------|--------|---------|
| 0.1 | Create `09-devops-news.md` | Header comment for DevOps/Infra category | `docs/feeds/09-devops-news.md` |
| 0.2 | Create `11-github-news.md` | Header comment for GitHub platform news | `docs/feeds/11-github-news.md` |
| 0.3 | Add 2 entries to `FILE_CATEGORY_MAP` | Wire new feed files to categories | `src/ingesters/manual-feeds/index.ts` |

**FILE_CATEGORY_MAP additions:**
```ts
"09-devops-news.md":   "devops",
"11-github-news.md":   "github",
```

**Complexity:** Low — pure config, no logic.

### Corrections vs. Original Plan

| Original Plan | Validation Correction | Why |
|---------------|---------------------|-----|
| `woocommerce.com/blog/feed/` | `developer.woocommerce.com/feed/` | Storefront is a theme with no content feed; WooCommerce Dev Blog is the correct feed |
| `docker.com/blog/feed/` | `docker.com/feed/` | The actual WordPress RSS path, verified live |
| `cvefeed.io/rss/` listed as trivial | ⚠️ temporary only; official NVD is JSON-only | cvefeed.io is third-party; cvedaily.com is shutting down |

---

## Phase 1: RSS Config Additions

Add 8 RSS entries to `src/ingesters/rss/feeds.ts`. Zero code changes — pure data additions to the `RSS_FEEDS` array.

### Complexity

**Low** — each is a single line in the `RSS_FEEDS` array. The existing `parseRSS()` handler supports both `<item>` and `<entry>` XML elements.

### Sources (Ordered by Priority)

| # | URL | Category | Feed File | Priority | Rationale |
|---|-----|----------|-----------|----------|-----------|
| 1 | `https://wordpress.org/news/feed/` | devops | `09-devops-news.md` | High | Official WP project news (releases, security) |
| 2 | `https://developer.woocommerce.com/feed/` | devops | `09-devops-news.md` | High | WooCommerce engineering blog |
| 3 | `https://www.docker.com/feed/` | devops | `09-devops-news.md` | High | Docker product & engineering updates |
| 4 | `https://devops.com/feed/` | devops | `09-devops-news.md` | High | Broad DevOps industry coverage |
| 5 | `https://github.blog/feed/` | github | `11-github-news.md` | High | GitHub platform announcements |
| 6 | `https://mshibanami.github.io/GitHubTrendingRSS/daily/all.xml` | github | `04-github-trending.md` | High | Automated daily trending (replaces manual) |
| 7 | `https://mshibanami.github.io/GitHubTrendingRSS/weekly/all.xml` | github | `04-github-trending.md` | High | Weekly trending aggregate |
| 8 | `https://mshibanami.github.io/GitHubTrendingRSS/monthly/all.xml` | github | `04-github-trending.md` | Medium | Monthly trending aggregate |
| 9 | `https://cvefeed.io/rss/` (temporary) | security | `08-security-alerts.md` | Medium | Interim CVE feed until NVD ingester is built |

**Priority rationale:** Feeds 1–6 are high-priority because engineering-intelligence gaps were the original driver. Feeds 8–9 add breadth but shift the dashboard's focus toward security.

### Corrected URLs vs. Original Plan

| Plan's URL | Corrected URL | Source |
|------------|--------------|--------|
| `https://woocommerce.com/blog/feed/` | `https://developer.woocommerce.com/feed/` | Validation §2 |
| `https://www.docker.com/blog/feed/` | `https://www.docker.com/feed/` | Validation §5 |
| GitHub trending "already covered" | ❌ Add 3 mshibanami RSS entries | Validation §4 |

### GitHub Trending — Important Correction

The original plan claimed GitHub daily/weekly/monthly trending is "Already covered" by the existing `github-trending` ingester. **This is incorrect.** The existing ingester reads `ideas/trending.md` — a manually maintained markdown file with no automation. Adding the 3 mshibanami/GitHubTrendingRSS entries above converts this from manual to fully automated.

The existing manual ingester can remain as an override — entries from the automated RSS feed and the manual file coexist via SQLite dedup by URL.

---

## Phase 2: Docker Infrastructure

Independent phase. No source dependencies.

### Tasks

| # | Task | Detail | Complexity |
|---|------|--------|------------|
| 2.1 | Create `Dockerfile` | Multi-stage: deps → runner. Node 22 Alpine. `npm run dev` default CMD | Low |
| 2.2 | Create `docker-compose.yml` | Single service, port 3000, volumes for `data/` and `docs/feeds/` | Low |
| 2.3 | Create `.dockerignore` | Exclude `node_modules`, `data/`, `.git` | Low |
| 2.4 | Create `.env` or document Windows volume paths | `//c/Users/...` for Docker on Windows | Low |
| 2.5 | Update `docs/README.md` | Add Docker setup section | Low |

### Complexity

**Low–Medium** — the files themselves are trivially small. The complexity is in Windows path handling for volume mounts and verifying hot-reload works inside the container.

### Risk

- **SQLite concurrency**: Single-container dev is fine. If this becomes a multi-container setup, SQLite file locking may become an issue.
- **Windows Docker Desktop**: Volume paths work differently. Document in setup.

---

## Phase 3: NVD/CVE Ingestion

Requires a new ingester module. The only source that does not fit the existing RSS pipeline.

### Architecture

Follow `src/ingesters/hacker-news/index.ts` — it already demonstrates the JSON-fetch → parse → `upsertEntry()` pattern.

### Tasks

| # | Task | Detail | Complexity |
|---|------|--------|------------|
| 3.1 | Register for NVD API key | `https://nvd.nist.gov/developers/request-an-api-key` | Low |
| 3.2 | Create `src/ingesters/nvd/index.ts` | Fetch from NVD API v2.0, parse JSON, call `upsertEntry()` | Medium |
| 3.3 | Add `NVD_API_KEY` to environment config | Document in `.env.example` and README | Low |
| 3.4 | Register in `run-all.ts` | Add `nvd` to the orchestrator | Low |
| 3.5 | Create `08-security-alerts.md` (if not Phase 0) | Feed file for CVE entries | Low |
| 3.6 | Handle rate limits | NIST recommends max 1 request per 2 hours | Low |
| 3.7 | Remove temporary cvefeed.io RSS entry | After NVD ingester is verified working | Low |

### Complexity

**Medium** — the HN ingester serves as a reference implementation. The main work is understanding the NVD JSON schema and mapping fields to `feed_items` columns.

### API Details

| Attribute | Value |
|-----------|-------|
| Endpoint | `https://services.nvd.nist.gov/rest/json/cves/2.0` |
| Method | GET |
| Params | `pubStartDate`, `pubEndDate` (ISO 8601), `resultsPerPage`, `startIndex` |
| Response | JSON with `vulnerabilities[]` array |
| Auth | API key in `apiKey` query param or `apiKey` header |
| Rate limit | Not published precisely; NIST says "every 2 hours" for bulk polling |

### Decision: Official vs. Third-Party

| Option | Effort | Reliability | Risk |
|--------|--------|-------------|------|
| **A: cvefeed.io RSS (existing pipeline)** | 1 line in RSS_FEEDS | Depends on third-party | cvedaily.com shutting down; cvefeed.io may follow |
| **B: assurestart.co RSS (existing pipeline)** | 1 line in RSS_FEEDS | Depends on third-party | Unknown SLAs |
| **C: Official NVD API v2.0 (new ingester)** | New module (~100 lines) | Official NIST service | Requires API key |

**Recommendation:** Start with Option A (1-line add in Phase 1) for immediate coverage. Schedule Option C in Phase 3 for production quality. Remove Option A when C is verified.

---

## Phase 4: Intern Tasks

### Recommended Task Additions

| # | Task | Difficulty |
|---|------|-----------|
| 4.1 | Run the app in Docker | beginner |
| 4.2 | Add a new RSS source to the ingester | intermediate |
| 4.3 | Audit feed files for stale entries | beginner |
| 4.4 | Build an ingester health widget | intermediate |
| 4.5 | Write an API endpoint test (`GET /api/feed`) | intermediate |
| 4.6 | Optimize Docker image size | advanced |

| 4.8 | Write a data integrity SQL query | beginner |

**Complexity:** Low — pure data entry in `src/config/intern-tasks.ts`. Each is a structured object with `{ title, description, difficulty }`.

### Priority

- 4.1, 4.2, 4.3: High — immediate onboarding value (ties to Phase 0, 1, 2)
- 4.8: High — catches data quality issues early
- 4.4, 4.5, 4.7: Medium — add dashboard value after ingestion is stable
- 4.6: Low — nice to have, after Docker is working

---

## Phase 5: Bonus Sources & Feed Rotation

### GitHub Release Notes Tracking

If specific repo release notes are desired (e.g., `vercel/next.js`, `nodejs/node`), the Atom feed URL pattern is:
```
https://github.com/{owner}/{repo}/releases.atom
```
These fit `RSS_FEEDS` — no new ingester needed. The open question is which repos to track.

**Complexity:** Low per repo. **Recommendation:** Defer until a specific need arises.

### Feed Rotation (Phase 4 from Original Plan)

Original plan's Phase 4 recommended deferring feed rotation until files exceed ~500 lines. No change.

**Status:** Deferred.

---

## Summary: Complexity & Effort

| Phase | Title | Complexity | Est. Time | Dependencies | Can Parallelize |
|-------|-------|-----------|-----------|-------------|----------------|
| 0 | Prerequisites (feed files + maps) | Low | ~15 min | None | — |
| 1 | RSS config additions | Low | ~15 min | Phase 0 | No |
| 2 | Docker infrastructure | Medium | ~30 min | None | With Phases 0, 3, 4 |
| 3 | NVD/CVE ingester | Medium | ~2–3 hr | Phase 0 | With Phases 2, 4 |
| 4 | Intern tasks | Low | ~15 min | None | With Phases 2, 3 |
| 5 | Bonus sources + rotation | Varies | Deferred | Phase 1 | N/A |

**Total active work:** ~3.5–4 hours across Phases 0–4.

---

## Recommended GitHub Issues

| # | Title | Phase | Labels |
|---|-------|-------|--------|
| EI-1 | Create feed files for new categories (devops, github) | 0 | `docs`, `ingestion` |
| EI-2 | Add WordPress, WooCommerce, Docker, DevOps.com RSS feeds | 1 | `ingestion`, `rss` |
| EI-3 | Add GitHub Trending RSS feeds (daily/weekly/monthly) | 1 | `ingestion`, `github` |
| EI-4 | Add GitHub Blog RSS feed | 1 | `ingestion`, `rss` |
| EI-5 | Add temporary CVE feed from cvefeed.io RSS | 1 | `ingestion`, `security` |
| EI-6 | Create Dockerfile and docker-compose for reproducible dev | 2 | `infra`, `docker` |
| EI-7 | Build NVD API v2.0 ingester for official CVE data | 3 | `ingestion`, `security` |
| EI-8 | Add intern tasks covering Docker, feeds, and data integrity | 4 | `docs`, `onboarding` |
| EI-9 | Replace cvefeed.io RSS with official NVD ingester | 3 | `cleanup`, `security` |
---

## Recommended Phase Labels

| Label | Meaning |
|-------|---------|
| `phase-0-prereqs` | Feed files, config maps — zero code risk |
| `phase-1-rss` | RSS_FEEDS additions — pure config, immediate value |
| `phase-2-docker` | Container infra — independent infra work |
| `phase-3-nvd` | New NVD ingester — requires new module + API key |
| `phase-4-intern-tasks` | Onboarding task definitions — docs only |
| `phase-5-bonus` | Deferred items — no timeline |

---

## Key Risks & Mitigations

| Risk | Phase | Likelihood | Impact | Mitigation |
|------|-------|-----------|--------|------------|
| mshibanami/GitHubTrendingRSS repo archived | 1 | Low | Medium | Monitor repo health; RSSHub is a backup |
| cvefeed.io goes offline before NVD ingester built | 1→3 | Medium | Low | Temporary; expedite Phase 3 if it happens |
| NVD API key not obtained | 3 | Low | High | Key is free; register before Phase 3 starts |
| Windows Docker Desktop volume path issues | 2 | Medium | Low | Document `//c/Users/...` pattern; add `.env` override |
| Feed files grow beyond 500 lines | 1 | Low | Low | Deferred rotation; DB is source of truth |

---

## How to Use This Roadmap

1. **Start with Phase 0** — creates the files and mappings that all RSS additions depend on.
2. **Phase 1 is the highest-ROI step** — 8 sources, ~15 minutes, zero code logic changes.
3. **Phase 2 (Docker) and Phase 3 (NVD) can be done in parallel.**
4. **Phase 4 (intern tasks) is independent** — can be done by anyone at any time.
5. **Validation corrections from the research document are reflected in Phase 1** — the 3 corrected URLs should be used in place of the original plan's suggestions.
