# Engineering Intelligence — Execution Plan

**Date:** 2026-06-22
**Audience:** Developer implementing the 10 new feed sources
**Prerequisite Reading:**

- [`../research/engineering-intelligence-source-validation.md`](../research/engineering-intelligence-source-validation.md) — URL validation & corrections
- [`engineering-intelligence-implementation-roadmap.md`](./engineering-intelligence-implementation-roadmap.md) — phased ordering & complexity

---

## Architecture Summary

Categories are **free-form strings** — no enum, no type union, no badge-color map. Any string stored in `category` during ingestion automatically becomes filterable via the API. The UI-side CATEGORIES array is the canonical list for the filter dropdown and must be kept in sync.

Sources are similar: the `source` field is a free-form string. The UI-side SOURCES array and SOURCE_BADGE map define rendering. All RSS feeds insert with `source: "rss"` (hardcoded in `src/ingesters/rss/index.ts:34`), so new RSS sources need no SOURCE_BADGE entries.

---

## Files to Modify

### 1. `src/ingesters/rss/feeds.ts` — RSS_FEEDS Array

Add 9 entries. Categories: `devops` (4), `github` (4), `security` (1).

| # | URL                                                              | Category | Feed File               |
| - | ---------------------------------------------------------------- | -------- | ----------------------- |
| 1 | `https://wordpress.org/news/feed/`                               | devops   | `09-devops-news.md`     |
| 2 | `https://developer.woocommerce.com/feed/`                        | devops   | `09-devops-news.md`     |
| 3 | `https://www.docker.com/feed/`                                   | devops   | `09-devops-news.md`     |
| 4 | `https://devops.com/feed/`                                       | devops   | `09-devops-news.md`     |
| 5 | `https://github.blog/feed/`                                      | github   | `11-github-news.md`     |
| 6 | `https://mshibanami.github.io/GitHubTrendingRSS/daily/all.xml`   | github   | `04-github-trending.md` |
| 7 | `https://mshibanami.github.io/GitHubTrendingRSS/weekly/all.xml`  | github   | `04-github-trending.md` |
| 8 | `https://mshibanami.github.io/GitHubTrendingRSS/monthly/all.xml` | github   | `04-github-trending.md` |
| 9 | `https://cvefeed.io/rss/` (temporary)                            | security | `08-security-alerts.md` |

**Change type:** Data addition (9 lines of array entries)
**Corrections from original plan:**

- Use `developer.woocommerce.com/feed/` not `woocommerce.com/blog/feed/`
- Use `docker.com/feed/` not `docker.com/blog/feed/`
- Replace "already covered" GitHub trending with 3 mshibanami RSS entries
- WordPress.org news feeds go to `devops` category (or could split to `cms`)

### 2. `docs/feeds/` — 2 New Feed Files

| File                           | Purpose                                            |
| ------------------------------ | -------------------------------------------------- |
| `docs/feeds/09-devops-news.md` | WordPress, WooCommerce, Docker, DevOps.com entries |
| `docs/feeds/11-github-news.md` | GitHub Blog entries                                |

Each file needs a header comment matching the format in existing files (e.g. `# DevOps News`).
**Change type:** 2 new files, ~3 lines each (header only, entries auto-populated by RSS ingester)

### 3. `src/ingesters/manual-feeds/index.ts` — FILE_CATEGORY_MAP

Add 2 entries:

```ts
"09-devops-news.md":   "devops",
"11-github-news.md":   "github",
```

**Change type:** 2 lines added to the map object.

### 4. `app/feed/page.tsx` — CATEGORIES Array (Line 53–63)

Add `"devops"` to the array:

```ts
const CATEGORIES = [
  "ai",
  "cloud",
  "devops", // NEW
  "django",
  "nextjs",
  "hn",
  "github",
  "security",
  "rumors",
  "general",
];
```

**Change type:** 2 lines added to a const array.
**No SOURCES or SOURCE_BADGE changes needed** — all RSS feeds use `source: "rss"`.

### 5. `app/page.tsx` — Dashboard Homepage Sections

The dashboard currently has 4 hardcoded sections:

- AI Changes (`category = 'ai'`)
- Framework Updates (`category IN ('nextjs', 'django')`)
- Trending Repos (`source = 'github_trending'`)
- Security (`category = 'security'`)

**Decision needed:** Should the DevOps category get its own dashboard section?

**If yes**, add a new SectionCard component. Example pattern (lines 339–345):

```tsx
<SectionCard
  title={`DevOps & Infrastructure (${devopsItems.length})`}
  icon={CloudIcon}
  items={devopsItems}
  viewAllHref="/feed?category=devops"
  delay={0}
/>
```

Requires 1 new SQL query + 1 new SectionCard in the grid layout.

**If no**, the data still flows into the BreakdownCard (By Category tab) and the /feed page filter. The homepage just won't feature them individually.

**Recommendation:** Start without homepage sections. Add them only if Sir Bo asks. The data is fully accessible via `/feed?category=devops` and the BreakdownCard categories tab.

### 6. `app/api/feed/route.ts` — No Change Needed

Categories are free-form TEXT in the DB. The API uses dynamic `WHERE category = @category`. Any string works without code changes.

### 7. `src/db/schema.ts` — No Change Needed

`category TEXT NOT NULL DEFAULT 'general'` — no enum constraint. New categories store and query without migration.

### 8. `src/lib/analytics.ts` — No Change Needed

`getItemsByCategory()` uses `GROUP BY category` — new categories appear automatically.

### 9. GitHub Trending Dashboard Section — Potential Impact

The existing "Trending Repos" section on the homepage (`app/page.tsx:237-243`) queries:

```sql
WHERE source = 'github_trending'
```

The new mshibanami RSS feeds insert with `source: "rss"` and `category: "github"`. They will **not** appear in the Trending Repos section unless the query is updated to also include `OR (source = 'rss' AND category = 'github')`.

**Decision needed:** Update the trending repos query to include both sources, or leave the section as-is (showing only the existing github_trending ingester data).

---

## New Feed Files Required

| File                            | Category | Sources Writing To It                          | Auto-Populated?    |
| ------------------------------- | -------- | ---------------------------------------------- | ------------------ |
| `docs/feeds/09-devops-news.md`  | devops   | WordPress, WooCommerce, Docker, DevOps.com RSS | Yes (RSS ingester) |
| `docs/feeds/11-github-news.md`  | github   | GitHub Blog RSS                                | Yes (RSS ingester) |

All files are auto-populated by the RSS ingester's `appendToFeed()` call. No manual entry needed.

---

## Category Changes

| Category  | New?           | Used By                                                     | Purpose                               |
| --------- | -------------- | ----------------------------------------------------------- | ------------------------------------- |
| `devops`  | ✅ New         | WordPress, WooCommerce, Docker, DevOps.com RSS              | Infrastructure & DevOps industry news |
| `github`  | Already exists | GitHub Blog RSS, GitHub Trending RSS (daily/weekly/monthly) | GitHub platform & trending repos      |

---

## UI Impact

### Feed Page (`/feed`)

| Element                  | Impact                                             | Change Required?                        |
| ------------------------ | -------------------------------------------------- | --------------------------------------- |
| Category filter dropdown | `devops` will appear as an option                 | ✅ Add to CATEGORIES array              |
| Source filter dropdown   | No change — all RSS feeds are `source: "rss"`      | ❌ None                                 |
| Source badge colors      | No change — all RSS feeds get the blue "rss" badge | ❌ None                                 |
| Category badges          | Category text renders via `variant="outline"`      | ❌ None (no color mapping per category) |
| Count                    | Items will appear per normal pagination            | ❌ None                                 |
| "New since last visit"   | Works automatically                                | ❌ None                                 |

### Dashboard Homepage (`/`)

| Element                                          | Impact                                                            | Change Required?                                                            |
| ------------------------------------------------ | ----------------------------------------------------------------- | --------------------------------------------------------------------------- |
| SectionCards (AI, Framework, Trending, Security) | No change — queries are hardcoded per existing categories         | ❌ None (unless adding DevOps/Startup sections)                             |
| Trending Repos section                           | New RSS-based GitHub trending items NOT shown here                | ⚠️ Query uses `source = 'github_trending'`; new items have `source = 'rss'` |
| BreakdownCard By Category                        | `devops`/`github` appear automatically once data exists | ❌ None (dynamic query)                                                     |
| AutomationStatus                                 | No change — tracks ingester sources, not categories               | ❌ None                                                                     |

---

## Risks

| Risk                                                                                | Likelihood | Impact | Mitigation                                               |
| ----------------------------------------------------------------------------------- | ---------- | ------ | -------------------------------------------------------- |
| `devops` category not in CATEGORIES array → filter dropdown missing | Medium     | Medium | Explicitly add to the array in step 1                    |
| WordPress feed changes format                                                       | Low        | Low    | Existing parser handles standard RSS/Atom                |
| mshibanami/GitHubTrendingRSS breaks or changes URL                                  | Low        | Medium | Monitor on first ingest; RSSHub is backup                |
| cvefeed.io goes offline before NVD ingester built                                   | Medium     | Low    | Only a temporary source; no functional damage            |
| Feed files grow unbounded                                                           | Low        | Low    | DB dedup works; rotation deferred to future              |
| Docker volume permissions on Windows                                                | Medium     | Low    | `.env` path override for Docker phase                    |
| New RSS sources fail silently                                                       | Low        | Medium | Ingester logs HTTP errors; check `npm run ingest` output |
| Duplicate content from overlapping RSS sources                                      | Low        | Low    | DB UNIQUE(source, url) constraint handles dedup          |

---

## Implementation Order

### Step 1: Create Feed Files (3 min)

Create 2 files in `docs/feeds/`:

- `09-devops-news.md`
- `11-github-news.md`

Each gets a single markdown header line (e.g. `# DevOps News`). The RSS ingester appends entries.

### Step 2: Update FILE_CATEGORY_MAP (1 min)

Edit `src/ingesters/manual-feeds/index.ts` — add 3 entries to `FILE_CATEGORY_MAP`.

### Step 3: Add RSS_FEEDS Entries (5 min)

Edit `src/ingesters/rss/feeds.ts` — add 10 entries to `RSS_FEEDS` array.

**Order within the file:** Group by category following existing patterns. Suggested placement:

- After the cloud block, add a `// DevOps` comment block with 4 entries
- After existing github entry, add 4 entries for GitHub Trending + GitHub Blog
- Add new `// Startup` block at the end

### Step 4: Update CATEGORIES Array (1 min)

Edit `app/feed/page.tsx` — add `"devops"` to the CATEGORIES constant.

### Step 5: Verify (5 min)

Run `npm run ingest` (or `npm run ingest:rss`). Check:

- No HTTP errors in console output
- New feed files populated in `docs/feeds/`
- DB has new rows: `SELECT DISTINCT category FROM feed_items`
- `/feed?category=devops` loads on the UI
- Category filter dropdown has `devops`

### Step 6: Optional — Update Dashboard Sections

If Sir Bo requests, add a `SectionCard` component for the `devops` category on `app/page.tsx`. Add a SQL query matching the existing pattern (lines 221–251).

### Step 7: Optional — Fix Trending Repos Query

If Sir Bo wants the new GitHub Trending RSS items visible on the homepage, update the query at `app/page.tsx:225` to include both sources:

```sql
WHERE (source = 'github_trending' OR (source = 'rss' AND category = 'github'))
```

---

## Source-to-File Mapping (Cheat Sheet)

| Source                | URL                                        | Category | Feed File               | Inserted In                                       | Source in DB |
| --------------------- | ------------------------------------------ | -------- | ----------------------- | ------------------------------------------------- | ------------ |
| WordPress News        | `wordpress.org/news/feed/`                 | devops   | `09-devops-news.md`     | RSS_FEEDS + FILE_CATEGORY_MAP                     | `rss`        |
| WooCommerce Dev       | `developer.woocommerce.com/feed/`          | devops   | `09-devops-news.md`     | RSS_FEEDS + FILE_CATEGORY_MAP                     | `rss`        |
| Docker Blog           | `docker.com/feed/`                         | devops   | `09-devops-news.md`     | RSS_FEEDS + FILE_CATEGORY_MAP                     | `rss`        |
| DevOps.com            | `devops.com/feed/`                         | devops   | `09-devops-news.md`     | RSS_FEEDS + FILE_CATEGORY_MAP                     | `rss`        |
| GitHub Blog           | `github.blog/feed/`                        | github   | `11-github-news.md`     | RSS_FEEDS + FILE_CATEGORY_MAP                     | `rss`        |
| GH Trending (daily)   | `mshibanami.github.io/.../daily/all.xml`   | github   | `04-github-trending.md` | RSS_FEEDS only                                    | `rss`        |
| GH Trending (weekly)  | `mshibanami.github.io/.../weekly/all.xml`  | github   | `04-github-trending.md` | RSS_FEEDS only                                    | `rss`        |
| GH Trending (monthly) | `mshibanami.github.io/.../monthly/all.xml` | github   | `04-github-trending.md` | RSS_FEEDS only                                    | `rss`        |
| CVE feed (temp)       | `cvefeed.io/rss/`                          | security | `08-security-alerts.md` | RSS_FEEDS only (FILE_CATEGORY_MAP already exists) | `rss`        |

**Note:** GitHub Trending RSS entries write to `04-github-trending.md` which already exists in `FILE_CATEGORY_MAP` — no map change needed for those 3. CVE feed writes to `08-security-alerts.md` which also already exists.

---

## Summary of Changes by File

| File                                  | Type of Change                                                 | Lines Changed/Added |
| ------------------------------------- | -------------------------------------------------------------- | ------------------- |
| `docs/feeds/09-devops-news.md`        | **NEW**                                                        | ~1 line             |
| `docs/feeds/11-github-news.md`        | **NEW**                                                        | ~1 line             |
| `src/ingesters/rss/feeds.ts`          | Add 10 RSS_FEEDS entries                                       | +10 lines           |
| `src/ingesters/manual-feeds/index.ts` | Add 3 FILE_CATEGORY_MAP entries                                | +3 lines            |
| `app/feed/page.tsx`                   | Add 1 category to CATEGORIES array                           | +1 line            |
| `app/page.tsx`                        | **Optional:** Add DevOps section + fix Trending query | ~30 lines each      |
| `src/db/schema.ts`                    | No change                                                      | 0                   |
| `app/api/feed/route.ts`               | No change                                                      | 0                   |
| `src/lib/analytics.ts`                | No change                                                      | 0                   |

**Minimum mandatory changes:** 4 files, ~13 lines total (2 new files + edits in 2 source files + 1 UI file).
**Total time (minimum):** ~15 minutes including verification.
