# `app/feed/` — Full Developer Intelligence Feed

Client-side page for browsing, searching, and managing ingested feed items. Rendered at `/feed`.

## Page — `page.tsx`

A **client component** wrapped in `<Suspense>` (needed for `useSearchParams`).

### Filter Bar

7 filter controls synced to URL query params:

| Filter | Type | URL Param | Values |
|--------|------|-----------|--------|
| Search title | `Input` | `q` | Free text |
| Source | `Select` | `source` | `hn`, `rss`, `github_trending` |
| Category | `Select` | `category` | `ai`, `cloud`, `devops`, `django`, `nextjs`, `hn`, `github`, `security`, `rumors`, `general` |
| Status | `Select` | `is_read` | `0` (unread), `1` (read) |
| Pinned | `Select` | `is_pinned` | `1` (pinned), `0` (unpinned) |
| Relevance sort | Button | `sort` | Toggle `relevance` (toggles between outline/default variant) |
| Clear button | Button | — | Resets all filters |

Includes a **Refresh** button and an item count summary.

### Feed Items

Each item card shows:

- Source badge (color-coded: orange=HN, blue=RSS, purple=GitHub, green=manual)
- Category badge
- Relevance badge (when `ai_relevance_label` is set — teal for ≥70, amber for ≥50, blue for <50)
- Published date
- Score
- Title (clickable, opens in new tab)
- Tags (clickable hover effect)
- Summary (2-line clamp)

### Actions (per item)

| Action | Button | API Call |
|--------|--------|----------|
| Pin/Unpin | Pin icon | `PATCH /api/feed/[id]` with `is_pinned` |
| Read/Unread | Check icon | `PATCH /api/feed/[id]` with `is_read` |
| Delete | Trash icon | `DELETE /api/feed/[id]` (with animated fade-out) |

### Add Item Form

Toggle-able form to manually insert a feed item. Fields: Title, URL, Category (select), Tags (comma-separated). Calls `POST /api/feed`.

### "New Since Last Visit" Badge

Tracks `localStorage` timestamp (`feed_last_visited_at`). Shows count of items fetched after last visit.

### Pagination

Page-based navigation (50 items per page):

- **← Prev** / **Next →** buttons at the bottom
- Current page indicator: `Page N / M`
- Pagination resets to page 1 when filters change
- Page state synced to URL query param `page`

### Empty & Error States

- **Empty**: "No items found matching your filters"
- **Error**: Red banner with error message
- **Loading**: Skeleton cards via `CardSkeleton`

### Data Flow

```
FeedContent → fetch(`/api/feed?source=&category=&q=&is_read=&is_pinned=&sort=relevance&limit=50&offset=N`)
           → receives FeedResponse { items, total, limit, offset }
           → replaces items state (page-based, no appending)
```

### Relevance Scoring

Feed items can have `ai_relevance_score` and `ai_relevance_label` fields set by `src/lib/relevance-scorer.ts` at ingestion time. A "Relevant" toggle button (Target icon) in the filter bar switches sort order to `ai_relevance_score DESC`, surfacing items that match the user's stack watchlist at the top.
