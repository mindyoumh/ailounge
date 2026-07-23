# `app/api/feed/` — Feed API Routes

CRUD API for the Developer Intelligence Feed. Two route files.

## List & Create — `route.ts`

### `GET /api/feed`

List feed items with filtering and pagination.

**Query parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `source` | string | — | Filter by source (`manual`, `hn`, `rss`, `github_trending`) |
| `category` | string | — | Filter by category |
| `tag` | string | — | Filter by tag substring (LIKE) |
| `q` | string | — | Search title substring (LIKE) |
| `is_read` | `"0"` / `"1"` | — | Filter read/unread (via `user_feed_states`) |
| `is_pinned` | `"0"` / `"1"` | — | Filter pinned/unpinned (via `user_feed_states`) |
| `sort` | string | — | `"relevance"` — sorts by `ai_relevance_score DESC, published_at DESC` |
| `min_score` | number | — | Minimum `ai_relevance_score` filter |
| `limit` | number | 50 | Max items (capped at 200) |
| `offset` | number | 0 | Pagination offset |

**Response:**

```json
{
  "items": [{ "id": 1, "source": "hn", "title": "...", ... }],
  "total": 142,
  "limit": 50,
  "offset": 0
}
```

Items are ordered by `published_at DESC, fetched_at DESC` by default. When `sort=relevance`, items are ordered by `ai_relevance_score DESC NULLS LAST, published_at DESC`.

Read/pin filtering: global `is_pinned`/`is_read` columns were replaced with per-user state in `user_feed_states` table. When a user is authenticated, read/pin filters query this table. When `is_pinned=1`, only items with entries in `user_feed_states` are returned. When `is_read=0`, items with read states are excluded.

### `POST /api/feed`

Insert a new feed item.

**Request body:**

```json
{
  "title": "string (required)",
  "url": "string (required)",
  "category": "string (default: 'general')",
  "source": "string (default: 'manual')",
  "summary": "string | null",
  "tags": "string | null",
  "score": "number | null"
}
```

**Responses:**

| Status | Body |
|--------|------|
| 201 | `{ "ok": true, "id": 5 }` |
| 400 | `{ "error": "title and url are required" }` |
| 409 | `{ "error": "Duplicate entry (source + url already exists)" }` |

UNIQUE constraint on `(source, url)` prevents duplicates. `published_at` defaults to `NOW()`.

---

## Update & Delete — `[id]/route.ts`

### `PATCH /api/feed/[id]`

Update specific fields on a feed item.

**Request body** (any of):

```json
{
  "title": "string",
  "summary": "string",
  "tags": "string",
  "category": "string",
  "score": "number",
  "is_read": true,
  "is_pinned": false
}
```

`is_read` and `is_pinned` are coerced to 0/1 integers and written to `user_feed_states` for authenticated users, or directly on the feed_items row for anonymous users.

**Responses:**

| Status | Body |
|--------|------|
| 200 | `{ "ok": true, "item": { ...updated item... } }` |
| 400 | `{ "error": "No fields to update" }` |
| 404 | `{ "error": "Item not found" }` |

### `DELETE /api/feed/[id]`

Remove a feed item.

**Responses:**

| Status | Body |
|--------|------|
| 200 | `{ "ok": true }` |
| 404 | `{ "error": "Item not found" }` |
