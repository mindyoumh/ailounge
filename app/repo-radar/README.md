# `/repo-radar` — Repo Radar Dashboard

Client-side page (`"use client"`) for tracking GitHub repositories — stars, releases, PRs, issues, breaking changes, and security advisories.

## Route

| Path | File | Type |
|------|------|------|
| `/repo-radar` | `page.tsx` | Client |

## Features

### Card Grid

Each tracked repo displays as a card with:

- **Header**: `owner/repo` with external link icon, description (2-line clamp)
- **Metadata**: language dot + label, star count with gain badge
- **Release**: latest release tag (links to GitHub), breaking change badge (destructive), security advisory badge (amber)
- **Activity**: PRs opened/merged in last 7 days
- **Issues**: open issue count with spike badge when >1.5x previous count
- **Last activity**: time-ago timestamp
- **Notes**: inline-editable notes field (click to edit, Enter/blur to save, Escape to cancel)
- **Delete**: hover-reveal trash button with confirmation dialog (available for `lead` and `dev` roles only)

### Add Repository

- Toggle-able form with `owner/repo` input
- Validates format, checks for duplicates (409 response)
- Fetches GitHub API on add (repo info, latest release, recent PRs, recent issues)
- Detects breaking changes and security advisories from release body
- Shows error messages inline (validation, not found, rate limit, network)

### Refresh All

- Button in header triggers `POST /api/repo-radar/refresh`
- Refreshes all active repos sequentially
- Shows toast with updated/error counts
- Rate limit errors shown as dismissible amber banner

### Loading / Empty / Error States

- **Loading**: 6 skeleton cards with pulse animation (staggered)
- **Empty**: "No repos tracked" message with icon and add button
- **Rate limit**: amber banner with dismiss button
- **Delete confirmation**: modal dialog with backdrop blur

## Data Flow

```
User adds repo → POST /api/repo-radar → GitHub API (info, release, PRs, issues) → Supabase PostgreSQL → UI refresh

User clicks Refresh → POST /api/repo-radar/refresh → refreshAll() → GitHub API per repo → Supabase PostgreSQL → UI refresh

Page load → GET /api/repo-radar → Supabase PostgreSQL → card grid

Edit notes → PATCH /api/repo-radar/[id] → Supabase PostgreSQL → UI update

Delete repo → DELETE /api/repo-radar/[id] → Supabase PostgreSQL → card removed
```

## API Dependencies

| Endpoint | Purpose |
|----------|---------|
| `GET /api/repo-radar` | List all active repos |
| `POST /api/repo-radar` | Add a new repo (fetches GitHub data) |
| `PATCH /api/repo-radar/[id]` | Update notes or is_active |
| `DELETE /api/repo-radar/[id]` | Remove a repo |
| `POST /api/repo-radar/refresh` | Refresh all tracked repos |
