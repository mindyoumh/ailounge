# `components/sidebar/` — Fixed Left Sidebar

Replaces the old top `Navbar` component. Renders a fixed 240px sidebar with branding, navigation, theme toggle, and quick stats.

## Layout

```
┌─────────────────────────────┐
│  Logo + Brand (my-ailounge) │  px-6 py-5, border-b
│  Developer Intelligence     │
├─────────────────────────────┤
│  User Info (avatar, name)   │  px-3, bg-accent/30 card
├─────────────────────────────┤
│  Nav Items (7)              │  px-3 py-4, space-y-1
│  ● Briefing  (/)            │
│  ● Feed      (/feed)        │
│  ● Stack     (/watchlist)   │
│  ● Logs      (/logs)        │
│  ● Tasks     (/intern-tasks)│
│  ● Radar     (/repo-radar)  │
│  ● Prompts   (/prompts)     │
├─────────────────────────────┤
│  Theme Toggle (Light/Dark)  │  px-3 mb-2
│  Log out                    │  px-3 mb-2, hover:text-red
├─────────────────────────────┤
│  Quick Stats (from /api)    │  px-4 py-4, border-t
│  Total Items    42          │
│  Last Ingest    3h ago      │
│  Today          7 items     │
│  This Week      23 items    │
└─────────────────────────────┘
```

## Component: `Sidebar`

**Type:** Client (`"use client"`) — uses `usePathname()` for active nav detection, `useTheme()` for dark/light toggle, and `useUser()` from AuthProvider.

### Dependencies

- `useUser()` from `@/components/auth-provider` — provides `{ user, role, loading, signOut, refreshRole }`
- `useTheme()` from `@/components/theme-provider` — dark/light toggle
- Fetches `GET /api/stats` on mount for Quick Stats

### User Info Section

Shown when `user` is available and `loading` is false:

- Renders user avatar (GitHub avatar_url or fallback `User` icon in gradient circle)
- Displays full_name / user_name / email prefix
- Wrapped in `bg-accent/30` rounded card

### Nav Items

7 items defined in `NAV_ITEMS` constant (exported — consumed by `CommandPalette` in `components/command-palette.tsx`):

| Label | Icon | Path |
|-------|------|------|
| Briefing | `LayoutDashboard` | `/` |
| Feed | `Rss` | `/feed` |
| Stack | `Layers` | `/watchlist` |
| Logs | `ScrollText` | `/logs` |
| Tasks | `BookOpen` | `/intern-tasks` |
| Radar | `Radio` | `/repo-radar` |
| Prompts | `MessageSquare` | `/prompts` |

- Active item highlighted with `bg-accent border border-border shadow-sm`
- Inactive items show `text-muted-foreground`, hover to `text-foreground`

### Role Badge

A role badge appears next to the user info with 3 colors:
- `lead` — amber (`bg-amber-500/10 text-amber-600`)
- `dev` — emerald (`bg-emerald-500/10 text-emerald-600`)
- `intern` — blue (`bg-blue-500/10 text-blue-600`)

### Sub-component: `SidebarStats`

Fetches `GET /api/stats` on mount and displays 4 rows:

- **Total Items** — count from feed_items
- **Last Ingest** — timestamp displayed as relative time (e.g. "3h ago")
- **Today** — items fetched today
- **This Week** — items fetched this week

Uses a `timeAgo()` helper: `just now` → `Xm ago` → `Xh ago` → `Xd ago` → formatted date.

### Logout Button

Shown when `user` is available. Calls `signOut()` from AuthProvider, then redirects to `/login` via `router.push()`. Has `hover:text-red-500 hover:bg-red-500/10` styling.

### Sub-component: `ThemeToggle`

Button inside the sidebar footer that calls `useTheme().toggle()` to cycle dark/light mode. Icon switches between `Sun` and `Moon`.

## State

No React state or URL params — all data is fetched via `fetch("/api/stats")` in a `useEffect`. No loading skeleton (API responds from Supabase — sub-10ms).
