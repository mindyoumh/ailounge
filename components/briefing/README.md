# `components/briefing/` — Engineering Briefing Components

9 components for the Engineering Briefing homepage (`app/page.tsx`). Designed with flat cards (`rounded-xl border border-border bg-card`), theme-based icon containers, and subtle hover shadow.

## Design System

Every component uses shared tokens from `globals.css`:

- **Card** — `rounded-xl border border-border bg-card transition-all duration-200 hover:shadow-sm`
- **Icon containers** — `rounded-md bg-accent/70` (or themed bg variants)
- **Source badges** — `rounded-full px-1.5 py-0.5 text-[9px] font-medium font-mono` with per-source colors
- **Rollovers** — `group-hover/card` gradient top bar (StackSummary)
- **No animations** — removed `animate-slide-up` and staggered delays in `b85e090`

## Component Reference

| Component | Type | Purpose | Props |
|-----------|------|---------|-------|
| **Greeting** | Client (`"use client"`) | Time-based greeting with user name from email prefix | `totalItems: number` |
| **StatCard** | Server (presentational) | KPI with theme icon, optional trend indicator | `label`, `value`, `icon`, `trend?`, `theme?` |
| **FeedSection** | Server (presentational) | Categorized feed list with count badge, empty state | `title`, `icon`, `items`, `viewAllHref?`, `theme?` |
| **FeedItemCard** | Server (presentational) | Single feed item row with source badge and date | `item: FeedItem` |
| **FeaturedNews** | Server (presentational) | Hero section — large card + 3 secondary cards for pinned items | `items` |
| **FeedBreakdown** | Client (`"use client"`) | Tabbed Nivo bar chart (By Source / By Category) — desktop side-by-side, mobile tabs | `sources`, `categories`, `total` |
| **InternTasks** | Server (presentational) | Recommended tool + today/tomorrow intern task cards | `recommendedItem`, `todayTask`, `tomorrowTask` |
| **StackSummary** | Client (`"use client"`) | Stack Watchlist summary card — fetches `/api/stats` | none — fetches own data |
| **IngestHealth** | Server (reads db) | Per-ingester health with status dots, source icons, error badge | none — reads `getIngestionStatus()` |
| **FeaturedPrompt** | Server (presentational) | Daily rotating featured prompt card, links to /prompts | `item` (PromptItem \| null) |

## Server vs Client

7 **Server Components** receive data via props or call `getDb()`:

- `Greeting` — time-based greeting, uses `useUser()` (client-only: reads localStorage session)
- `StatCard` — pure presentational
- `FeedSection` — receives `items` array, renders `FeedItemCard` children
- `FeedItemCard` — pure presentational row
- `FeaturedNews` — receives `items` array
- `InternTasks` — receives tasks from `src/config/intern-tasks.ts`
- `IngestHealth` — calls `getIngestionStatus()` from DB
- `FeaturedPrompt` — receives `item` prop from homepage server component

2 **Client Components** (`"use client"`):

- `FeedBreakdown` — uses `Tabs` from Radix UI + `ResponsiveBar` from `@nivo/bar`
- `StackSummary` — fetches `/api/stats` on mount for stack risk counts

## Theme Color Map

Used by `StatCard`, `FeedSection`, and `FeedItemCard` source badges:

| Theme | Icon Container | Icon/Text | Badge |
|-------|---------------|-----------|-------|
| `teal` | `bg-teal-100 dark:bg-teal-900/50` | `text-teal-600 dark:text-teal-400` | `bg-teal-100 dark:bg-teal-900/50` |
| `purple` | `bg-purple-100 dark:bg-purple-900/50` | `text-purple-600 dark:text-purple-400` | `bg-purple-100 dark:bg-purple-900/50` |
| `blue` | `bg-blue-100 dark:bg-blue-900/50` | `text-blue-600 dark:text-blue-400` | `bg-blue-100 dark:bg-blue-900/50` |
| `rose` | `bg-rose-100 dark:bg-rose-900/50` | `text-rose-600 dark:text-rose-400` | `bg-rose-100 dark:bg-rose-900/50` |
| `amber` | `bg-amber-100 dark:bg-amber-900/50` | `text-amber-600 dark:text-amber-400` | `bg-amber-100 dark:bg-amber-900/50` |

## Source Badge Colors

Used by `FeaturedNews` and `FeedItemCard` for source labels:

| Source | Color class |
|--------|-------------|
| `hn` | `text-orange-600 dark:text-orange-400` / `bg-orange-100 dark:bg-orange-900/50` |
| `github_trending` | `text-purple-600 dark:text-purple-400` / `bg-purple-100 dark:bg-purple-900/50` |
| `rss` | `text-blue-600 dark:text-blue-400` / `bg-blue-100 dark:bg-blue-900/50` |
| `repo_radar` | `text-teal-600 dark:text-teal-400` / `bg-teal-100 dark:bg-teal-900/50` |

## Props Detail

### Greeting

| Prop | Type | Description |
|------|------|-------------|
| `totalItems` | `number` | Total item count displayed in the subtitle |

- Uses `useUser()` to extract email prefix as the user's name
- Time-based greeting: Good morning (< 12h), Good afternoon (< 17h), Good evening
- Formats current date as "Monday, July 21"

### StatCard

| Prop | Type | Description |
|------|------|-------------|
| `label` | `string` | Display label below the value |
| `value` | `string \| number` | Large bold value |
| `icon` | `React.ElementType` | Lucide icon component |
| `trend` | `{ value: string, positive: boolean }?` | Optional trend arrow + text |
| `theme` | `"blue" \| "teal" \| "purple" \| "amber" \| "rose"?` | Icon container color theme |

### FeedSection

| Prop | Type | Description |
|------|------|-------------|
| `title` | `string` | Section heading |
| `icon` | `React.ElementType` | Lucide icon in header |
| `items` | `FeedItem[]` | Items to render via `FeedItemCard` |
| `viewAllHref` | `string?` | Link to filtered feed page |
| `theme` | `"teal" \| "purple" \| "blue" \| "rose"?` | Icon and badge color theme |

### FeedItemCard

| Prop | Type | Description |
|------|------|-------------|
| `item` | `FeedItem` | `{ source, title, url, published_at }` |

### FeedBreakdown

| Prop | Type | Description |
|------|------|-------------|
| `sources` | `BreakdownItem[]` | `[{ name: string, count: number }]` |
| `categories` | `BreakdownItem[]` | `[{ name: string, count: number }]` |
| `total` | `number` | Total for percentage calculation |

- Uses Nivo `ResponsiveBar` with horizontal layout, per-name color hash
- Source colors: hn=orange, github_trending=purple, rss=blue
- **Layout:** Desktop (≥1024px) shows sources and categories side-by-side; mobile uses tabs

### InternTasks

| Prop | Type | Description |
|------|------|-------------|
| `recommendedItem` | `FeedItem \| null` | Top recommended tool/news item |
| `todayTask` | `InternTaskData` | `{ title, description, difficulty, category }` |
| `tomorrowTask` | `InternTaskData` | `{ title, description, difficulty, category }` |

- Difficulty badges: `beginner` (emerald), `intermediate` (amber), `advanced` (red)
- Category badge colors: synthetic-data (purple), mock-apis (blue), local-db (emerald), code-review (orange), docs-research (pink), git-workflow (cyan)
- Tasks rotate daily from `src/config/intern-tasks.ts`
- Includes "View all N tasks" link at bottom routing to `/intern-tasks`
- Layout: recommended tool card (if available) + tasks card with today/tomorrow

### StackSummary

**Type:** Client (`"use client"`) — fetches data via `useEffect`.

| Prop | Type | Description |
|------|------|-------------|
| (none) | — | Fetches `/api/stats` on mount, reads `stackTotal`, `stackHigh`, `stackMedium`, `stackLow` |

- Renders as a clickable card linking to `/watchlist`
- Shows total package count plus per-risk-level breakdown with colored icons
- Gradient top bar (`bg-gradient-to-r from-accent-vibrant/60 to-accent-vibrant/20`) on hover
- ArrowRight icon on hover
- **Hidden** when `total === 0` (no items tracked yet)
- Icons: `ShieldX` (rose) for high, `ShieldAlert` (amber) for medium, `ShieldCheck` (emerald) for low

### IngestHealth

**Type:** Server — reads DB directly (no `"use client"`).

| Prop | Type | Description |
|------|------|-------------|
| (none) | — | Reads `getIngestionStatus()` via `serviceClient` |

- Source config: hn → Radio (orange), github_trending → TrendingUp (purple), rss → Rss (blue), repo_radar → Radar (teal)
- Status dot: green when ok, red when error, gray when no data
- Empty state: "No data. Run `npm run ingest`"
- Header badge: "Healthy" (emerald) for 0 errors, "N error(s)" (red) otherwise
- Shows per-source item count (+N) on last run
- Replaced `AutomationStatus` in commit `e331bf9`

### FeaturedPrompt

**Type:** Server (presentational).

| Prop | Type | Description |
|------|------|-------------|
| `item` | `PromptItem \| null` | Featured prompt object with `title`, `description`, `category` |

- Renders a flat card with the featured prompt title, category badge, description preview, and a "View in Library" link to `/prompts`
- ArrowRight icon on hover
- When `item` is null, renders nothing (hidden)
