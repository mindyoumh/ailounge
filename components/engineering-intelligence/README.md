# `components/engineering-intelligence/` — Dashboard Widgets

4 custom widgets for the Engineering Briefing homepage (`app/page.tsx`).

## Widget Reference

| Widget | Type | Data Source | Purpose |
|--------|------|-------------|---------|
| **AutomationStatus** | Server | `getIngestionStatus()`, `getGlobalIngestionStatus()` | Per-source status dots (green/red/gray), error count badge, "X ago" timestamps, item count |
| **StatCard** | Server | Injected props | Unified stat card with gradient icon, accent bar, optional subtitle |
| **BreakdownCard** | Client | `getItemsBySource()`, `getItemsByCategory()` | Tabbed panel (By Source / By Category) with Nivo horizontal bar chart |
| **IngestButton** | Client | `POST /api/ingest` | Triggers on-demand ingestion with sonner toast feedback |

## Deleted Components (replaced)

| Component | Replaced By | Reason |
|-----------|-------------|--------|
| `LastIngestionStat` | `StatCard` | Unified into single configurable stat card |
| `TimeWindowStat` | `StatCard` | Unified into single configurable stat card |

## Server vs Client

2 widgets are **Server Components** — they query Supabase PostgreSQL directly or receive data via props:

- `AutomationStatus` — calls `getIngestionStatus()` which reads from `kv_store`
- `StatCard` — pure presentational, receives `value`, `label`, `icon`, etc. as props

2 widgets are **Client Components** (`"use client"`):

- `BreakdownCard` — uses `Tabs` from Radix UI + `ResponsiveBar` from `@nivo/bar`
- `IngestButton` — calls `fetch("/api/ingest")` on click, uses `sonner` toast

## Props

### StatCard
```ts
interface StatCardProps {
  label: string;
  value: string | number;
  icon: React.ElementType;    // lucide-react icon
  accentColor: string;        // Tailwind border color e.g. "bg-emerald-500"
  gradient: string;           // Tailwind gradient e.g. "from-emerald-500 to-teal-500"
  secondary?: string;         // Optional subtitle
  delay?: number;             // Animation delay in ms
}
```

### BreakdownCard
```ts
interface BreakdownCardProps {
  sources: BreakdownItem[];    // [{ name: string, count: number }]
  categories: BreakdownItem[]; // [{ name: string, count: number }]
  total: number;
  delay?: number;              // Animation delay in ms
}
```

- Uses Nivo `ResponsiveBar` with horizontal layout, per-name color hash
- Palette: amber, red, violet, cyan, lime, pink, orange, teal

### IngestButton
No props — self-contained. Calls `POST /api/ingest`, shows spinner during request, displays sonner toast on success/failure.

### AutomationStatus
No props — reads all data from `kv_store` internally.
