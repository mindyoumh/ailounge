# `/components` — UI Components & Dashboard Widgets

Reusable React components used by the Developer Dashboard pages.

## Structure

| Directory / File | Contents |
|------------------|----------|
| `ui/` | shadcn/ui-style primitives — 13 components (Button, Card, Badge, Input, Select, Tabs, Toggle, Separator, Table, Skeleton, DropdownMenu, Command, Dialog) |
| `engineering-intelligence/` | Dashboard-specific widgets — 3 components (BreakdownCard, IngestButton, StatCard). `AutomationStatus` replaced by `IngestHealth` in `briefing/` |
| `briefing/` | Engineering Briefing homepage components — 9 components (Greeting, StatCard, FeedSection, FeedItemCard, FeaturedNews, FeedBreakdown, InternTasks, IngestHealth, StackSummary, FeaturedPrompt) |
| `sidebar/` | Fixed left sidebar — user info, nav, theme toggle, logout, Quick Stats |
| `prompts/` | Prompt Library — 3 components (PromptCard, CategoryFilter, SourceFilter) |
| `logs/` | Log Analysis Dashboard — 9 components (CsvUpload, OverviewCards, ErrorTrendChart, SourceBreakdown, SeverityLegend, PatternDrillDown, DateFilter, PatternSearch, SeverityFilter) |
| `command-palette/` | Command Palette — Cmd+K palette, searches pages/feed/prompts/watchlist/radar |
| `intern-tasks/` | Intern Safe Task Board — 1 component (InternTaskCard) with expandable detail sections |
| `theme-provider.tsx` | Dark/light mode React Context (used by Sidebar theme toggle) |
| `auth-provider.tsx` | Supabase Auth context — `AuthProvider` + `useUser()` hook (user, role, loading, signOut, refreshRole) |
| `shell.tsx` | Layout shell — hides Sidebar on `/login` and `/signup` routes |

## Shared Utility

All components import `cn()` from `@/lib/utils` — the shadcn pattern using `clsx` + `tailwind-merge`.

There is a second `cn()` at `src/lib/utils.ts` (simple string join) — that one is for ingesters, NOT UI components.

## Conventions

- `ui/` components are standard shadcn/ui — minimal customization from defaults
- `engineering-intelligence/` + `briefing/` + `sidebar/` are custom-built for this project
- `"use client"` where needed: Radix primitives, Sidebar (usePathname), ThemeProvider, FeedBreakdown (Nivo), BreakdownCard, IngestButton, CsvUpload, ErrorTrendChart, SourceBreakdown
- Server components call `getDb()` at request time — no `"use client"` needed

## Sub-READMEs

- [UI Primitives →](./ui/README.md)
- [Dashboard Widgets →](./engineering-intelligence/README.md)
- [Briefing Components →](./briefing/README.md)
- [Sidebar →](./sidebar/README.md)
- [Prompt Library Components →](./prompts/README.md)
- [Log Analysis Components →](./logs/README.md)
- [Intern Task Components →](./intern-tasks/README.md)
- [Command Palette →](./command-palette/README.md)
