# `components/command-palette/` — Command Palette

## Component

### `CommandPalette`

**Type:** Client (`"use client"`)

A Cmd+K / Ctrl+K keyboard shortcut palette for quick navigation and search across the dashboard.

### Trigger

| Shortcut | Action |
|----------|--------|
| `Meta+K` (Mac) / `Ctrl+K` (Windows) | Toggle open/close |
| `Esc` | Close |

### Sections (in order)

| Section | Source | Data |
|---------|--------|------|
| Pages | `NAV_ITEMS` from `components/sidebar/sidebar.tsx` | 7 nav items with lucide icons |
| Stack Watchlist | `GET /api/watchlist` (fetched on open) | All tracked items with category subtitle |
| Repo Radar | `GET /api/repo-radar` (fetched on open) | All tracked repos with language/description |
| Feed | `GET /api/feed?q=...&limit=5` (debounced 200ms, min 2 chars) | Live search of feed items by title |
| Prompts | `GET /api/prompts?search=...&limit=5` (debounced 200ms, min 2 chars) | Live search of prompts by title |

### Behavior

- Opens empty by default, shows Pages + static data (watchlist, radar)
- Typing triggers debounced live search against feed and prompts APIs
- Selecting a URL opens internal routes via `router.push()` or external URLs in a new tab
- Footer shows keyboard hints: `↑↓ navigate`, `↵ open`, `esc close`

### Dependencies

- `cmdk` — `CommandPrimitive` via `@/components/ui/command`
- `@radix-ui/react-dialog` — via `@/components/ui/dialog`
- `NAV_ITEMS` from `@/components/sidebar/sidebar`

### Rendering

Rendered globally in `app/layout.tsx`:
```tsx
<CommandPalette />
```
