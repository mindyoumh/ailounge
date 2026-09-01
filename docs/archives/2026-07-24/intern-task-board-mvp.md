# Module 8: Intern Safe Task Board — MVP Plan

## Scope (Strict)

A **read-only catalog** of safe learning tasks for interns. This is **not** a project management tool, productivity app, or gamification system. No user accounts, no task claiming, no completion tracking, no completion history, no localStorage, no analytics for completed tasks.

Tasks are read-only — presented in a filterable, sortable grid for browsing.

## Guiding Principles (from Sir Bo)

| Principle | Implication |
|-----------|-------------|
| Interns learn on **safe, isolated** tasks | Each task exercises a synthetic env: mock APIs, local SQLite, fake logs, fake incidents, generated fixtures, practice branches, documentation/research |
| Not a productivity/PM tool | No claiming, no completing, no tracking progress over time |
| Tasks organized by **category** **and** **difficulty** | Two-axis navigation — pick what you want to learn, at your level |
| Each task is self-contained | Title, Category, Difficulty, Description, Learning Objective, Safe Environment, Expected Output, Resources |
| Lightweight, consistent with existing dashboard | Same shadcn card patterns, same responsive grid, same server-first architecture |

## 1. Data Model (Expand `src/config/intern-tasks.ts`)

### Interface

```ts
export interface InternTask {
  title: string;
  category: "synthetic-data" | "mock-apis" | "local-db" | "code-review" | "docs-research" | "git-workflow";
  difficulty: "beginner" | "intermediate" | "advanced";
  description: string;
  learningObjective: string;
  safeEnvironment: string;
  expectedOutput: string;
  resources: string[];
}
```

### Categories

| Category | What it covers |
|----------|----------------|
| `synthetic-data` | Generate fixtures, seed data, fake datasets |
| `mock-apis` | Build mock servers, stub external services |
| `local-db` | SQLite queries, local DB exercises |
| `code-review` | Review vulnerable/sample PRs, spot bugs |
| `docs-research` | Summarize news, write docs, research topics |
| `git-workflow` | Branching, rebase, conflict resolution practice |

### Difficulty levels

`beginner` | `intermediate` | `advanced`

## 2. Page Structure

### Route

`/intern-tasks` — single page, server component.

### Layout

```
┌──────────────────────────────────────────────────────────────────┐
│  Intern Safe Task Board                                          │
│  Safe, isolated tasks for learning and contributing              │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Category tabs / pills: All | Synthetic Data | Mock APIs | ...   │
│  Difficulty filter: All | Beginner | Intermediate | Advanced     │
│  Sort: Default | By difficulty | By title                        │
│  "Showing N tasks"                                               │
│                                                                  │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐        │
│  │ Task Card     │  │ Task Card     │  │ Task Card     │        │
│  │ [category]    │  │ [category]    │  │ [category]    │        │
│  │ [difficulty]  │  │ [difficulty]  │  │ [difficulty]  │        │
│  │ Title         │  │ Title         │  │ Title         │        │
│  │ Description   │  │ Description   │  │ Description   │        │
│  │ [Expand ▸]    │  │ [Expand ▸]    │  │ [Expand ▸]    │        │
│  └───────────────┘  └───────────────┘  └───────────────┘        │
│                                                                  │
│  ┌───────────────┐  ┌───────────────┐                           │
│  │ Task Card     │  │ Task Card     │                           │
│  │ ...           │  │ ...           │                           │
│  └───────────────┘  └───────────────┘                           │
└──────────────────────────────────────────────────────────────────┘
```

### Task Card (expanded)

Each card shows compact info by default. Clicking "Expand" reveals full detail inline:

```
┌──────────────────────────────────────────────────────────────┐
│ [synthetic-data]  [intermediate]                              │
│ **Create seed fixtures for the feed table**                   │
│ Write a script that generates 50 fake `feed_items` rows       │
│                                                               │
│ [▸ Expand]                                                    │
│ ──── expanded ────                                            │
│ **Learning Objective:** Practice generating realistic test     │
│ data with SQLite.                                             │
│ **Safe Environment:** Local SQLite DB, no production data.    │
│ **Expected Output:** `seed.ts` script + 50 rows in DB.        │
│ **Resources:** src/db/schema.ts, better-sqlite3 docs          │
│ ──── collapsed ────                                           │
└──────────────────────────────────────────────────────────────┘
```

## 3. Component Tree

```
app/intern-tasks/page.tsx  (Server)
├── InternTaskHeader        (Server) — page title + subtitle
├── InternTaskControls      (Client) — category pills + difficulty select + sort select
└── InternTaskGrid          (Server) — maps filtered tasks to cards
    └── InternTaskCard      (Client) — expandable card with full details
```

### Component Spec

| Component | File | Type | Purpose |
|-----------|------|------|---------|
| `InternTaskHeader` | `components/intern-tasks/intern-task-header.tsx` | Server | Page icon + title + subtitle, same gradient icon style as homepage |
| `InternTaskControls` | `components/intern-tasks/intern-task-controls.tsx` | Client (`"use client"`) | Category pill row + difficulty Select + sort Select. Stateful: `useState` for each filter. No persistence. Pattern: watchlist filter bar |
| `InternTaskGrid` | `components/intern-tasks/intern-task-grid.tsx` | Server | Responsive grid `grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4`. Accepts `tasks: InternTask[]`, renders cards |
| `InternTaskCard` | `components/intern-tasks/intern-task-card.tsx` | Client (`"use client"`) | Expandable shadcn Card. Shows compact: category badge, difficulty badge, title, description, expand button. Expanded: learningObjective, safeEnvironment, expectedOutput, resources list. Uses `cn()` from `@/lib/utils` |

No new UI primitives needed — all shadcn components already exist (`Card`, `Badge`, `Button`, `Select`).

## 4. Filtering & Sorting

### Category filter

Row of pill buttons: `All | Synthetic Data | Mock APIs | Local DB | Code Review | Docs & Research | Git Workflow`

- `All` is default, shows all categories
- Clicking a category filters to that category only
- Active pill has accent color, inactive has muted border
- Pattern: same as feed page source pills

### Difficulty filter

`Select` dropdown: `All | Beginner | Intermediate | Advanced`

- `All` is default
- Changes shown set immediately

### Sort

`Select` dropdown: `Default | By Difficulty ↑ | By Difficulty ↓ | By Title A–Z | By Title Z–A`

- Default = array order from config

**State:** Category, difficulty, and sort are `useState` values in `InternTaskControls`. No persistence — resets on page reload.

## 5. What We DON'T Build (Explicitly Excluded)

| Feature | Reason |
|---------|--------|
| Task claiming | Turns it into a PM tool; violates "no user accounts" |
| Completion checkbox / toggle | Turns it into a productivity tracker |
| Completion history / streak | Gamification, out of scope |
| localStorage for any state | No persistence needed for read-only catalog |
| User accounts / auth | Multi-user state is explicitly out of scope |
| API routes | All data comes from static config |
| SQLite tables | Config-only data layer |
| Analytics / metrics on tasks | No tracking of what interns do |
| Daily pick | Implies a "today's recommended work" pattern, too prescriptive |

## 6. Sidebar & Homepage Integration

### Sidebar

Add to `NAV_ITEMS` in `components/sidebar/sidebar.tsx`:

```ts
{ href: "/intern-tasks", label: "Tasks", icon: BookOpen }
```

Between Logs and Radar, maintaining alphabetical-by-label order.

### Homepage widget

The existing `components/briefing/intern-tasks.tsx` widget already shows 3 randomized tasks + "View all →" link. Keep as-is — it correctly rotates through tasks and links to `/intern-tasks`.

No changes needed to the homepage widget.

## 7. Task Data Enhancement

### New fields to add to `InternTask` interface and each of the 13 existing tasks:

| Task | Category |
|------|----------|
| Set up a local dev environment | `git-workflow` |
| Write a test for an ingester | `mock-apis` |
| Add a new RSS feed source | `docs-research` |
| Build a chart for feed volume | `local-db` |
| Review and tag unclassified entries | `docs-research` |
| Review a pull request for common bugs | `code-review` |
| Write a synthetic health check | `mock-apis` |
| Replace a live RSS feed with a mock | `mock-apis` |
| Query the DB for untagged entries | `local-db` |
| Create seed fixtures for the feed table | `synthetic-data` |
| Practice a rebase workflow | `git-workflow` |
| Investigate a spike in 500 errors | `local-db` |
| Respond to a simulated outage | `code-review` |

### Task count per category

| Category | Count |
|----------|-------|
| synthetic-data | 1 |
| mock-apis | 3 |
| local-db | 3 |
| code-review | 2 |
| docs-research | 2 |
| git-workflow | 2 |

### Task count per difficulty

| Difficulty | Count |
|------------|-------|
| beginner | 4 |
| intermediate | 6 |
| advanced | 3 |

## 8. Implementation Phases

### Phase 1: Enhance data model

**Files modified:** `src/config/intern-tasks.ts`

- Add `category`, `learningObjective`, `safeEnvironment`, `expectedOutput`, `resources` to `InternTask` interface
- Populate all 13 tasks with the new fields
- Assign categories per table above

**No code to modify — data-only change.**

### Phase 2: Build page shell + sidebar

| Step | File | Action |
|------|------|--------|
| 2.1 | `app/intern-tasks/page.tsx` | Create. Server component. Import `INTERN_TASKS`, render header + controls + grid. All filtering/sorting done client-side via state in controls |
| 2.2 | `components/sidebar/sidebar.tsx` | Add `{ href: "/intern-tasks", label: "Tasks", icon: BookOpen }` to NAV_ITEMS |

### Phase 3: Build components

| Step | File | Action |
|------|------|--------|
| 3.1 | `components/intern-tasks/intern-task-header.tsx` | Create. Server component. Page icon + title + subtitle |
| 3.2 | `components/intern-tasks/intern-task-controls.tsx` | Create. Client component. Category pills + difficulty select + sort select. useState for each filter. Pass filtered/sorted tasks down |
| 3.3 | `components/intern-tasks/intern-task-grid.tsx` | Create. Server component. Responsive grid, maps tasks to InternTaskCard |
| 3.4 | `components/intern-tasks/intern-task-card.tsx` | Create. Client component. Expandable card. Inline expand/collapse via useState |

### Phase 4: Polish

| Step | File | Action |
|------|------|--------|
| 4.1 | `app/intern-tasks/page.tsx` | Handle empty state (zero tasks match filter) — show "No tasks match your filters" message |
| 4.2 | `components/intern-tasks/intern-task-card.tsx` | Add hover subtle lift animation, smooth expand transition |
| 4.3 | — | `npm run build` — verify passes |

## 9. Files Summary

| File | Action | Est. Lines |
|------|--------|-----------|
| `src/config/intern-tasks.ts` | Modify (add fields, populate) | ~+15 per task |
| `app/intern-tasks/page.tsx` | Create | ~40 |
| `components/intern-tasks/intern-task-header.tsx` | Create | ~20 |
| `components/intern-tasks/intern-task-controls.tsx` | Create | ~60 |
| `components/intern-tasks/intern-task-grid.tsx` | Create | ~20 |
| `components/intern-tasks/intern-task-card.tsx` | Create | ~80 |
| `components/sidebar/sidebar.tsx` | Modify | ~+1 |
| **Total** | **6 new, 2 modified** | **~360 new lines** |

## 10. Deviations from Original Plan (and Why)

| Original Plan | Revised Plan | Rationale |
|---------------|--------------|-----------|
| Completion toggle + localStorage | ❌ Removed entirely | Not a productivity tracker; read-only per spec |
| Daily pick component | ❌ Removed entirely | Too prescriptive; homepage widget already rotates tasks |
| `completion-toggle.tsx` component | ❌ Removed | No completion state to manage |
| Tasks filtered by difficulty only | Filter by **category + difficulty + sort** | Aligns with "organize by category AND difficulty" |
| `InternTask` missing new fields | Added `category`, `learningObjective`, `safeEnvironment`, `expectedOutput`, `resources` | Each task is self-contained with full detail on expand |
| "Deferred" items claiming tracking etc. | **Explicitly excluded** | Not deferred — out of scope permanently per Sir Bo |
| TaskCard shows strikethrough on complete | Card is **expandable** inline | Read-only means visual state only shows more info, not progress |
