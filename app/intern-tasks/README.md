# `/intern-tasks` — Intern Safe Task Board

## Route

`/intern-tasks` — Public (excluded from auth middleware). No API routes, no DB dependency — purely static config.

## Page

Client component (`"use client"`) — full-page read-only catalog of safe learning tasks for interns.

### Layout

```
┌──────────────────────────────────────────────────┐
│  Intern Safe Task Board                           │
│  Safe, isolated tasks for learning                │
├──────────────────────────────────────────────────┤
│  Category pills: All | Synthetic Data | Mock ...  │
│  Difficulty: [All Levels ▼]  Sort: [Default ▼]   │
│  Showing 13 of 13 tasks                           │
├──────────────────────────────────────────────────┤
│  ┌──────────┐ ┌──────────┐ ┌──────────┐         │
│  │ Task Card│ │ Task Card│ │ Task Card│         │
│  │ [expand ▾]│ │ [expand ▾]│ │ [expand ▾]│        │
│  └──────────┘ └──────────┘ └──────────┘         │
└──────────────────────────────────────────────────┘
```

### Features

| Feature | Detail |
|---------|--------|
| Category filter | 7 pill buttons: All, Synthetic Data, Mock APIs, Local DB, Code Review, Docs & Research, Git Workflow |
| Difficulty filter | Select dropdown: All, Beginner, Intermediate, Advanced |
| Sort | Select dropdown: Default, Difficulty ↑/↓, Title A–Z/Z–A |
| Grid | Responsive `grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4` |
| Empty state | "No tasks match your filters" with Reset Filters button |
| Staggered animation | Cards animate in with `delay={index * 50}ms` |

### Task Card (expanded)

Each card shows compact info by default. Clicking "More details" reveals:

- **Learning Objective** — what the intern will learn
- **Safe Environment** — why it's safe and isolated
- **Expected Output** — what success looks like
- **Resources** — links to relevant files/docs

### Data Source

All tasks come from `src/config/intern-tasks.ts` — a static array of `InternTask` objects. No database queries, no API calls. The config is imported directly:

```ts
import { INTERN_TASKS } from "@/src/config/intern-tasks";
```

### Related

- Homepage widget: `components/briefing/intern-tasks.tsx` shows daily rotation + "View all" link
- Config: `src/config/intern-tasks.ts` (13 tasks, 6 categories, 3 difficulty levels)
- Plan doc: `docs/plans/intern-task-board-mvp-plan.md`
