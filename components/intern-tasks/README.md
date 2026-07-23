# `components/intern-tasks/` — Intern Safe Task Board Components

## Components

### `InternTaskCard`

**Type:** Client (`"use client"`)

Expandable shadcn-style card used in the `/intern-tasks` grid. Shows compact info by default; clicking "More details" reveals full sections inline.

#### Props

| Prop | Type | Description |
|------|------|-------------|
| `task` | `InternTask` | Task data from config (title, description, difficulty, category, learningObjective, safeEnvironment, expectedOutput, resources) |
| `index` | `number` | Grid position — used for staggered animation delay (`index * 50ms`) |
| `categoryColor` | `string` | Tailwind classes for category badge (`bg-purple-500/10 text-purple-600 ...`) |
| `difficultyColor` | `string` | Tailwind classes for difficulty badge (`bg-emerald-500/10 ...`) |

#### Layout

```
┌──────────────────────────────────────┐
│ [category badge]  [difficulty badge] │
│ Title                                │
│ Description text                     │
│ [More details ▾]                     │
│ ── expanded ──                       │
│ 🎯 Learning Objective: ...           │
│ 🧪 Safe Environment: ...            │
│ 📄 Expected Output: ...             │
│ 🔖 Resources: ...                    │
│ ── collapsed ──                      │
└──────────────────────────────────────┘
```

#### Expanded sections

| Section | Icon | Content |
|---------|------|---------|
| Learning Objective | `Target` | What the intern will learn |
| Safe Environment | `FlaskConical` | Why it's safe and isolated |
| Expected Output | `FileOutput` | What success looks like |
| Resources | `Bookmark` | Links to relevant files/docs (list) |

#### Animations

- Card entrance: `animate-slide-up` with staggered `animationDelay`
- Expanded content: `animate-fade-in`
- Hover: `hover:shadow-lg` lift effect

### Related components (in `components/briefing/`)

- `InternTasks` — homepage widget showing daily rotation + "View all →" link
