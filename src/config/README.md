# `src/config/` — Seed Data & Configuration

Seed data and configuration for the Developer Dashboard features. Currently contains intern task definitions for Module 8 (Intern Safe Task Board) and package suggestions for the Stack Watchlist.

## Files

### `package-suggestions.ts`

Curated list of ~181 packages for the Watchlist add-item search combobox. Provides autocomplete suggestions across 10 ecosystems.

**Interface:**

```ts
interface PackageSuggestion {
  name: string;
  ecosystem: string;
}
```

**Coverage by ecosystem:**

| Ecosystem | Count | Examples |
|-----------|-------|----------|
| npm | 69 | React, Next.js, Vue, Svelte, Tailwind CSS, Prisma, Vite, Supabase, Clerk |
| PyPI | 25 | Django, Flask, FastAPI, PyTorch, Pandas, SQLAlchemy |
| crates.io | 11 | Serde, Tokio, Axum, Actix, Rocket |
| Go | 10 | Cobra, Viper, Gin, Fiber, GORM |
| Maven | 9 | Spring Boot, Hibernate, Log4j, JUnit |
| NuGet | 7 | ASP.NET Core, Entity Framework, Serilog |
| RubyGems | 6 | Rails, Devise, RSpec, Sidekiq |
| Packagist | 5 | Laravel, Symfony, Composer, PHPUnit |
| (none/infra) | 39 | PostgreSQL, Docker, Kubernetes, AWS, Redis, Nginx |


**How it's consumed:**

`GET /api/packages/search?q=` filters this list (case-insensitive substring), falls back to npm registry API when < 8 curated matches, merges results (max 10).

### `intern-tasks.ts`

Provides a static array of safe, self-contained tasks that interns can pick up without touching production systems.

**Interface:**

```ts
interface InternTask {
  title: string;
  description: string;
  difficulty: "beginner" | "intermediate" | "advanced";
  category: "synthetic-data" | "mock-apis" | "local-db" | "code-review" | "docs-research" | "git-workflow";
  learningObjective: string;
  safeEnvironment: string;
  expectedOutput: string;
  resources: string[];
}
```

**Data:** 13 tasks across 6 categories:

| Category | Count |
|----------|-------|
| synthetic-data | 1 |
| mock-apis | 3 |
| local-db | 3 |
| code-review | 2 |
| docs-research | 2 |
| git-workflow | 2 |

**How it's consumed:**

Two consumers:

1. **Homepage widget** (`components/briefing/intern-tasks.tsx`) — day-based rotation, shows today's task + tomorrow's preview with category + difficulty badges and a "View all" link.
2. **Full page** (`app/intern-tasks/page.tsx`) — filterable grid with category pills, difficulty select, sort controls, and expandable cards showing all task detail fields.

**How to add a task:**

```ts
{
  title: "Task name",
  description: "Brief description of what to do",
  difficulty: "beginner", // or "intermediate" | "advanced"
  category: "mock-apis", // one of 6 categories
  learningObjective: "What the intern will learn",
  safeEnvironment: "Why it's safe",
  expectedOutput: "What success looks like",
  resources: ["Link 1", "Link 2"],
}
```
