# Plane — Engineering Report

> **Date:** 2026-07-21
> **Repository:** github.com/makeplane/plane
> **Version:** v1.3.1 (API: 0.24.0)
> **License:** AGPL-3.0

---

## 1. What It Is

Plane is an **open-source project management tool** — a self-described alternative to Linear and Jira. It provides issues, cycles (sprints), modules (epics), pages (wiki), and analytics in a unified interface.

Key characteristics:

- **Full-featured project management** — Issues, cycles, modules, pages, intake, analytics
- **Real-time collaboration** — Yjs + Hocuspocus for multi-user editing
- **Self-hostable** — Docker Compose deployment with 13 services
- **Multi-app architecture** — Web, Admin, Space (public views), Live (real-time)
- **Enterprise features** — Role-based access, webhooks, notifications, export

**What it is NOT:** Plane is not a general-purpose application framework. It is a standalone product with a Django backend, not a library or SDK for embedding.

---

## 2. Research Scope

This report evaluates Plane for potential technology adoption, component reuse, or architectural inspiration. Specific areas of interest:

- **Real-time collaboration** — The Hocuspocus + Yjs pattern for multi-user editing
- **Domain model** — Issue/Cycle/Module/Page hierarchy for project management features
- **UI component library** — `@plane/ui` built on BlueprintJS + Radix UI + Headless UI
- **Monorepo structure** — pnpm workspaces + Turborepo organization

**Sources consulted:**

| Source | File | Content |
|--------|------|---------|
| `AGENTS.md` | Mind You | Tech stack definition, constraints, coding rules |
| `DASHBOARD-BUILD-SPEC.md` | Mind You | Architecture spec, feature requirements |
| `repository-analysis.md` | `.scratch/plane-rnd/` | Full technical analysis of the repository |
| `third-party-repository-rnd-synthesis.md` | `docs/research/` | Cross-repository synthesis and recommendations |

---

## 3. Technology Stack Summary

### Languages

| Language | Usage | Scope |
|----------|-------|-------|
| Python 3.12+ | Backend API (Django REST Framework) | Primary (backend) |
| TypeScript | Frontend apps, shared packages | Primary (frontend) |
| Go | Caddy reverse proxy | Infrastructure |
| SQL | Database migrations (Django) | Data layer |

### Frontend Stack

| Technology | Usage | Mind You Equivalent |
|------------|-------|---------------------|
| React 19 | Frontend UI | React 19 (same) |
| React Router 7 | SSR routing | Next.js App Router |
| MobX | State management | None (server-first) |
| Tailwind CSS 4 | Styling | Tailwind CSS 4 (same) |
| Vite | Build tool | Next.js / Turbopack |
| SWR | Data fetching | Server components + fetch |
| Axios | HTTP client | fetch() |
| Radix UI | UI primitives | Radix UI / shadcn/ui |
| Headless UI | UI primitives | Radix UI / shadcn/ui |
| BlueprintJS | UI components | shadcn/ui |
| Recharts | Charts | Nivo |
| react-hook-form | Forms | React Hook Form |

### Backend Stack

| Technology | Usage | Mind You Equivalent |
|------------|-------|---------------------|
| Django 5.x | REST API framework | Next.js API routes |
| PostgreSQL 15 | Primary database | Supabase PostgreSQL |
| Valkey/Redis 7.x | Cache, real-time sync | Supabase (managed) |
| RabbitMQ 3.13 | Message queue (Celery) | N/A |
| MinIO / S3 | Object storage | Supabase Storage |
| Celery | Background tasks | N/A |
| Hocuspocus | Real-time WebSocket server | N/A |

### Real-Time Collaboration

| Technology | Purpose |
|------------|---------|
| Hocuspocus | Yjs WebSocket server |
| Yjs | CRDT for real-time collaboration |
| y-prosemirror | ProseMirror ↔ Yjs binding |
| Redis extension | Multi-instance sync |

### Build & Dev Tools

| Tool | Purpose |
|------|---------|
| pnpm | Package manager |
| Turborepo | Monorepo task runner |
| oxlint / oxfmt | Linting / formatting |
| Storybook | Component documentation |

### Infrastructure

| Component | Technology |
|-----------|------------|
| Database | PostgreSQL 15 |
| Cache / Sync | Valkey/Redis 7.x |
| Message Queue | RabbitMQ 3.13 |
| Object Storage | MinIO (S3-compatible) |
| Real-time | Hocuspocus + Yjs |
| Deployment | Docker Compose (13 containers) |

---

## 4. Architecture Highlights

### Monorepo Structure

```
plane/
├── apps/
│   ├── api/           # Django REST API (Python)
│   ├── web/           # Main project management UI (React Router 7)
│   ├── admin/         # Server administration (React Router 7)
│   ├── space/         # Public shared views (React Router 7)
│   ├── live/          # Real-time collaboration (Hocuspocus + Yjs)
│   └── proxy/         # Caddy reverse proxy (Go)
├── packages/
│   ├── editor/        # Rich text editor (TipTap/Yjs)
│   ├── ui/            # Shared UI components (BlueprintJS + Radix)
│   ├── shared-state/  # MobX stores
│   ├── services/      # API service layer
│   ├── types/         # TypeScript types
│   ├── constants/     # Shared constants
│   ├── hooks/         # React hooks
│   ├── utils/         # Utility functions
│   ├── i18n/          # Internationalization
│   └── ...            # 15 total shared packages
```

### Multi-App Architecture

1. **Web app** — Main project management interface (React Router 7 SSR)
2. **Admin app** — Server/instance administration
3. **Space app** — Public-facing shared views (read-only, guest access)
4. **Live app** — Real-time collaboration server (Hocuspocus + Yjs)
5. **API app** — Django REST backend (Python)
6. **Proxy** — Caddy reverse proxy (Go)

### Domain Model

```
Workspace
├── Project
│   ├── Issue (core entity)
│   │   ├── Cycle (sprint)
│   │   ├── Module (epic/feature)
│   │   ├── State (status workflow)
│   │   └── Page (wiki/docs)
│   └── Analytics
└── Intake (external issues)
```

### Real-Time Collaboration Flow

```
Client (React + TipTap)
  │
  ├── @hocuspocus/provider (WebSocket)
  │
  └── Hocuspocus Server (apps/live/)
        │
        ├── Yjs CRDT (real-time sync)
        │
        └── Redis Extension (multi-instance broadcast)
```

### Deployment

- **Docker Compose** — 13 containers for full stack
- **Services:** web, admin, space, api, worker, beat-worker, migrator, live, db, redis, mq, minio, proxy
- **Operational overhead:** Significant — requires Python, Node.js, Redis, RabbitMQ, MinIO

---

## 5. Compatibility with Mind You

### Stack Alignment

| Dimension | Mind You | Plane | Alignment |
|-----------|----------|-------|-----------|
| Frontend framework | React 19 | React 19 | ✅ Same |
| Routing | Next.js App Router | React Router 7 SSR | ❌ Different |
| Styling | Tailwind CSS 4 | Tailwind CSS 4 | ✅ Same |
| UI components | Radix UI / shadcn | BlueprintJS + Radix | ⚠️ Different |
| State management | None (server-first) | MobX | ❌ Different |
| Backend | Next.js API routes | Django REST Framework | ❌ Different |
| Database | Supabase (managed) | PostgreSQL (self-managed) | ⚠️ Same DB, different access |
| Auth | Supabase Auth | Custom auth | ❌ Incompatible |
| Real-time | N/A | Hocuspocus + Yjs | ⚠️ Different paradigm |
| Build tool | Next.js / Turbopack | Vite | ⚠️ Different |
| Data fetching | Server components | SWR + Axios | ❌ Different |

### Integration Points

**Real-time collaboration (Hocuspocus + Yjs):**
- Battle-tested pattern for multi-user editing
- Could inspire Mind You's real-time features (if needed)
- Requires separate Node.js service (not compatible with Next.js API routes)

**Domain model (Issue/Cycle/Module/Page):**
- Well-thought-out hierarchy for project management
- Could inform Mind You's data model design
- Not directly reusable (Django models, not TypeScript)

**UI component library (`@plane/ui`):**
- Custom components built on BlueprintJS + Radix UI + Headless UI
- BlueprintJS is a large, opinionated library — not aligned with shadcn/ui
- Radix UI usage is compatible but layered under BlueprintJS

**Editor (`@plane/editor`):**
- TipTap-based (not BlockSuite)
- Yjs integration for collaboration
- Not extractable without significant adapter work

### Compatibility Verdict

| Area | Verdict |
|------|---------|
| React 19 | ✅ Compatible |
| Tailwind CSS 4 | ✅ Compatible |
| Next.js App Router | ❌ No (React Router 7) |
| Supabase Auth | ❌ No integration |
| Radix UI / shadcn | ⚠️ Partial (BlueprintJS dependency) |
| Server Components | ❌ No (React Router 7 SSR) |
| Real-time collaboration | ⚠️ Requires separate service |
| License | ❌ AGPL-3.0 (hard blocker) |

---

## 6. Recommended Technologies and Patterns

### Direct Adoption Candidates

**None.** AGPL-3.0 license makes any code extraction legally toxic for proprietary use.

### Architecture Patterns Worth Studying

| Pattern | Source | Value | Effort to Adopt |
|---------|--------|-------|-----------------|
| **Hocuspocus + Yjs collaboration** | `apps/live/` | Battle-tested real-time editing pattern | 2-3 weeks (separate Node.js service) |
| **Domain model design** | Django models | Issue/Cycle/Module/Page hierarchy as reference | 1-2 days (study only) |
| **Monorepo package organization** | `packages/` | Clean `@plane/*` package structure | 1 day (study only) |
| **Docker Compose infrastructure** | `docker-compose.yml` | 13-service deployment pattern | Reference only |

### Component-Level Reuse

| Component | Reusable? | Notes |
|-----------|-----------|-------|
| `@plane/editor` | ❌ No | TipTap-based, AGPL license |
| `@plane/ui` | ❌ No | BlueprintJS dependency, AGPL license |
| `@plane/shared-state` | ❌ No | MobX-based, AGPL license |
| Hocuspocus server | ⚠️ Pattern only | Study the pattern, don't copy code |
| Django models | ❌ No | Python, not TypeScript |

### Estimated Effort to Adopt

| Pattern | Integration Effort | Maintenance Burden | Recommendation |
|---------|-------------------|-------------------|----------------|
| Hocuspocus + Yjs pattern | Medium (2-3 weeks) | Medium (separate service) | Consider if real-time needed |
| Domain model study | Low (1-2 days) | None | Reference only |
| Monorepo structure | Low (1 day) | None | Reference only |

---

## 7. Not Recommended for Adoption

### Direct Component Integration

1. **`@plane/editor`** — TipTap-based, not BlockSuite. AGPL license prohibits use in proprietary projects.

2. **`@plane/ui`** — Built on BlueprintJS, a large opinionated library. Mind You uses Radix UI + shadcn/ui. Different component philosophy.

3. **`@plane/shared-state`** — MobX-based state management. Mind You uses server-first components with no state library. Different paradigm.

4. **Django backend** — Python/Django REST Framework. Completely different from Next.js App Router. Would require maintaining a separate Python service.

### Architectural Patterns to Avoid

1. **React Router 7 SSR** — Plane uses React Router 7 for SSR, not Next.js App Router. Fundamentally different routing/rendering model.

2. **MobX state management** — Different paradigm from Mind You's server-first approach. Adds cognitive load without clear benefit.

3. **BlueprintJS UI library** — Large, opinionated library. Mind You's shadcn/ui + Radix UI approach is lighter and more flexible.

4. **RabbitMQ + Celery** — Python-based background task processing. Not compatible with Node.js stack.

5. **13-container deployment** — Significant operational overhead. Mind You's Vercel deployment is simpler.

### Reasons Not to Adopt Plane Directly

| Reason | Severity | Detail |
|--------|----------|--------|
| AGPL-3.0 license | 🔴 Critical | Any derivative work must be open-sourced. Hard blocker for proprietary Mind You. |
| Django backend | 🔴 High | Python/Django vs Next.js App Router — fundamentally different architectures |
| React Router 7 | 🔴 High | Not Next.js App Router — different routing/rendering model |
| MobX state | 🟡 Medium | Different paradigm from server-first components |
| BlueprintJS | 🟡 Medium | Large, opinionated library vs lightweight shadcn/ui |
| 13-container deployment | 🟡 Medium | Significant operational overhead |
| TipTap editor | 🟡 Medium | Different from BlockSuite, not extractable |

---

## 8. Areas for Future Reference

### Real-Time Collaboration Pattern

The Hocuspocus + Yjs pattern is battle-tested and well-architectured. If Mind You needs real-time collaborative editing, this pattern is worth studying:

- **Architectural area:** Real-time collaboration service
- **Key components:** Hocuspocus (WebSocket server), Yjs (CRDT), Redis extension (multi-instance sync)
- **Integration approach:** Separate Node.js service, not embedded in Next.js

**Potential use:** If Mind You needs multi-user editing (e.g., collaborative dashboards, shared annotations), the Hocuspocus + Yjs pattern provides a proven foundation. Would require a separate service outside Next.js API routes.

### Domain Model Design

Plane's Issue/Cycle/Module/Page hierarchy is a well-thought-out project management domain model:

- **Architectural area:** Data model for project management
- **Key entities:** Issue (core), Cycle (time-boxed), Module (feature group), Page (documentation)
- **Value:** Reference for designing Mind You's own project/task management features

**Potential use:** If Mind You adds project management features, Plane's domain model provides a mature reference for entity relationships and status workflows.

### Monorepo Package Organization

The `@plane/*` package structure is clean and well-organized:

- **Architectural area:** Monorepo organization and package design
- **Key packages:** `editor`, `ui`, `shared-state`, `services`, `types`, `constants`, `hooks`, `utils`, `i18n`
- **Value:** Reference for structuring shared packages in a monorepo

**Potential use:** If Mind You grows into a monorepo, Plane's package organization provides a clean template.

---

## 9. Verdict

### Do Not Pursue Integration — AGPL License Is a Hard Blocker

**Plane is not a viable integration target for Mind You.**

The AGPL-3.0 license makes any code extraction legally toxic for proprietary use. Even if the tech stack were compatible (it isn't — Django vs Next.js), the license alone disqualifies Plane as a source of reusable components.

**What can be studied (without adopting Plane):**
- Hocuspocus + Yjs collaboration pattern (if real-time features needed)
- Domain model design (if adding project management features)
- Monorepo package organization (if scaling into monorepo)

**Why this is the right decision:**
1. AGPL-3.0 requires open-sourcing any derivative work — incompatible with proprietary Mind You
2. Django backend is fundamentally different from Next.js App Router
3. React Router 7 is not Next.js App Router — different routing/rendering model
4. MobX + BlueprintJS are different paradigms from Mind You's stack
5. 13-container deployment adds significant operational overhead

---

## 10. Relationship to the Final Synthesis

This report provides technical depth on Plane's architecture, domain model, and real-time collaboration patterns — supporting the **license assessment** and **adoption recommendation** sections of the primary synthesis.

**Key contributions to the synthesis:**

| Synthesis Section | This Report's Contribution |
|-------------------|----------------------------|
| License Assessment | AGPL-3.0 analysis and implications (Section 7) |
| Technology Compatibility Matrix | Detailed stack comparison (Section 5) |
| Adoption Recommendation | Component-level reusability assessment (Section 6) |
| Architecture Patterns | Hocuspocus + Yjs pattern study (Section 8) |
| Decision Rationale | Reasons not to adopt (Section 7) |

**This report is supplementary.** The primary decision document remains `third-party-repository-rnd-synthesis.md`.
