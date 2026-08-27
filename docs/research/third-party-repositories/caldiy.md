# Cal.diy — Engineering Report

> **Date:** 2026-07-21
> **Repository:** github.com/calcom/cal.diy
> **Version:** v6.2.0 (web), API v2
> **License:** MIT

---

## 1. What It Is

Cal.diy is the **community-driven, fully open-source scheduling platform** — a fork of Cal.com with all enterprise/commercial code removed. It provides appointment scheduling, calendar integrations, video conferencing, CRM integrations, and payment processing via 150+ modular integrations.

Key characteristics:

- **Full-featured scheduling** — Event types, booking pages, availability windows, round-robin scheduling
- **150+ integrations** — Calendar, video, CRM, payment via app-store architecture
- **Self-hosted deployment** — Docker Compose with 5 services
- **MIT licensed** — No enterprise restrictions, fully open for proprietary use
- **Next.js 16** — App Router + Pages Router hybrid

**What it is NOT:** Cal.diy explicitly warns it is for **personal, non-production use**. It recommends Cal.com's hosted/enterprise product for commercial deployments.

---

## 2. Research Scope

This report evaluates Cal.diy for potential technology adoption, component reuse, or architectural inspiration. Specific areas of interest:

- **App Store pattern** — 150+ modular integrations with standardized interfaces
- **Testing infrastructure** — Vitest + Playwright setup
- **Biome configuration** — Linting and formatting setup
- **AI agent documentation** — `agents/` directory as a model for AI tooling

**Sources consulted:**

| Source | File | Content |
|--------|------|---------|
| `AGENTS.md` | Mind You | Tech stack definition, constraints, coding rules |
| `DASHBOARD-BUILD-SPEC.md` | Mind You | Architecture spec, feature requirements |
| `repository-analysis.md` | Internal research notes (not committed) | Full technical analysis of the repository |
| `third-party-repository-rnd-synthesis.md` | `docs/research/third-party-repositories/` | Cross-repository synthesis and recommendations |

---

## 3. Technology Stack Summary

### Languages

| Language | Usage | Scope |
|----------|-------|-------|
| TypeScript | Frontend, backend, shared packages | Primary (95%+) |
| SQL | Database migrations (Prisma) | Data layer |
| CSS | Tailwind CSS 4 | Styling |

### Frontend Stack

| Technology | Usage | Mind You Equivalent |
|------------|-------|---------------------|
| Next.js 16 | Web application (hybrid App + Pages Router) | Next.js 16 (same) |
| React 18.2 | UI library (pinned, NOT React 19) | React 19 |
| Tailwind CSS 4 | Styling | Tailwind CSS 4 (same) |
| Radix UI | UI primitives | Radix UI / shadcn/ui |
| Lexical | Rich text editor | N/A (evaluating) |
| Jotai | Atomic state management | None (server-first) |
| TanStack React Query 5 | Data fetching | Server components + fetch |
| react-hook-form 7 | Forms | Plain controlled inputs (useState) — no form library |
| Zod 3.25 | Validation | Zod |
| next-auth 4 | Authentication | Supabase Auth |
| next-i18next 15 | Internationalization | None (planned) |

### Backend Stack

| Technology | Usage | Mind You Equivalent |
|------------|-------|---------------------|
| tRPC v11 | Type-safe API (internal) | Next.js Route Handlers |
| NestJS 10 | REST API v2 (external) | N/A |
| Prisma 6.16 | ORM (PostgreSQL) | Supabase client (direct SQL) |
| PostgreSQL | Database | Supabase PostgreSQL |
| Redis | Caching, rate limiting, sessions | Supabase (managed) |
| Bull | Job queues (API v2) | N/A |

### Build & Dev Tools

| Tool | Purpose |
|------|---------|
| Yarn 4.12 | Package manager |
| Turborepo 2.7 | Monorepo build orchestration |
| Biome 2.3 | Linting + formatting (NOT ESLint/Prettier) |
| Vitest 4.1 | Unit testing |
| Playwright 1.57 | E2E testing |
| Husky | Git hooks |
| Changesets | Versioning |

### Infrastructure

| Component | Technology |
|-----------|------------|
| Database | PostgreSQL (via Prisma) |
| Cache | Redis |
| Deployment | Docker Compose (5 services) |
| CI/CD | GitHub Actions |

---

## 4. Architecture Highlights

### Monorepo Structure

```
Cal.diy/
├── apps/
│   ├── web/              # Next.js 16 (App Router + Pages Router)
│   ├── api/v2/           # NestJS 10 REST API (Platform API)
│   └── docs/             # Documentation site
├── packages/
│   ├── prisma/           # Database schema (2851 lines) + migrations
│   ├── trpc/             # tRPC v11 API layer
│   ├── ui/               # Shared UI components (Radix UI + Lexical)
│   ├── features/         # 72 feature modules
│   ├── lib/              # Shared utilities (183 files)
│   ├── app-store/        # 150+ integrations
│   ├── platform/         # Platform SDK (atoms, constants, enums, types)
│   ├── i18n/             # Internationalization
│   ├── embeds/           # Embeddable widget SDK
│   └── ...               # 20+ total packages
```

### Key Architectural Patterns

1. **Dual API layer** — tRPC (internal, type-safe) + NestJS REST API v2 (external, platform)

2. **App Store pattern** — 150+ integrations as modular packages with standardized interfaces:
   - `config.json` (metadata)
   - `api/` (API handlers)
   - `components/` (UI components)
   - `lib/` (business logic)
   - Auto-generated types via `app-store-cli`

3. **Feature collocation** — `packages/features/` contains 72 self-contained feature modules

4. **Hybrid routing** — Next.js App Router (`app/`) + Pages Router (`pages/`) coexist

5. **Prisma as single source of truth** — Schema defines all models, generates Zod types + Kysely types

6. **Embeddable widgets** — `@calcom/embed-core` + `@calcom/embed-react` for embedding scheduling in third-party sites

### Domain Model

From `packages/prisma/schema.prisma` (2851 lines):

```
Organization
├── Team
│   ├── User
│   │   ├── EventType (scheduling configurations)
│   │   │   ├── Booking → Attendee
│   │   │   ├── Schedule → Availability
│   │   │   └── Host → HostGroup (round-robin)
│   │   ├── Credential (integration credentials, encrypted)
│   │   └── Calendar → SelectedCalendar
│   └── Workflow → WorkflowStep (automation)
└── OAuthClient → PlatformOAuthClient (API access)
```

### Data Flow

```
Client (React + Next.js)
  │
  ├── tRPC Client (type-safe API)
  │
  └── tRPC Server (packages/trpc/)
        │
        ├── Prisma ORM (packages/prisma/)
        │
        └── PostgreSQL
```

### Deployment

- **Docker Compose** — 5 services (database, redis, calcom, calcom-api, studio)
- **Cloud platforms** — Vercel, Railway, Northflank, Render, Elestio (one-click deploys)
- **Self-hosted** — Clone + `yarn` + configure `.env` + `yarn dx`

---

## 5. Compatibility with Mind You

### Stack Alignment

| Dimension | Mind You | Cal.diy | Alignment |
|-----------|----------|---------|-----------|
| Framework | Next.js 16 | Next.js 16 | ✅ Same |
| React | 19.2 | 18.2 | ⚠️ Different major |
| TypeScript | 5.4 | 5.9 | ✅ Compatible |
| Styling | Tailwind CSS 4 | Tailwind CSS 4 | ✅ Same |
| UI components | Radix UI / shadcn | Radix UI + custom | ✅ Compatible |
| Database | Supabase (managed) | PostgreSQL + Prisma | ⚠️ Different approach |
| ORM | None (Supabase client) | Prisma 6 | ⚠️ Different approach |
| API | Route Handlers | tRPC + NestJS | ⚠️ Different approach |
| State | None (server-first) | Jotai | ⚠️ Different approach |
| Auth | Supabase Auth | NextAuth.js 4 | ⚠️ Different approach |
| Package manager | npm | Yarn 4.12 | ⚠️ Different |
| Linter | None (planned) | Biome 2.3 | ✅ Cal.diy has it |
| Testing | None (planned) | Vitest + Playwright | ✅ Cal.diy has it |

### Integration Points

**Biome configuration:**
- Drop-in replacement for ESLint + Prettier
- Faster, opinionated linting/formatting
- Directly adaptable to Mind You

**Vitest + Playwright setup:**
- Test infrastructure patterns
- Could establish Mind You's testing foundation

**App Store pattern:**
- Modular integration architecture (150+ packages)
- Could inspire Mind You's data ingestion pipeline as plugin packages
- Standardized interfaces for each data source

**AI agent documentation:**
- `agents/` directory is an excellent model for AI tooling
- Knowledge base, rules, commands, skills structure

**Lexical editor:**
- React-native, SSR-compatible, MIT-licensed
- Alternative to BlockSuite (which is Lit-based, not React)

### Compatibility Verdict

| Area | Verdict |
|------|---------|
| Next.js 16 | ✅ Compatible |
| React 19 | ⚠️ Cal.diy pinned to 18.2 |
| Tailwind CSS 4 | ✅ Compatible |
| Radix UI / shadcn | ✅ Compatible |
| Supabase Auth | ❌ No integration |
| Server Components | ⚠️ Cal.diy uses hybrid routing |
| License | ✅ MIT (fully compatible) |

---

## 6. Recommended Technologies and Patterns

### Direct Adoption Candidates

| Component | Source | Effort | Value |
|-----------|--------|--------|-------|
| **Biome configuration** | `biome.json` | 1 day | High — replaces ESLint + Prettier with faster tooling |
| **Vitest setup** | `vitest.config.ts` | 1-2 days | High — establishes test infrastructure |
| **Playwright setup** | `playwright.config.ts` | 2-3 days | Medium — E2E testing patterns |

### Architecture Patterns Worth Studying

| Pattern | Source | Value | Effort to Adopt |
|---------|--------|-------|-----------------|
| **App Store pattern** | `packages/app-store/` | Modular integration architecture for data pipeline | 1-2 weeks to prototype |
| **AI agent documentation** | `agents/` directory | Model for AI tooling documentation | 1-2 days to adapt |
| **Feature collocation** | `packages/features/` | 72 self-contained feature modules | Reference only |
| **Embeddable widgets** | `packages/embeds/` | Widget SDK for embedding in third-party sites | 1-2 weeks if needed |

### Component-Level Reuse

| Component | Reusable? | Notes |
|-----------|-----------|-------|
| `biome.json` | ✅ Yes | Direct adaptation |
| `vitest.config.ts` | ✅ Yes | Direct adaptation |
| `playwright.config.ts` | ✅ Yes | Direct adaptation |
| `packages/app-store/` | ⚠️ Pattern only | Study the architecture, don't copy code |
| `agents/` directory | ✅ Yes | Adapt documentation structure |
| `@calcom/ui` | ⚠️ Partially | Radix UI compatible, but includes Lexical and custom components |
| Lexical editor | ⚠️ Evaluate | React-native, SSR-compatible, MIT-licensed |

### Estimated Effort to Adopt

| Pattern | Integration Effort | Maintenance Burden | Recommendation |
|---------|-------------------|-------------------|----------------|
| Biome config | Low (1 day) | Low | Adopt now |
| Vitest setup | Low (1-2 days) | Low | Adopt now |
| App Store pattern | Medium (1-2 weeks) | Medium | Study for data pipeline |
| AI agent docs | Low (1-2 days) | Low | Adopt now |
| Lexical editor | Medium (1-2 weeks) | Low | Evaluate if rich text needed |

---

## 7. Not Recommended for Adoption

### Direct Component Integration

1. **tRPC layer** — Mind You uses Next.js Route Handlers, not tRPC. Different API paradigm.

2. **NestJS API v2** — Separate NestJS server is heavy for Mind You's needs. Adds operational complexity.

3. **NextAuth.js 4** — Mind You uses Supabase Auth. Different authentication approach.

4. **Prisma ORM** — Mind You uses Supabase client directly. Adding Prisma introduces another abstraction layer.

5. **Jotai state management** — Mind You uses server-first components with no state library. Different paradigm.

### Architectural Patterns to Avoid

1. **Hybrid routing (App Router + Pages Router)** — Cal.diy mixes both routers. Mind You should commit to App Router only.

2. **Dual API layer (tRPC + NestJS)** — Adds complexity. Mind You's single API layer is sufficient.

3. **Yarn 4.12** — Mind You uses npm. Switching package managers adds friction.

4. **Full scheduling domain model** — Cal.diy's 2851-line Prisma schema is domain-specific. Not relevant to Mind You's data visualization focus.

### Reasons Not to Adopt Cal.diy Directly

| Reason | Severity | Detail |
|--------|----------|--------|
| React 18.2 vs 19 | 🟡 Medium | Cal.diy pinned to React 18.2; Mind You uses React 19. Component sharing may cause issues. |
| Prisma vs Supabase | 🟡 Medium | Different database access patterns. Cannot directly share database code. |
| Hybrid routing | 🟡 Medium | Mix of App Router + Pages Router could create confusion if borrowing patterns. |
| Scope creep | 🟡 Medium | Cal.diy is a full scheduling platform. Much of it is irrelevant to Mind You's needs. |
| Non-production warning | 🟡 Medium | Cal.diy explicitly warns it is for personal, not production use. |

---

## 8. Areas for Future Reference

### App Store Pattern (Modular Integrations)

The app-store architecture with 150+ modular integrations is Cal.diy's most sophisticated architectural achievement. Each integration is a self-contained package with standardized interfaces:

- **Architectural area:** Plugin/integration architecture
- **Key components:** `config.json` (metadata), `api/` (handlers), `components/` (UI), `lib/` (logic)
- **Value:** Directly informs how Mind You could structure its data ingestion pipeline as plugin packages

**Potential use:** If Mind You's data ingestion pipeline grows beyond 4-5 sources, the app-store pattern provides a mature reference for modular, extensible integration architecture. Each data source (HN, RSS, GitHub, etc.) could become a self-contained "app" with standardized interfaces.

### AI Agent Documentation Model

The `agents/` directory is an excellent model for AI coding agent tooling:

- **Architectural area:** AI agent documentation and tooling
- **Key components:** Knowledge base, rules, commands, skills
- **Value:** Reference for structuring Mind You's own AI agent documentation

**Potential use:** If Mind You adds AI agent capabilities, the `agents/` directory structure provides a proven documentation model.

### Feature Collocation Pattern

The `packages/features/` directory contains 72 self-contained feature modules:

- **Architectural area:** Feature module organization
- **Key pattern:** Each feature is a self-contained module with its own components, hooks, and utilities
- **Value:** Reference for structuring Mind You's feature modules as the codebase grows

**Potential use:** If Mind You's feature set expands, the feature collocation pattern provides a clean template for organizing feature-specific code.

### Embeddable Widget SDK

The `@calcom/embed-core` + `@calcom/embed-react` packages demonstrate how to build embeddable widgets:

- **Architectural area:** Widget/embed architecture
- **Key components:** Core SDK, React wrapper, embed snippet
- **Value:** Reference if Mind You needs to embed components in third-party sites

**Potential use:** If Mind You needs to embed dashboards or widgets in external sites, Cal.diy's embed SDK provides a proven pattern.

---

## 9. Verdict

### Adopt Tooling, Study Patterns, Skip Domain Logic

**Cal.diy is the most compatible repository evaluated** — MIT license, Next.js 16, Radix UI, Tailwind CSS 4. The tooling (Biome, Vitest, Playwright) is directly adaptable. The app-store pattern is architecturally valuable for Mind You's data pipeline.

**What to adopt now:**
- Biome configuration (replace ESLint + Prettier)
- Vitest + Playwright setup (establish test infrastructure)
- AI agent documentation structure

**What to study for patterns:**
- App Store architecture (modular integrations for data pipeline)
- Feature collocation (self-contained feature modules)
- Embeddable widget SDK (if embedding needed)

**What to skip:**
- tRPC layer (Mind You uses Route Handlers)
- NestJS API v2 (too heavy)
- NextAuth.js (Mind You uses Supabase Auth)
- Prisma ORM (Mind You uses Supabase client)
- Scheduling domain logic (not relevant)

**Why this is the right decision:**
1. MIT license is fully compatible with proprietary use
2. Next.js 16 + Radix UI + Tailwind CSS 4 alignment is strong
3. Tooling gaps (Biome, Vitest, Playwright) can be filled directly
4. App-store pattern is transferable to data pipeline architecture
5. Domain-specific logic (scheduling) is irrelevant to Mind You's focus

---

## 10. Relationship to the Final Synthesis

This report provides technical depth on Cal.diy's architecture, app-store pattern, and tooling — supporting the **compatibility assessment** and **adoption recommendation** sections of the primary synthesis.

**Key contributions to the synthesis:**

| Synthesis Section | This Report's Contribution |
|-------------------|----------------------------|
| Technology Compatibility Matrix | Detailed stack comparison (Section 5) |
| Adoption Recommendation | Component-level reusability assessment (Section 6) |
| Architecture Patterns | App Store pattern study (Section 8) |
| Tooling Gaps | Biome, Vitest, Playwright adoption (Section 6) |
| Decision Rationale | Reasons to adopt tooling, skip domain logic (Section 9) |

**This report is supplementary.** The primary decision document remains `third-party-repository-rnd-synthesis.md`.
