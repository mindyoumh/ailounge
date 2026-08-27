# AFFiNE — Engineering Report

> **Date:** 2026-07-21
> **Repository:** github.com/toeverything/AFFiNE
> **Version:** v0.27.0 (canary)
> **License:** Mixed — MIT for most of the tree (client, BlockSuite, desktop); `packages/backend` and `packages/common/native` are under AFFiNE's proprietary Enterprise Edition (EE) license; CE client-side assets are MPL-2.0

---

## 1. What It Is

AFFiNE is an **open-source, all-in-one workspace** that combines documents, whiteboards, and databases into a single interface. It positions itself as a **privacy-focused, local-first, open-source alternative to Notion and Miro**.

Key characteristics:

- **Hyper-fused editor** — Docs, canvas, and tables are merged (not separate apps)
- **Local-first** — Data lives on your disk first; cloud sync is optional
- **Real-time collaboration** — Multi-user editing via CRDTs
- **Self-hostable** — Docker-based deployment with full feature parity
- **AI integration** — Built-in multimodal AI partner (summarization, mind maps, code generation)
- **Cross-platform** — Web, Desktop (Electron), iOS, Android

**What it is NOT:** AFFiNE is not a general-purpose application framework. It is a standalone product that happens to expose reusable subsystems (BlockSuite, CRDT sync) as separate packages.

---

## 2. Research Scope

This report evaluates AFFiNE for potential technology adoption, component reuse, or architectural inspiration. Specific areas of interest:

- **BlockSuite** — The open-source collaborative editor engine embedded as a workspace member
- **CRDT sync** — The y-octo/Yjs-based local-first synchronization layer
- **Deployment model** — Docker Compose self-hosted deployment
- **Backend patterns** — NestJS + Apollo Server + Prisma architecture

**Sources consulted:**

| Source | File | Content |
|--------|------|---------|
| `AGENTS.md` | Mind You | Tech stack definition, constraints, coding rules |
| `DASHBOARD-BUILD-SPEC.md` | Mind You | Architecture spec, feature requirements |
| `repository-analysis.md` | Internal research notes (not committed) | Full technical analysis of the repository |
| `blocksuite-evaluation.md` | Internal research notes (not committed) | Deep evaluation of BlockSuite editor engine |
| `installation.md` | Internal research notes (not committed) | Deployment method comparison |
| `research.md` | Internal research notes (not committed) | Product comparison (Notion/Miro alternatives) |

---

## 3. Tech Stack Summary

### Languages

| Language | Usage | Scope |
|----------|-------|-------|
| TypeScript | Frontend, server, build tools | Primary (90%+) |
| Rust | Native modules (CRDT, storage, crypto) | Performance-critical paths |
| SQL | Database migrations | Prisma schema |

### Frameworks & Libraries

| Technology | Usage | Mind You Equivalent |
|------------|-------|---------------------|
| React 19 | Frontend UI | React 19 (same) |
| NestJS | Backend server | Next.js App Router |
| Apollo Server v5 | GraphQL API | REST (tRPC planned) |
| BlockSuite | Collaborative editor engine | N/A |
| Jotai | State management | N/A (no state mgmt yet) |
| Prisma | ORM (PostgreSQL) | Supabase (direct SQL) |
| BullMQ | Job queue (Redis-backed) | N/A |
| yjs / y-octo | CRDT sync | N/A |
| Electron | Desktop shell | Web-only |

### Build & Dev Tools

| Tool | Purpose |
|------|---------|
| Yarn 4 (Berry) | Package manager |
| Vite | Frontend bundler |
| oxlint | Rust-based linter |
| Vitest | Unit testing |
| Playwright | E2E testing |
| Node.js >=22.12.0 | Runtime |

### Infrastructure

| Component | Technology |
|-----------|------------|
| Database | PostgreSQL 16 (with pgvector) |
| Cache / Queue | Redis |
| Blob Storage | S3-compatible |
| Deployment | Docker Compose |
| CI/CD | GitHub Actions |

---

## 4. Architecture Highlights

### Monorepo Structure

```
AFFiNE/
├── blocksuite/          # BlockSuite — editor engine (separate project)
│   ├── affine/          #   AFFiNE-specific blocks, widgets
│   ├── framework/       #   Core: store, sync, std, global
│   └── playground/      #   Development playground
├── packages/
│   ├── backend/
│   │   ├── native/      #   Rust native modules (NAPI-RS)
│   │   └── server/      #   NestJS backend server
│   ├── common/
│   │   ├── auth/        #   Authentication utilities
│   │   ├── nbstore/     #   CRDT document storage
│   │   ├── realtime/    #   Real-time sync protocols
│   │   └── s3-compat/   #   S3-compatible blob storage
│   └── frontend/
│       └── apps/
│           ├── web/     #   React web app (Vite)
│           └── electron/#   Desktop shell (Electron)
```

### Architectural Patterns

1. **Local-first via CRDT** — Edits happen locally, sync when online. y-octo (Rust) provides fast binary CRDT operations; Yjs (JS) provides compatibility layer.

2. **Server-authoritative** — Collaboration, billing, and access control are managed server-side. Local data is eventually consistent.

3. **Block-based document model** — BlockSuite defines a composable block system where everything (paragraphs, lists, tables, images, whiteboard elements) is a block.

4. **Native Rust modules** — Performance-critical operations (CRDT, storage, crypto) are compiled via NAPI-RS for Node.js bindings.

5. **1159+ line Prisma schema** — 30+ database models covering users, workspaces, docs, blobs, permissions, billing, and notifications.

### Data Flow

```
Client (React + BlockSuite)
  │
  ├── Local storage (IndexedDB / SQLite)
  │
  ├── CRDT sync (y-octo/Yjs)
  │     │
  │     └── Server (NestJS)
  │           ├── PostgreSQL (structured data)
  │           ├── Redis (cache, queues)
  │           └── S3 (blob storage)
  │
  └── WebSocket (Socket.io)
        └── Real-time collaboration
```

### Deployment

- **Docker Compose** — Server + PostgreSQL + Redis (minimal setup, ~15 minutes)
- **Cloud-hosted** — AFFiNE.pro managed service
- **Self-hosted with custom infra** — Kubernetes, bare metal, etc.

---

## 5. Compatibility with Mind You

### Stack Alignment

| Dimension | Mind You | AFFiNE | Alignment |
|-----------|----------|--------|-----------|
| Frontend framework | React 19 | React 19 | ✅ Same |
| Styling | Tailwind CSS 4 | Custom CSS | ⚠️ Differs |
| UI components | Radix UI / shadcn | Custom (Lit) | ⚠️ Differs |
| Backend | Next.js App Router | NestJS | ❌ Different |
| Database | Supabase (managed PostgreSQL) | PostgreSQL (self-managed) | ⚠️ Same DB, different access |
| Auth | Supabase Auth | Custom session mgmt | ❌ Incompatible |
| ORM | Direct SQL / Supabase client | Prisma | ❌ Different |
| State management | None (server-first) | Jotai | ⚠️ Different |
| Build tool | Next.js / Turbopack | Vite | ⚠️ Different |

### Integration Points

**BlockSuite** (editor engine):
- Framework-agnostic core (`@blocksuite/store`, `@blocksuite/sync`) — no React dependency
- UI is **Lit-based**, not React — requires wrapper or headless approach
- **No SSR support** — cannot be used in Next.js Server Components
- CRDT sync engine has pluggable `DocSource` interface — could theoretically integrate with Supabase

**CRDT sync layer**:
- y-octo (Rust) + Yjs (JS) provide local-first data synchronization
- `@blocksuite/sync` package is generic — not coupled to AFFiNE's data model
- Binary format (Yjs updates) doesn't map cleanly to PostgreSQL rows

**Deployment**:
- Docker Compose setup is well-documented and production-ready
- Could serve as reference for Mind You's own deployment (if moving beyond Vercel)

### Compatibility Verdict

| Area | Verdict |
|------|---------|
| React 19 | ✅ Compatible |
| Next.js App Router | ❌ No direct use (NestJS backend) |
| Supabase Auth | ❌ No integration |
| Tailwind CSS | ⚠️ Possible with wrapper |
| shadcn/ui | ⚠️ Possible with wrapper |
| Server Components | ❌ No SSR in BlockSuite |

---

## 6. Recommended Technologies and Patterns

### Direct Adoption Candidates

**None at this time.** AFFiNE's architecture is incompatible with Mind You's Next.js App Router + Supabase stack.

### Architecture Patterns Worth Studying

| Pattern | Source | Value | Effort to Adopt |
|---------|--------|-------|-----------------|
| **Local-first CRDT sync** | BlockSuite sync engine | Enables offline-capable features, reduces server load | 2-3 weeks to prototype with Supabase |
| **Block-based document model** | BlockSuite store | Flexible content structure, extensible blocks | 2-3 weeks headless mode (no Lit UI) |
| **Pluggable DocSource interface** | `@blocksuite/sync` | Abstracts storage backend — could implement for Supabase | 1-2 weeks |
| **Docker Compose deployment** | `deploy/` directory | Reference for self-hosted deployment beyond Vercel | 1-2 days to configure |
| **NestJS module organization** | `packages/backend/server/` | Backend architectural patterns (if migrating from Next.js API routes) | Only if paradigm shift planned |

Note: `packages/backend/server/` is EE-licensed, not MIT — its code must not be copied, only its patterns studied.

### Component-Level Reuse

| Component | Reusable? | Notes |
|-----------|-----------|-------|
| `@blocksuite/store` | ⚠️ Partially | Document model is generic; API surface is well-defined |
| `@blocksuite/sync` | ⚠️ Partially | CRDT sync is pluggable; requires custom `DocSource` |
| `@blocksuite/std` | ❌ No | Tightly coupled to BlockSuite's block system |
| `@blocksuite/affine-*` | ❌ No | AFFiNE-specific blocks — not reusable outside AFFiNE |
| NestJS server | ❌ No | Completely different paradigm from Next.js |
| Prisma schema | ⚠️ Reference only | 1159+ lines — useful as schema design reference, not directly usable |

### Estimated Effort to Adopt

| Pattern | Integration Effort | Maintenance Burden | Recommendation |
|---------|-------------------|-------------------|----------------|
| CRDT sync (headless) | Medium (19-36 days) | High (ongoing) | Consider only if offline-first required |
| Block-based document model | Medium (14-23 days embedded, 19-36 days headless) | High | Not aligned with current needs |
| Docker Compose deployment | Low (1-2 days) | Low | Reference only |
| NestJS patterns | High (3-5 weeks) | Medium | Only if migrating from Next.js |

---

## 7. Not Recommended for Adoption

### Direct Component Integration

1. **BlockSuite UI** — Lit-based Web Components, not React. Embedding requires wrapper layer, breaks Server Components, adds Shadow DOM styling conflicts with Tailwind.

2. **NestJS backend** — Fundamentally different paradigm from Next.js App Router. Mind You's server-first, API routes pattern is simpler and more appropriate for the current scale.

3. **Prisma ORM** — Mind You uses Supabase client with direct SQL. Adding Prisma introduces another abstraction layer without clear benefit.

4. **Custom auth system** — AFFiNE's session management is incompatible with Supabase Auth. No migration path without significant rewrite.

### Architectural Patterns to Avoid

1. **Monorepo with 100+ packages** — AFFiNE's scale is appropriate for a product company; Mind You is a focused application. Keep it simple.

2. **Rust native modules** — Performance benefits are real but add build complexity (NAPI-RS, cargo, Rust toolchain). Not justified for Mind You's current needs.

3. **GraphQL API** — AFFiNE uses Apollo Server v5. Mind You's REST/tRPC pattern is simpler and sufficient.

4. **Electron desktop shell** — Mind You is web-only. Desktop deployment adds significant maintenance burden.

### Reasons Not to Adopt AFFiNE Directly

| Reason | Severity | Detail |
|--------|----------|--------|
| Tech stack mismatch | 🔴 High | NestJS + Vite vs Next.js App Router — fundamentally different architectures |
| Auth incompatibility | 🔴 High | Custom session management doesn't integrate with Supabase Auth |
| No SSR support | 🔴 High | BlockSuite is client-only; cannot be used in Next.js Server Components |
| Complexity overhead | 🟡 Medium | 100+ packages, Rust + TypeScript + SQL, Yarn 4 monorepo |
| Documentation gap | 🟡 Medium | No architecture docs — understanding requires deep source code reading |
| Bundle size | 🟡 Medium | Full AFFiNE deployment is heavy; extracting components is non-trivial |

---

## 8. Areas for Future Reference

### CRDT Sync Layer

The `@blocksuite/sync` package provides a **pluggable `DocSource` interface** for abstracting storage backends. This is architecturally interesting for Mind You if offline-first or collaborative features become necessary.

- **Architectural area:** Collaborative data synchronization
- **Key interface:** `DocSource` — abstracts document persistence (could implement for Supabase)
- **CRDT format:** Yjs binary updates (not JSON/SQL-friendly)

**Potential use:** If Mind You needs offline-capable features (e.g., offline editing with later sync), the CRDT pattern from y-octo/Yjs is worth studying. The `DocSource` interface could be implemented against Supabase's Realtime capabilities.

### Block-Based Document Model

BlockSuite's block system is a well-designed pattern for composable, extensible content structures. Even without adopting the library, the architectural patterns are valuable reference:

- **Architectural area:** Content structure and block composition
- **Key concept:** `defineBlockSchema` — declarative block type definition
- **Extensibility:** DI container for store and view extensions

**Potential use:** If Mind You needs to support custom content types (beyond simple key-value data), BlockSuite's block schema pattern is a good reference for designing extensible content structures.

### Docker Compose Deployment

AFFiNE's Docker Compose setup is well-documented and production-ready. Could serve as reference for Mind You's own deployment configuration if moving beyond Vercel.

- **Architectural area:** Container-based deployment and orchestration

### AI Copilot Integration

AFFiNE's built-in AI partner (multimodal, summarization, mind maps) demonstrates patterns for AI integration in productivity tools. However, this is tightly coupled to AFFiNE's block model and not extractable.

- **Architectural area:** AI feature integration patterns

---

## 9. Verdict

### Do Not Pursue Integration at This Time

**AFFiNE is not a viable integration target for Mind You.**

The tech stack (NestJS + Vite + custom auth) is fundamentally incompatible with Next.js 16 App Router + Supabase. BlockSuite's Lit-based UI cannot be embedded in a React 19 application without significant wrapper work that would break Server Components.

**What can be studied (without adopting AFFiNE):**
- CRDT sync patterns from `@blocksuite/sync` (if offline-first becomes necessary)
- Block schema patterns from `@blocksuite/store` (if custom content types needed)
- Docker Compose deployment configuration (as deployment reference)

**Why this is the right decision:**
1. Mind You doesn't need a full document editor — it's a data visualization/ingestion tool
2. The integration effort (14-36 days) is not justified by the value delivered
3. The maintenance burden (Rust toolchain, Yarn 4, NestJS) adds complexity Mind You doesn't need
4. Supabase Auth incompatibility is a fundamental blocker

---

## 10. Relationship to the Final Synthesis

This report provides technical depth on AFFiNE's architecture, BlockSuite's editor engine, and CRDT sync patterns — supporting the **compatibility assessment** and **adoption recommendation** sections of the primary synthesis.

**Key contributions to the synthesis:**

| Synthesis Section | This Report's Contribution |
|-------------------|----------------------------|
| Technology Compatibility Matrix | Detailed stack comparison (Section 5) |
| Adoption Recommendation | Component-level reusability assessment (Section 6) |
| Effort Estimates | Integration effort by pattern type (Section 6, table) |
| Architecture Patterns | BlockSuite patterns as reference (Section 8) |
| Decision Rationale | Reasons not to adopt (Section 7) |

**This report is supplementary.** The primary decision document remains `third-party-repository-rnd-synthesis.md`.
