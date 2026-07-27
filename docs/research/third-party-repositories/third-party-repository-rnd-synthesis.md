# Third-Party Repository R&D Synthesis

**Scope:** AFFiNE/BlockSuite, Plane, Cal.diy
**Purpose:** Engineering recommendation based on completed repository research
**Related Research:** `affine-rnd/`, `plane-rnd/`, `caldiy-rnd/` (6 reports total)
**Status:** Final
**Date:** July 27, 2026

---

## Executive Verdict

**None of the three evaluated repositories should be adopted as a platform for Mind You.** Each has value as a reference architecture or source of isolated patterns, but direct integration is inadvisable for all three.

| Repository | Verdict | One-Line Reason |
|------------|---------|-----------------|
| **AFFiNE** | Study patterns only | NestJS/Vite stack incompatible; BlockSuite's Lit UI incompatible with React 19 App Router |
| **Plane** | Do not use | AGPL-3.0 license is a hard legal blocker; Django backend irreconcilable with Next.js |
| **Cal.diy** | Adopt tooling + study patterns | Closest stack match (Next.js 16, Radix UI, Tailwind 4); Biome/Vitest directly adoptable |

**The only concrete actions from this R&D are: adopt Biome, adopt Vitest, study Cal.diy's app-store integration pattern.**

## Decision Summary

| Decision Area | Recommendation | Basis |
|---------------|----------------|-------|
| Editor integration | No editor needed today | BlockSuite incompatible, TipTap AGPL, Lexical conditional |
| Real-time collaboration | Not needed today | Mind You is single-user; patterns documented for future reference |
| Development tooling | Adopt Biome + Vitest immediately | Proven alongside Next.js 16 in Cal.diy; Mind You has no test infrastructure |
| Ingestion pipeline architecture | Study Cal.diy app-store pattern | Modular interface for 150+ integrations directly informs plugin design |
| Code reuse from any repository | None recommended | Stack mismatches or license blockers prevent direct code adoption |

---

## Research Methodology

Three open-source repositories were evaluated using a consistent set of criteria:

**Repositories:**
- AFFiNE v0.27.0 (MIT license)
- Plane v1.3.1 (AGPL-3.0 license)
- Cal.diy v6.2.0 (MIT license)

**Evaluation criteria applied uniformly:**
1. **License compatibility** with Mind You's project
2. **Tech stack alignment** — framework, React version, CSS, UI library, database, auth
3. **Editor/rich-text viability** — rendering model, SSR support, React compatibility, license
4. **Real-time collaboration patterns** — CRDT, transport, sync model
5. **Adoptable tooling** — linting, testing, build configuration
6. **Architectural transferability** — patterns that could inform Mind You's design without code reuse

**Scope boundaries:** This synthesis covers repository analysis only. Product-level comparison is documented separately in `affine-rnd/research.md`. Deployment and installation guidance is documented in `affine-rnd/installation.md`.

---

## Repository Scorecard

| Dimension | AFFiNE | Plane | Cal.diy |
|-----------|--------|-------|---------|
| License | MIT ✅ | AGPL-3.0 ❌ | MIT ✅ |
| Framework match | NestJS/Vite ❌ | Django/RR7 ❌ | Next.js 16 ✅ |
| React compatibility | 19 ✅ | 19 ✅ | 18.2 ⚠️ |
| UI library match | Lit ❌ | BlueprintJS ❌ | Radix UI ✅ |
| Database compatibility | PostgreSQL ✅ | PostgreSQL ✅ | PostgreSQL ✅ |
| Editor viable | BlockSuite (Lit) ❌ | TipTap (AGPL) ❌ | Lexical ✅ |
| Real-time patterns | Yjs ✅ | Yjs+Hocuspouse ✅ | None ❌ |
| Adoptable tooling | Vitest ✅ | None | Biome ✅, Vitest ✅ |

---

## Key Technology Decisions

### Editor Decision

Three editor engines were evaluated across the three repositories:

- **BlockSuite (AFFiNE):** Rejected. Lit-based Web Components are fundamentally incompatible with React 19 App Router. No SSR support. Integration effort estimated at 14–23 days (embedded) or 19–36 days (headless).
- **TipTap (Plane):** Rejected. AGPL-3.0 license makes it unusable in a proprietary project.
- **Lexical (Cal.diy):** Conditional candidate. React-native, SSR-compatible, MIT-licensed, used at Meta scale. Recommended for evaluation only if Mind You develops rich text editing requirements.

**Conclusion:** No editor is needed for Mind You's current dashboard architecture. Lexical remains the fallback if requirements change.

### Real-Time Collaboration

Both AFFiNE and Plane demonstrate Yjs-based CRDT patterns:

- **AFFiNE** uses y-octo for local-first sync with server authority. Architecture is overkill for Mind You's current single-user model.
- **Plane** uses Hocuspouse (Yjs + WebSocket + Redis) for server-authoritative real-time collaboration. This is the more practical reference pattern.
- **Cal.diy** has no real-time collaboration features.

**Conclusion:** Real-time collaboration is not needed today. If multi-user features are planned, a standalone Node.js WebSocket service inspired by Plane's `apps/live/` architecture is the recommended approach.

### Development Tooling

- **Biome:** Recommended for adoption. Replaces ESLint + Prettier. Proven alongside Next.js 16 in Cal.diy.
- **Vitest:** Recommended for adoption. Establishes test infrastructure where none currently exists in Mind You.
- **Playwright:** Recommended for short-term adoption. Provides E2E testing foundation.
- **App-store integration pattern:** Recommended for study. Cal.diy's modular interface for 150+ self-contained integrations directly informs how Mind You could restructure its ingestion pipeline as plugin packages.

---

## Engineering Recommendations

### Recommended for Adoption

| Item | Source | Rationale |
|------|--------|-----------|
| Biome configuration | Cal.diy | Proven with Next.js 16; replaces ESLint + Prettier with faster, opinionated tooling |
| Vitest setup | Cal.diy | Establishes test infrastructure where none currently exists |
| Playwright configuration | Cal.diy | E2E testing foundation for critical user flows |
| App-store integration pattern | Cal.diy `packages/app-store/` | Modular interface for 150+ integrations directly informs ingestion pipeline plugin architecture |

### Recommended for Future Evaluation

| Item | Source | Condition |
|------|--------|-----------|
| Lexical editor | Cal.diy `@calcom/ui` | If Mind You develops rich text editing requirements |
| Hocuspouse + Yjs pattern | Plane `apps/live/` | If Mind You needs real-time multi-user collaboration |
| BlockSuite store/sync packages | AFFiNE `@blocksuite/store`, `@blocksuite/sync` | If Mind You needs offline-first or CRDT-based content management |

### Not Recommended Based on Current Findings

| Item | Source | Reason |
|------|--------|--------|
| AFFiNE as platform | AFFiNE | NestJS/Vite stack incompatible with Next.js 16 App Router |
| BlockSuite editor | AFFiNE | Lit vs React mismatch, no SSR, high integration cost (14–36 days across two approaches) |
| Any Plane code | Plane | AGPL-3.0 license requires open-sourcing derivative work |
| Plane domain model | Plane | Django-specific patterns not transferable to Node.js/Next.js |

---

## Potential Future Investigations

The following are optional follow-up research areas. They are not recommended actions — they are documented paths to pursue only if project requirements change.

- **Lexical editor evaluation** — If Mind You needs rich text editing (e.g., for notes, documentation, or prompt editing), evaluate Lexical as a React-native, SSR-compatible, MIT-licensed editor. Study `@calcom/ui` from Cal.diy as a reference implementation.

- **Ingestion pipeline redesign** — If the current `src/ingesters/` architecture needs modularization, study Cal.diy's `packages/app-store/` pattern as a reference architecture for self-contained plugin packages with standardized interfaces.

- **Real-time collaboration** — If multi-user features are planned, study Plane's `apps/live/` (Hocuspouse + Yjs) and AFFiNE's `@blocksuite/sync` for CRDT patterns. A standalone Node.js WebSocket service is the recommended implementation approach.

- **Offline-first content** — If offline capability is needed, study `@blocksuite/store` and `@blocksuite/sync` for CRDT document model patterns. Do not adopt BlockSuite itself — study the patterns in isolation.

---

## Open Questions

The following unresolved questions influence whether future investigations are pursued:

1. **Does Mind You need a rich text editor?** The dashboard is a data visualization and ingestion tool. If editing is limited to forms and search, no editor evaluation is needed.

2. **Does Mind You need real-time collaboration?** The current architecture is single-user. If multi-user features are not planned, CRDT and sync pattern research is academic.

3. **What is the deployment model?** Self-hosted (Docker) versus Vercel/cloud affects which architectural patterns are relevant. Cal.diy supports both; AFFiNE and Plane are self-hosted only.

4. **Is Mind You proprietary or open-source?** If proprietary, AGPL-licensed code (Plane) is a legal risk. If open-source, more options become available.

---

## Research Limitations

This synthesis has the following limitations:

- **Scope is limited to repository analysis.** Product-level comparison, market positioning, and community activity were evaluated separately for AFFiNE only (`affine-rnd/research.md`).
- **No runtime testing was performed.** All evaluations are based on source code analysis, package.json inspection, and documentation review. No repositories were installed, built, or executed.
- **Version-specific findings.** Results apply to the specific versions evaluated (AFFiNE v0.27.0, Plane v1.3.1, Cal.diy v6.2.0). Newer versions may change the assessment.
- **Integration effort estimates are directional.** Effort ranges (e.g., BlockSuite integration at 14–36 days across two evaluated approaches) are based on code complexity analysis, not hands-on implementation.
- **Lexical was not deeply evaluated.** Cal.diy uses Lexical, but a dedicated BlockSuite-level evaluation was not performed. Lexical is recommended for future evaluation, not adoption.

---

## Supporting Research

The following reports were used to produce this synthesis:

**AFFiNE / BlockSuite:**
- `affine-rnd/repository-analysis.md` — Full code-level analysis of AFFiNE's architecture, tech stack, and integration feasibility
- `affine-rnd/blocksuite-evaluation.md` — Deep evaluation of BlockSuite's editor engine, CRDT model, and React compatibility
- `affine-rnd/research.md` — Product-level comparison of AFFiNE's features against Mind You's dashboard
- `affine-rnd/installation.md` — Evaluation of AFFiNE deployment methods

**Plane:**
- `plane-rnd/repository-analysis.md` — Full code-level analysis of Plane's architecture, domain model, and license implications

**Cal.diy:**
- `caldiy-rnd/repository-analysis.md` — Full code-level analysis of Cal.diy's architecture, app-store pattern, and tooling
