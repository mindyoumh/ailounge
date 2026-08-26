# Planning Document Review — Overlap, Priority, & Consolidation

**Date:** 2026-06-22
**Documents Reviewed:**
| # | File | Lines | Role |
|---|------|-------|------|
| A | Engineering Intelligence Expansion Plan (archived, then deleted 2026-08-26 — unique content captured in §4-5 below) | 250 | Strategic "what" — original plan from Sir Bo |
| B | `docs/plans/engineering-intelligence-implementation-roadmap.md` | 286 | Phased "when" — execution order, complexity, issues |
| C | `docs/plans/engineering-intelligence-execution-plan.md` | 284 | Tactical "how" — exact files, lines, changes for a developer |

---

## 1. Overlap Analysis

### Content Appearing in All Three Documents

| Content | Doc A (§) | Doc B (§) | Doc C (§) | Notes |
|---------|-----------|-----------|-----------|-------|
| RSS sources table (URLs, categories, feed files) | Phase 1 source table | Phase 1 sources | Files to Modify §1 | Doc A has wrong URLs; B and C are corrected |
| FILE_CATEGORY_MAP additions | Phase 1 recommended additions | Phase 0 tasks | Files to Modify §3 | Identical code blocks in all three |
| New feed file creation (09, 11) | Phase 1 recommended additions | Phase 0 tasks | Files to Modify §2 | Same 2 files, same purpose. Startup feed (10) was later removed as redundant (covered by HN) |
| Risk tables | Risks & Mitigations | Key Risks & Mitigations | Risks | Each has different focus and entries |

### Content Appearing in Two of Three Documents

| Content | Doc A | Doc B | Doc C | Primary Holder |
|---------|-------|-------|-------|---------------|
| URL corrections vs. original plan | ❌ (has wrong URLs) | ✅ Corrections table | ✅ Corrections list | B (most detailed) |
| GitHub Trending correction | ❌ ("already covered") | ✅ § GitHub Trending | ✅ ¶4 | B (full explanation) |
| Docker infrastructure | ✅ Phase 2 (design code) | ✅ Phase 2 (tasks) | ❌ (excluded) | A (design), B (tasks) |
| Intern tasks | ✅ Phase 3 (8 tasks) | ✅ Phase 4 (8 tasks) | ❌ (excluded) | A (code blocks), B (priorities) |
| Feed rotation | ✅ Phase 4 (2 options) | ✅ Phase 5 (deferred) | ❌ (excluded) | A (options analysis) |
| GitHub Issues | ❌ | ✅ 10 issues with labels | ❌ | B (unique) |
| Phase labels | ❌ | ✅ 6 labels defined | ❌ | B (unique) |
| Complexity/effort summary | ❌ | ✅ per-phase table | ✅ summary table | B (more detailed) |
| CVE/NVD architecture | ❌ (trivial RSS) | ✅ Phase 3 (detailed) | ❌ (mentions cvefeed.io) | B (unique) |

### Content Unique to One Document

| Content | Doc | Why It's Unique |
|---------|-----|-----------------|
| Background / Sir Bo's request | A §1-8 | Strategic context for the entire effort |
| Current architecture reference | A §9-18 | System overview table (ingesters, DB, UI) |
| Dockerfile design code | A §108-120 | Full dockerfile and docker-compose.yml |
| Feed rotation options analysis | A §206-221 | Option A vs B with pros/cons/recommendation |
| Open questions | A §244-250 | 5 unresolved questions about scope/design |
| Priority-ordered source rationale | B §71-86 | Why feed #1 is higher priority than feed #9 |
| NVD API details (endpoint, params, rate limits) | B §153-162 | Technical reference for the NVD ingester |
| Official vs. third-party CVE decision table | B §164-172 | Option A/B/C comparison with recommendation |
| GitHub Issues (10) | B §237-250 | Ticketable work items with labels |
| Phase labels | B §254-263 | Labels for organizing work |
| "How to Use This Roadmap" guide | B §280-286 | Reader guidance |
| Architecture summary (free-form strings) | C §12-17 | Codebase design constraint explanation |
| Per-file analysis of what NOT to change | C §114-134 | schema.ts, api/feed/route.ts, analytics.ts |
| UI impact analysis (table form) | C §160-181 | Per-element: filter dropdown, badges, sections |
| Dashboard sections decision analysis | C §88-112 | Whether to add DevOps homepage section (startup was later removed as redundant) |
| Trending Repos query impact | C §126-134 | New RSS items won't appear in existing section |
| Source-to-file mapping cheat sheet | C §249-264 | Single table: source → URL → category → file → DB source |
| Changes summary table (file → type → lines) | C §268-283 | Exact scope for implementation planning |

### Notable Gap

The **CATEGORIES array update** in `app/feed/page.tsx` (adding `devops`) appears **only in Doc C** (§67-86). Neither Doc A nor Doc B identifies this required change. This is a significant omission in A and B. _(Note: the original Doc C also included `startup`, which was later removed as redundant — Y Combinator content is already covered by the HN ingester.)_

---

## 2. Which Document Should Be Primary

**Recommendation: Doc B (`implementation-roadmap.md`) should be the primary planning document.**

| Criterion | A (Expansion Plan) | B (Roadmap) | C (Execution Plan) |
|-----------|-------------------|-------------|-------------------|
| Has accurate URLs | ❌ (3 errors) | ✅ (corrected) | ✅ (corrected) |
| Has correct GitHub trending assessment | ❌ ("already covered") | ✅ (corrected) | ✅ (notes correction) |
| Covers all expansion areas | ✅ (4 phases) | ✅ (6 phases, more granular) | ❌ (RSS only) |
| Has implementation order | ✅ (high-level) | ✅ (detailed phases with deps) | ✅ (step-by-step numbered) |
| Includes effort estimates | ❌ | ✅ (per-phase) | ✅ (per-file) |
| Includes CI/task tracking | ❌ | ✅ (GitHub Issues, labels) | ❌ |
| Contains no errors after validation | ❌ | ✅ | ✅ |
| Audience | Strategy | Lead/Manager | Developer |

Doc B is the only one that is:
- **Correct** (incorporates all validation findings)
- **Comprehensive** (covers RSS, Docker, NVD, intern tasks, bonus)
- **Actionable** (has phases, issues, priorities, labels, risks)
- **Cross-referencing** (explicitly links to A and the validation research)

---

## 3. Should the Execution Plan Be Merged Into the Roadmap?

**Recommendation: Do NOT merge. Keep as a separate document.**

### Reasons to Not Merge

| Factor | Detail |
|--------|--------|
| **Different audience** | Roadmap = lead/manager deciding what to do. Execution Plan = developer needing exact line numbers and code blocks. |
| **Different level of detail** | Roadmap says "add entries to RSS_FEEDS." Execution Plan says "add these exact 10 lines, after the cloud block, grouped by comment blocks." |
| **Excluded content** | Execution Plan's unique content (UI impact analysis, source-to-file cheat sheet, what-NOT-to-change analysis) would bloat the Roadmap beyond its intended scope. |
| **Already cross-referenced** | Execution Plan lists the Roadmap as prerequisite reading. The two are designed as a chain, not a merge. |
| **One has been used already** | Execution Plan was implemented directly (Steps 1-4). It proved its value as a standalone developer spec. |

### What Should Change

The Roadmap should add a brief reference to the Execution Plan for implementation-level detail:

> **For exact file changes, line numbers, and code blocks, see the [Execution Plan](./engineering-intelligence-execution-plan.md), Steps 1-7.**

This removes the need for the Roadmap to duplicate the RSS_FEEDS entries, FILE_CATEGORY_MAP code, or CATEGORIES array changes, while still directing the reader to where they are.

---

## 4. Can Any Document Be Safely Removed?

### A: Expansion Plan (archived, then deleted 2026-08-26)

**Can be archived, but not deleted.**

| Content | Status | Fate |
|---------|--------|------|
| Background / Sir Bo's context | Unique to A | Move to Roadmap §1 |
| Current architecture reference | Unique to A | Move to Roadmap as subsection |
| Source analysis (with errors) | Superseded by B and C | Remove |
| Recommended RSS_FEEDS (with errors) | Superseded by C | Remove |
| Dockerfile design code | Unique to A | Move to a new `docs/plans/engineering/docker-design.md` or into Roadmap Phase 2 |
| Intern tasks code blocks | Unique to A | Move to Roadmap Phase 4 (B has task list but no code blocks) |
| Feed rotation options | Unique to A | Move to Roadmap Phase 5 |
| Open questions (5) | Unique to A | Move to Roadmap as "Open Questions" section |
| Risks | A has "CVE feed too noisy" and "New category not filterable" that B and C don't replicate | Merge unique entries into Roadmap risks table |

**Total unique content in A that is not in B or C:**
- Background context (~8 lines)
- Architecture reference (~10 lines)
- Dockerfile/docker-compose code (~30 lines)
- Intern tasks code blocks (~50 lines)
- Feed rotation options analysis (~20 lines)
- Open questions (~7 lines)
- 2 unique risk entries

**Verdict:** A can be **archived** (moved out of `docs/plans/`) after its unique content is migrated into the Roadmap. The migration effort is small (~125 lines of unique content, mostly code blocks that can be copy-pasted).

### B: Roadmap (`implementation-roadmap.md`)

**Keep as primary.** No other document covers GitHub Issues, phase labels, priority ordering, or the NVD API reference.

### C: Execution Plan (`execution-plan.md`)

**Keep as developer reference.** No other document covers the per-file what-NOT-to-change analysis, UI impact tables, source-to-file cheat sheet, or exact line-level change specifications.

### Summary of Proposed Consolidation

| Document | Action | Rationale |
|----------|--------|-----------|
| `expansion-plan.md` | **Archive after migration** | Superseded by B+C; unique content can be absorbed into B |
| `implementation-roadmap.md` | **Primary document** | Keep as-is, optionally absorb unique content from A |
| `execution-plan.md` | **Keep separate** | Different purpose and audience than B |

---

## 5. Migration Plan (If Accepted)

If the Expansion Plan is archived, the following unique content should be migrated to the Roadmap:

1. Add "Background" section to Roadmap (from A §1-8)
2. Add "Current Architecture" reference table to Roadmap (from A §9-18)
3. Add intern tasks code blocks to Roadmap Phase 4 (from A §157-199) — B currently only has the task list table, not the code
4. Add Dockerfile/docker-compose design code to Roadmap Phase 2 (from A §108-136)
5. Add feed rotation analysis to Roadmap Phase 5 (from A §206-221)
6. Add "Open Questions" section to Roadmap (from A §244-250)
7. Merge 2 unique risk entries into Roadmap risks table

After migration, `expansion-plan.md` was moved into an archive folder and has since been deleted outright (2026-08-26 doc-archive cleanup) — its unique content remains captured in the migration plan above.
