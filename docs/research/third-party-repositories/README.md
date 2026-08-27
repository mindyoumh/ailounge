# Third-Party Repository R&D

Engineering evaluation of three open-source repositories for the Mind You project.

**Scope:** AFFiNE/BlockSuite, Plane, Cal.diy
**Date:** 2026-07-21 – 2026-07-27
**Status:** Complete

---

## Overview

This documentation set presents the findings of a structured research and development effort evaluating three open-source repositories for potential integration with, adoption by, or architectural inspiration for Mind You.

The evaluation assessed each repository against Mind You's technology stack (Next.js 16, React 19, Radix UI, Supabase PostgreSQL) and identified which technologies, patterns, and tooling are recommended for adoption or further study.

The primary outcome is an engineering decision: **none of the three repositories should be adopted as a platform.** Two items from Cal.diy are recommended for immediate adoption (Biome, Vitest), and one architectural pattern is recommended for study (app-store integration).

---

## Documentation Structure

```
README.md (you are here)
    │
    ├── third-party-repository-rnd-synthesis.md
    │       Primary engineering decision document.
    │       Contains: verdicts, recommendations, rationale.
    │
    ├── affine.md
    │       Detailed evaluation of AFFiNE v0.27.0.
    │       Supports the synthesis conclusions on AFFiNE/BlockSuite.
    │
    ├── plane.md
    │       Detailed evaluation of Plane v1.3.1.
    │       Supports the synthesis conclusions on Plane.
    │
    └── caldiy.md
            Detailed evaluation of Cal.diy v6.2.0.
            Supports the synthesis conclusions on Cal.diy.
```

Start with the synthesis for decisions. Read the individual reports for technical depth.

---

## Start Here

| If you want to... | Read this |
|---------------------|-----------|
| Understand the final recommendation | [Synthesis](third-party-repository-rnd-synthesis.md) — Executive Verdict + Decision Summary |
| Compare all three repositories | [Synthesis](third-party-repository-rnd-synthesis.md) — Repository Scorecard |
| Understand why AFFiNE/BlockSuite doesn't fit | [affine.md](affine.md) |
| Study Plane's real-time collaboration pattern | [plane.md](plane.md) |
| Learn what to adopt from Cal.diy | [caldiy.md](caldiy.md) |

---

## Document Index

| Document | Description |
|----------|-------------|
| [third-party-repository-rnd-synthesis.md](third-party-repository-rnd-synthesis.md) | Primary engineering decision document — verdicts, recommendations, and rationale |
| [affine.md](affine.md) | Technical evaluation of AFFiNE v0.27.0 and BlockSuite editor engine |
| [plane.md](plane.md) | Technical evaluation of Plane v1.3.1 and its real-time collaboration architecture |
| [caldiy.md](caldiy.md) | Technical evaluation of Cal.diy v6.2.0 and its integration architecture |

---

## Repositories Evaluated

| Repository | Version | License | Verdict |
|------------|---------|---------|---------|
| AFFiNE | v0.27.0 | MIT (client) / EE (backend server) | Study patterns only |
| Plane | v1.3.1 | AGPL-3.0 | Do not use |
| Cal.diy | v6.2.0 | MIT | Adopt tooling + study patterns |

---

## Scope and Limitations

This documentation set covers repository-level analysis only. It does not include runtime testing, performance benchmarking, security auditing, or production deployment evaluation. All findings are version-specific and based on source code analysis, dependency inspection, and documentation review conducted in July 2026.

The synthesis is the primary engineering decision document. The individual repository reports provide supporting technical detail. For verdicts, recommendations, and cross-repository comparison, refer to the synthesis.
