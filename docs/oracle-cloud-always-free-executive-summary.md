# Executive Summary: Oracle Cloud Always Free Evaluation

## Overview

This document summarizes the findings of a comprehensive engineering evaluation of Oracle Cloud Infrastructure (OCI) Always Free tier as a hosting platform for Mind You AI Council. Mind You is an internal engineering intelligence dashboard built on Next.js 16, Supabase PostgreSQL, and GitHub Actions-based scheduled ingestion. The evaluation was undertaken to determine whether Oracle Always Free could replace the current Vercel and hosted Supabase infrastructure at no cost while preserving the application's existing architecture and operational requirements.

The central research question addressed two claims: that Oracle Always Free provides 4 vCPU and 24 GB of RAM at no cost indefinitely, and that this allocation would be suitable for hosting Mind You. The evaluation determined that the first claim is no longer accurate — Oracle reduced the Always Free ARM A1 allocation from 4 OCPUs / 24 GB RAM to 2 OCPUs / 12 GB RAM in June 2026 — while the second claim, subject to documented operational limitations, is supported by the evidence.

## Methodology

Research was conducted using a four-tier source hierarchy. Tier 1 sources comprised Oracle's official documentation, including the Always Free Resources page, Compute Shapes specifications, and Limits by Service documentation. Tier 2 sources included Oracle blog posts and technical documentation. Tier 3 sources encompassed community reports, third-party documentation from Supabase, Vercel, Railway, Render, Docker, and Caddy, and community observations regarding idle reclamation behavior and quota changes. All Tier 1 sources were verified against live documentation as of 2026-07-23.

Findings were validated through structured section reviews, cross-reference verification, gap analysis between documentation sources, unsupported-claims checks, and confidence assessments for each section. The evaluation comprised thirteen sections covering resource specifications, operational limitations, deployment architecture options, a comparison matrix, migration effort estimates, cost analysis, risk assessment, performance benchmark plans, setup instructions, a production readiness checklist, and a go/no-go decision framework.

## Key Findings

Oracle Always Free currently provides 2 OCPUs and 12 GB of RAM on ARM-based Ampere A1 instances, along with 200 GB of block volume storage, 20 GB of object storage, and 10 TB of monthly outbound data transfer. The allocation was reduced from 4 OCPUs / 24 GB in June 2026 without prior announcement, demonstrating that Always Free resource limits are not contractually guaranteed to remain stable.

Two deployment architectures were evaluated. Architecture A deploys the full Mind You stack — Next.js, Supabase PostgreSQL, GoTrue authentication, PostgREST, Supabase Studio, and Realtime — on a single Oracle Always Free VM using Docker Compose. This approach requires zero application code changes, as the existing Supabase client library is compatible with self-hosted Supabase. Migration effort is estimated at 12 to 14 hours. Architecture B retains Supabase Cloud for the database while deploying only the Next.js application to Oracle, but requires a complete rewrite of all database queries, authentication logic, and storage integration, with an estimated effort of 45 to 80 hours.

Oracle Always Free was compared against four alternatives: the current Vercel and hosted Supabase arrangement, Vercel Hobby (free), Railway (trial/free), and Render (free). Oracle Always Free is the only evaluated option that simultaneously provides sufficient RAM for the full stack, always-on compute without sleep or cold starts, self-hosted PostgreSQL, commercial use permission, and permanent free availability. Vercel Hobby comes closest but is restricted to non-commercial use and provides a maximum of 2 GB of RAM per function.

The evaluation identified ten risks, three of which carry critical impact: idle reclamation of the VM if utilization falls below 20 percent over a 7-day window, account suspension after 90 days of inactivity, and the possibility of future quota reductions. All three have documented mitigations. No SLA is provided for Always Free services, and estimated uptime based on community reports is 95 to 99 percent.

## Recommendation

Oracle Always Free with Architecture A is recommended for Mind You, subject to five conditions: the team accepts self-managed infrastructure; the team accepts the absence of an SLA; idle reclamation prevention is implemented; off-site backups are maintained independent of Oracle Cloud; and Vercel is retained as a fallback. Architecture A is recommended over Architecture B because it preserves the existing Supabase ecosystem, requires no application code changes, and enables rollback through DNS re-pointing. If any condition is unacceptable, the alternative is Vercel Pro combined with Supabase Pro at $45 per month, which provides managed infrastructure, an SLA, and no operational overhead.

The recommendation carries a medium-high confidence level. Factual claims regarding compute, storage, and cost are rated high confidence based on Tier 1 Oracle documentation. Engineering estimates for migration effort and RAM budget are rated medium confidence based on codebase inspection without validation through actual migration. Risk assessments and uptime estimates carry medium confidence based on community reports without systematic measurement.

## Conclusion

The evaluation concludes that Oracle Cloud Always Free is a technically viable hosting platform for Mind You, provided the documented operational trade-offs are accepted. The platform offers the highest RAM allocation among the free-tier options evaluated, at zero monthly cost with no time limit, and supports the existing application architecture with minimal migration effort under Architecture A. The primary risks are well-documented and have established mitigations. This evaluation provides the evidence base required for an informed infrastructure decision and establishes a framework for reassessment should Oracle change Always Free terms in the future.
