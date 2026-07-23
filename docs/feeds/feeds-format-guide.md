# docs/feeds — Format Guide & Standards

This document defines the standard format for all `docs/feeds/*.md` files.
Every intern must follow this format exactly so the ingester can parse entries without breaking.

---

## Why This Format Exists

When you or a teammate writes a news entry in one of these files, the **manual feeds ingester**
(`src/ingesters/manual-feeds/index.ts`) reads the file daily, parses each entry,
and upserts it into the `feed_items` Supabase PostgreSQL table.

The dashboard then displays it alongside auto-fetched sources (HN, RSS, GitHub Trending).

---

## The Standard Entry Format

Every news entry must follow this exact pattern:

```
- [Title of the article or update](https://full-url-here.com) | YYYY-MM-DD | tag1, tag2, tag3
```

### Rules

| Field | Required | Notes |
|---|---|---|
| Title | ✅ Yes | Keep it short and clear. No quotes needed. |
| URL | ✅ Yes | Must be a full URL including `https://` |
| Date | ✅ Yes | Format: `YYYY-MM-DD` (e.g. `2026-06-08`) |
| Tags | ✅ Yes | At least 1 tag. Comma-separated. Lowercase. |

### Valid Tags per File

| File | Allowed Tags |
|---|---|
| `01-ai-news.md` | `ai`, `model`, `release`, `benchmark`, `pricing`, `agent`, `research` |
| `02-cloud-news.md` | `cloud`, `aws`, `gcp`, `azure`, `outage`, `pricing`, `infra` |
| `03-django-news.md` | `django`, `python`, `drf`, `release`, `security`, `cve` |
| `04-github-trending.md` | `github`, `trending`, `repo`, `opensource` |
| `05-hacker-news.md` | `hn`, `discussion`, `engineering`, `career` |
| `06-nextjs-news.md` | `nextjs`, `vercel`, `release`, `rfc`, `security`, `cve`, `canary` |
| `07-rumors.md` | `rumor`, `leak`, `unverified` — **always add `unverified` tag** |
| `08-security-alerts.md` | `security`, `cve`, `patch`, `critical`, `high`, `medium` |
| `09-wordpress-news.md` | `wordpress`, `woocommerce`, `release`, `plugin`, `theme`, `community` |
| `10-docker-news.md` | `docker`, `container`, `release`, `security`, `cve`, `compose` |
| `11-devops-news.md` | `devops`, `ci/cd`, `kubernetes`, `infra`, `automation`, `platform` |
| `12-github-news.md` | `github`, `copilot`, `release`, `changelog`, `engineering`, `security` |

---

## Good vs Bad Examples

### ✅ Correct
```
- [Next.js 16.2 Released with AI Tooling](https://nextjs.org/blog/next-16-2) | 2026-06-08 | nextjs, release, performance
- [Critical CVE-2026-44578 SSRF via WebSockets](https://github.com/advisories/GHSA-xxxx) | 2026-06-07 | security, cve, critical
- [Turbopack stable for production builds](https://vercel.com/blog/turbopack-stable) | 2026-06-06 | nextjs, release
```

### ❌ Wrong — will break the ingester
```
- Next.js 16.2 Released (no URL, no date)
- [Missing date](https://example.com) | release
- [Wrong date format](https://example.com) | June 8 2026 | nextjs
- [Extra pipes](https://example.com) | 2026-06-08 | nextjs | release
```

---

## Section Headers (Optional but Recommended)

You can group entries under `## Month Year` headers for readability.
The ingester ignores these headers — they're just for humans reading the file.

```md
## June 2026

- [Entry 1](https://...) | 2026-06-08 | tag1
- [Entry 2](https://...) | 2026-06-07 | tag1, tag2

## May 2026

- [Older entry](https://...) | 2026-05-15 | tag1
```

---

## How to Add a New Entry (Step by Step)

1. Open the correct file for the topic (e.g. `06-nextjs-news.md` for Next.js news)
2. Add your entry at the **top** of the most recent month section
3. Follow the exact format: `- [Title](URL) | YYYY-MM-DD | tags`
4. Save and commit to GitHub
5. The ingester picks it up within 24 hours (or trigger manually from GitHub Actions)

---
---

# Individual Feed Files

Below is the content for each of the 12 feed files with starter entries.
Save each section as its own file in `docs/feeds/`.

---

# FILE: docs/feeds/01-ai-news.md

```md
# AI News
> Curated AI model updates, tooling releases, benchmarks, and research.
> Format: - [Title](URL) | YYYY-MM-DD | tag1, tag2

## June 2026

- [Claude Sonnet 4.6 Released — Faster and Cheaper](https://www.anthropic.com/news/claude-sonnet-4-6) | 2026-06-01 | ai, model, release, anthropic
- [OpenAI GPT-5 Available via API](https://openai.com/blog/gpt-5-api) | 2026-05-28 | ai, model, release, openai
- [DeepSeek V3.2 Benchmark Results](https://deepseek.com/blog/v3-2-benchmarks) | 2026-05-20 | ai, benchmark, model, deepseek
- [Gemini CLI Free Tier Announced](https://geminicli.com/blog/free-tier) | 2026-05-15 | ai, gemini, pricing, google
```

---

# FILE: docs/feeds/02-cloud-news.md

```md
# Cloud / AWS News
> AWS, GCP, Azure updates, pricing changes, outages, and infra news.
> Format: - [Title](URL) | YYYY-MM-DD | tag1, tag2

## June 2026

- [AWS us-east-1 Outage Post-Mortem](https://aws.amazon.com/message/us-east-1-outage) | 2026-06-03 | cloud, aws, outage, infra
- [GCP Launches New Asia-Pacific Region](https://cloud.google.com/blog/new-apac-region) | 2026-05-25 | cloud, gcp, infra
- [AWS EC2 Price Reduction for t4g Instances](https://aws.amazon.com/blogs/ec2-pricing) | 2026-05-10 | cloud, aws, pricing
```

---

# FILE: docs/feeds/03-django-news.md

```md
# Python / Django News
> Django, DRF, Celery, Python releases, security patches, ecosystem updates.
> Format: - [Title](URL) | YYYY-MM-DD | tag1, tag2

## June 2026

- [Django 5.2.1 Security Release](https://docs.djangoproject.com/en/5.2/releases/5.2.1/) | 2026-06-05 | django, release, security
- [Django REST Framework 3.16 Released](https://www.django-rest-framework.org/community/release-notes/) | 2026-05-18 | django, drf, release
- [Celery 5.4 Drops Python 3.8 Support](https://docs.celeryq.dev/en/stable/changelog.html) | 2026-05-10 | python, django, release
```

---

# FILE: docs/feeds/04-github-trending.md

```md
# GitHub Trending
> Notable repos from GitHub Trending — manually curated picks worth highlighting.
> Auto-fetched repos also appear here via scraper.py → SQLite.
> Format: - [Title](URL) | YYYY-MM-DD | tag1, tag2

## June 2026

- [muratcankoylan/Agent-Skills-for-Context-Engineering](https://github.com/muratcankoylan/Agent-Skills-for-Context-Engineering) | 2026-06-04 | github, trending, ai, agent
- [microsoft/markitdown](https://github.com/microsoft/markitdown) | 2026-06-04 | github, trending, opensource, tools
- [anthropics/claude-code](https://github.com/anthropics/claude-code) | 2026-06-01 | github, trending, ai, anthropic
```

---

# FILE: docs/feeds/05-hacker-news.md

```md
# Hacker News
> High-signal HN discussions worth reading — manually saved picks.
> Top stories are also auto-fetched daily via HN Algolia API → SQLite.
> Format: - [Title](URL) | YYYY-MM-DD | tag1, tag2

## June 2026

- [Ask HN: What's your current AI coding setup?](https://news.ycombinator.com/item?id=12345678) | 2026-06-07 | hn, discussion, ai
- [Show HN: We built a self-hosted Hacker News reader](https://news.ycombinator.com/item?id=12345000) | 2026-06-05 | hn, discussion, opensource
- [The myth of the 10x developer in the age of AI](https://news.ycombinator.com/item?id=11000000) | 2026-06-02 | hn, discussion, engineering, career
```

---

# FILE: docs/feeds/06-nextjs-news.md

```md
# Next.js News
> Releases, RFCs, breaking changes, CVEs, canary updates, and Vercel blog posts.
> Format: - [Title](URL) | YYYY-MM-DD | tag1, tag2

## June 2026

- [Next.js 16.2 Released — 400% faster dev startup](https://nextjs.org/blog/next-16-2) | 2026-06-01 | nextjs, release, performance
- [CVE-2026-44578 — Critical SSRF via WebSockets](https://github.com/advisories/GHSA-nextjs-ssrf) | 2026-05-30 | nextjs, security, cve, critical
- [CVE-2026-23870 — RSC Denial of Service](https://github.com/advisories/GHSA-nextjs-rsc-dos) | 2026-05-30 | nextjs, security, cve, high
- [Turbopack stable for next build](https://vercel.com/blog/turbopack-stable) | 2026-05-20 | nextjs, release, turbopack
- [Stable Adapter API — Deploy anywhere](https://nextjs.org/blog/adapter-api-stable) | 2026-05-15 | nextjs, release, deployment
- [RFC: Cache Components proposal](https://github.com/vercel/next.js/discussions/cache-components) | 2026-05-10 | nextjs, rfc, canary
```

---

# FILE: docs/feeds/07-rumors.md

```md
# Leaks / Rumors
> Unverified reports, leaks, and speculation. Always tagged `unverified`.
> Do NOT treat these as facts. Verify before acting on anything here.
> Format: - [Title](URL) | YYYY-MM-DD | tag1, tag2, unverified

## June 2026

- [Rumor: OpenAI planning GPT-6 release for Q3 2026](https://x.com/somedev/status/000000) | 2026-06-06 | rumor, ai, openai, unverified
- [Leak: Vercel working on self-hosted Next.js runtime](https://reddit.com/r/nextjs/comments/leak) | 2026-06-01 | rumor, nextjs, vercel, unverified
```

---

# FILE: docs/feeds/08-security-alerts.md

```md
# Security Alerts
> CVEs, npm/PyPI advisories, emergency patches. Check this daily.
> Format: - [Title](URL) | YYYY-MM-DD | tag1, tag2

## June 2026

- [CVE-2026-44578 — Next.js SSRF via WebSockets — CRITICAL](https://github.com/advisories/GHSA-nextjs-ssrf) | 2026-05-30 | security, cve, critical, nextjs
- [CVE-2026-23870 — React Server Components DoS — HIGH](https://github.com/advisories/GHSA-rsc-dos) | 2026-05-30 | security, cve, high, nextjs
- [npm: lodash prototype pollution patch](https://www.npmjs.com/advisories/lodash-fix) | 2026-05-22 | security, patch, npm, medium
- [PyPI: requests library MITM vulnerability](https://pypi.org/security/requests-mitm) | 2026-05-18 | security, cve, high, python
```

---

# FILE: docs/feeds/09-wordpress-news.md

```md
# WordPress News
> WordPress core releases, plugin/theme ecosystem, WooCommerce updates, and community events.
> Format: - [Title](URL) | YYYY-MM-DD | tag1, tag2

## June 2026

- [WordPress 7.0 "Armstrong"](https://wordpress.org/news/2026/05/armstrong/) | 2026-05-20 | wordpress, release
- [Introducing the WooCommerce dual API](https://developer.woocommerce.com/2026/06/04/introducing-the-woocommerce-dual-api/) | 2026-06-04 | wordpress, woocommerce, api
```

---

# FILE: docs/feeds/10-docker-news.md

```md
# Docker News
> Docker releases, security advisories, container ecosystem updates, and best practices.
> Format: - [Title](URL) | YYYY-MM-DD | tag1, tag2

## June 2026

- [Docker Hardened Images enhanced vulnerability scanning](https://www.docker.com/blog/docker-hardened-images-enhanced-vulnerability-scanning-with-docker-and-aikido/) | 2026-06-11 | docker, security
```

---

# FILE: docs/feeds/11-devops-news.md

```md
# DevOps News
> DevOps practices, CI/CD, platform engineering, infrastructure-as-code, and automation.
> Format: - [Title](URL) | YYYY-MM-DD | tag1, tag2

## June 2026

- [IaC Isn't Dying. AI Makes it More Important](https://devops.com/iac-isnt-dying-ai-makes-it-more-important/) | 2026-06-18 | devops, iac, ai
```

---

# FILE: docs/feeds/12-github-news.md

```md
# GitHub News
> GitHub platform announcements, changelogs, product updates, Copilot features, and engineering blog.
> Format: - [Title](URL) | YYYY-MM-DD | tag1, tag2

## June 2026

- [How we built an internal data analytics agent](https://github.blog/ai-and-ml/github-copilot/how-we-built-an-internal-data-analytics-agent/) | 2026-06-19 | github, copilot, ai
```
