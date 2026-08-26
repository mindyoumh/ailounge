# Engineering Intelligence Source Validation

**Date:** 2026-06-22
**Confidence:** High (verified via web search, source inspection, and live feed URL confirmation)
**Subject:** Validation of 6 proposed RSS/API sources from the original engineering-intelligence expansion plan (Sir Bo's proposal, since superseded by the corrected sources below)

---

## Executive Summary

All 6 proposed source categories have viable feed options, but the plan's specific URL recommendations contain several inaccuracies. Three of the plan's suggested URLs are incorrect or suboptimal: WooCommerce Storefront has no standalone feed, CVE.org data requires a new ingester (JSON API, not RSS), and GitHub Trending's existing coverage is manual-file-based rather than automated. This document provides corrected recommendations with confirmed RSS URLs, auth requirements, rate limits, and architecture compatibility notes.

---

## 1. WordPress News

### Official Source

WordPress.org News at `https://wordpress.org/news/` — the official news blog for the WordPress open-source project, covering releases, community events, security announcements, and ecosystem updates [citation:WordPress News](https://wordpress.org/news/).

### Feed Availability

| Feed Type | URL | Status |
|-----------|-----|--------|
| **RSS** | `https://wordpress.org/news/feed/` | ✅ Confirmed — WordPress generates RSS at `/feed/` by default |
| Atom | `https://wordpress.org/news/feed/atom/` | ✅ Available as alternative format |

**Additional WordPress feeds worth considering:**

| Source | Feed URL | Notes |
|--------|----------|-------|
| **WordPress.com Blog** | `https://wordpress.com/blog/feed/` | Official product blog for WordPress.com (hosted platform) |
| **WordPress Developer Blog** | `https://developer.wordpress.org/news/feed/` | Developer-focused: plugin/theme dev, Gutenberg, APIs |

### Architecture Fit

✅ **Fits existing `RSS_FEEDS` array.** Standard WordPress RSS/Atom feed. The existing `parseRSS()` function in `src/ingesters/rss/index.ts` handles `<item>` and `<entry>` elements natively. Add as:

```ts
{ url: "https://wordpress.org/news/feed/", category: "devops", feedFile: "09-devops-news.md" },
```

### Auth & Rate Limits

| Requirement | Detail |
|-------------|--------|
| Authentication | None |
| Rate limits | None observed — standard WordPress RSS |
| Content frequency | ~3–10 posts per week |

### Recommendation

Use `https://wordpress.org/news/feed/` as the primary source. It covers the open-source WordPress project broadly (releases, security, community). For a developer-oriented complement, add `https://developer.wordpress.org/news/feed/` as a second entry.

---

## 2. WordPress Storefront / WooCommerce

### What Storefront Is

Storefront (`woocommerce/storefront` on GitHub, 1K stars, 482 forks) is the **official WooCommerce theme** — a WordPress theme, not a content-producing entity [citation:WooCommerce Storefront GitHub](https://github.com/woocommerce/storefront). It has no blog, no news feed, and no standalone RSS feed.

### Feed Availability

**Storefront has no content RSS feed.** The plan's suggestion of `https://woocommerce.com/blog/feed/` is an assumed URL that was never verified. The correct approach is to use feeds from content-producing entities in the WooCommerce ecosystem:

| Source | Feed URL | Status | Notes |
|--------|----------|--------|-------|
| **WooCommerce Developer Blog** | `https://developer.woocommerce.com/feed/` | ✅ Confirmed — active, RSS feed link present on changelog page [citation:WooCommerce Dev Blog](https://developer.woocommerce.com/changelog/) | Technical content: releases, engineering, APIs. Most relevant for engineering intelligence. |
| **Automattic Corporate News** | `https://automattic.com/news/feed/` | ✅ Confirmed — active [citation:Automattic All Feeds](https://automattic.com/news/all-feeds/) | Covers all Automattic products (WordPress, WooCommerce, Tumblr, etc.) |
| **WooCommerce Releases (GitHub)** | `https://github.com/woocommerce/woocommerce/releases.atom` | ✅ Available as Atom feed | Raw release notes — very technical, high signal |

### Architecture Fit

✅ **Fits existing `RSS_FEEDS` array.** All three options are standard RSS/Atom feeds. The WooCommerce Developer Blog is the single best source for engineering-relevant content about WooCommerce.

```ts
{ url: "https://developer.woocommerce.com/feed/", category: "devops", feedFile: "09-devops-news.md" },
```

### Auth & Rate Limits

| Requirement | Detail |
|-------------|--------|
| Authentication | None |
| Rate limits | None observed |
| Content frequency | ~5–15 posts per month (WooCommerce Dev Blog) |

### Recommendation

Replace the plan's unverified `woocommerce.com/blog/feed/` with `https://developer.woocommerce.com/feed/` as the primary feed. Optionally add `https://automattic.com/news/feed/` for broader Automattic coverage (catches WordPress.com, WooCommerce, Tumblr, Day One, etc. in a single feed).

---

## 3. CVE.org / NVD

### Official Source

The official vulnerability data source is the **National Vulnerability Database (NVD)** maintained by NIST at `https://nvd.nist.gov/`. The CVE Program is at `https://www.cve.org/` [citation:NVD Home](https://nvd.nist.gov/).

### Plan's Proposed Source is Problematic

The plan suggested `https://cvefeed.io/rss/`. This is a **third-party service**, not an official source. Additionally, the related service **cvedaily.com is shutting down** — their site states: "Over the coming months, CVE Daily will be shut down" [citation:CVE Daily](https://cvedaily.com/). Relying on third-party CVE aggregators carries availability risk.

### Official Feed Options

| Source | URL | Format | Status |
|--------|-----|--------|--------|
| **NVD CVE API v2.0** | `https://services.nvd.nist.gov/rest/json/cves/2.0` | JSON (REST API) | ✅ Official — preferred method per NIST |
| **NVD JSON Feeds** | `https://nvd.nist.gov/vuln/data-feeds` | JSON (gzipped files) | ✅ Official — file-based, being deprecated in favor of API |
| **CISA KEV Catalog** | `https://www.cisa.gov/known-exploited-vulnerabilities-catalog` | Web + JSON | ✅ Official — narrower scope (exploited in wild only) |

### Third-Party RSS Options (Backup)

| Source | URL | Format | Risk |
|--------|-----|--------|------|
| **assurestart.co** | `https://assurestart.co/cve-rss-feeds` | RSS / JSON | Medium — third-party, but claims official-source sourcing |
| **cvefeed.io** | `https://cvefeed.io/rssfeed` | RSS / Atom | Medium — third-party, latest 25 CVEs only, updates every 15 min |

### Architecture Fit

⚠️ **Does NOT fit existing `RSS_FEEDS` array without a compatibility layer.** NVD's official data sources are JSON, not RSS. The existing RSS ingester parses RSS/Atom XML. Options:

1. **Low-effort but third-party:** Add `assurestart.co` or `cvefeed.io` RSS URLs to `RSS_FEEDS`. Works immediately but depends on third-party reliability.
2. **Correct but requires new ingester:** Build a new `nvd/` ingester that calls the NVD API v2.0 or downloads JSON feeds. More work but fully official.

### Auth & Rate Limits

| Requirement | Detail |
|-------------|--------|
| Authentication | **API key required** for reasonable rate limits. Register at `https://nvd.nist.gov/developers/request-an-api-key` |
| Rate limits (with key) | Higher limit — no specific number published, but NIST recommends polling no more than once every 2 hours [citation:NVD API Key Announcement](https://nvd.nist.gov/general/news/API-Key-Announcement) |
| Rate limits (without key) | Aggressive throttling — "reduction in requests per rolling 60 second window" |
| Content frequency | Updated continuously as CVEs are published |

### Recommendation

**For immediate implementation:** Use `https://cvefeed.io/rss/` as a temporary RSS source — it works, is updated every 15 minutes, and requires zero code changes.

**For production quality:** Build a new `nvd/` ingester module that calls `https://services.nvd.nist.gov/rest/json/cves/2.0` with an API key. This is the official, reliable path. The new ingester pattern would mirror `hacker-news/index.ts` — fetch JSON, parse, and call existing `upsertEntry()`.

---

## 4. GitHub Daily / Weekly / Monthly

### What the Plan Claimed

The plan stated GitHub daily/weekly/monthly trending is "already covered" by the existing `github-trending` ingester. **This is incorrect.**

### Current Implementation

The existing `github-trending` ingester (`src/ingesters/github-trending/index.ts`) reads from `ideas/trending.md` — a **manually maintained markdown file**. It does NOT scrape `github.com/trending`. There is no automated fetching, no daily/weekly/monthly parameter, and no temporal distinction. The file must be hand-edited to stay current.

### Actual Feed Availability

GitHub's trending page (`https://github.com/trending`) has daily/weekly/monthly tabs but **no official API or RSS** [citation:GitHub Trending](https://github.com/trending). There is no `github.com/trending/feed` or similar endpoint.

### Available Solutions

| Solution | URL Pattern | Type | Status |
|----------|-------------|------|--------|
| **mshibanami/GitHubTrendingRSS** | `https://mshibanami.github.io/GitHubTrendingRSS/{period}/{language}.xml` | RSS | ✅ **Recommended.** Active, 347 stars, MIT license, daily GitHub Actions build [citation:GitHubTrendingRSS](https://github.com/mshibanami/GitHubTrendingRSS). Latest build confirmed working. |
| **RSSHub** | `https://rsshub.app/github/trending/{period}/{language}` | RSS | ✅ Available but requires public instance (rate limited) or self-hosting |

The mshibanami/GitHubTrendingRSS project generates RSS feeds that are confirmed live and working [citation:daily/all.xml](https://mshibanami.github.io/GitHubTrendingRSS/daily/all.xml) [citation:weekly/all.xml](https://mshibanami.github.io/GitHubTrendingRSS/weekly/all.xml) [citation:monthly/all.xml](https://mshibanami.github.io/GitHubTrendingRSS/monthly/all.xml). It is even used in production by other teams (cited in a Zenn article about automated daily tech news feeds) [citation:Zenn Article](https://zenn.dev/iineineno03k/articles/20260325-claude-code-daily-feed).

### Architecture Fit

✅ **Fits existing `RSS_FEEDS` array.** Standard RSS XML. Add three entries:

```ts
{ url: "https://mshibanami.github.io/GitHubTrendingRSS/daily/all.xml",   category: "github", feedFile: "04-github-trending.md" },
{ url: "https://mshibanami.github.io/GitHubTrendingRSS/weekly/all.xml",  category: "github", feedFile: "04-github-trending.md" },
{ url: "https://mshibanami.github.io/GitHubTrendingRSS/monthly/all.xml", category: "github", feedFile: "04-github-trending.md" },
```

### Auth & Rate Limits

| Requirement | Detail |
|-------------|--------|
| Authentication | None — public GitHub Pages hosting |
| Rate limits | None — static files served from GitHub Pages |
| Update frequency | Daily (GitHub Actions runs once per day) |
| Reliability | Hosted on GitHub Pages; risk is low but tied to maintainer's repo |

### Recommendation

Replace the plan's "already covered" claim with 3 specific RSS entries from `mshibanami.github.io/GitHubTrendingRSS/`. The existing manual `ideas/trending.md` ingester can remain as a manual override. This gives fully automated daily/weekly/monthly trending data with zero custom scraping code.

---

## 5. Docker News

### Official Source

Docker Blog at `https://www.docker.com/blog/` — official product and engineering blog for Docker Inc. Active and frequently updated [citation:Docker Blog](https://www.docker.com/blog/).

### Feed Availability

| Feed | URL | Status |
|------|-----|--------|
| **Main Blog** | `https://www.docker.com/feed/` | ✅ Confirmed — live RSS feed (referenced in page source and Docker Hub footer) |
| Engineering | `https://www.docker.com/blog/category/engineering/feed/` | ✅ Available — engineering-specific subset |
| Products | `https://www.docker.com/blog/category/products/feed/` | ✅ Available — product announcements |
| Company | `https://www.docker.com/blog/category/company/feed/` | ✅ Available — company news |
| Community | `https://www.docker.com/blog/category/community-content/feed/` | ✅ Available — community content |

### Architecture Fit

✅ **Fits existing `RSS_FEEDS` array.** Standard WordPress RSS feed. The main blog feed is the best single entry point.

```ts
{ url: "https://www.docker.com/feed/", category: "devops", feedFile: "09-devops-news.md" },
```

### Auth & Rate Limits

| Requirement | Detail |
|-------------|--------|
| Authentication | None |
| Rate limits | None observed |
| Content frequency | ~5–15 posts per month |

### Recommendation

Use `https://www.docker.com/feed/` for broad coverage. If Docker Engineering content is preferred, use `https://www.docker.com/blog/category/engineering/feed/` instead — it filters to engineering-specific posts only.

---

## 6. DevOps News

### Official Source

DevOps.com at `https://devops.com/` — the largest collection of original DevOps content on the web. Industry publication covering DevOps philosophy, tools, business impact, best practices. Active and frequently updated [citation:DevOps.com](https://devops.com/).

### Feed Availability

| Feed | URL | Status |
|------|-----|--------|
| **Main Feed** | `https://devops.com/feed/` | ✅ Confirmed — live RSS feed, confirmed via FeedSpot and direct inspection [citation:DevOps.com Feed](https://devops.com/feed/) |
| DevSecOps Subfeed | `https://devops.com/category/blogs/devsecops/feed/` | ✅ Available — DevSecOps-specific subset |

### Additional DevOps RSS Options

| Source | Feed URL | Notes |
|--------|----------|-------|
| **AWS DevOps Blog** | `https://aws.amazon.com/blogs/devops/feed/` | Amazon's official DevOps blog |
| **Microsoft DevOps Blog** | `https://devblogs.microsoft.com/devops/feed/` | Azure DevOps team blog |
| **Kubernetes Blog** | `https://kubernetes.io/blog/feed.xml` | Official K8s blog |
| **Atlassian DevOps Blog** | `https://www.atlassian.com/blog/devops/feed` | DevOps content from Atlassian |
| **InfoQ DevOps** | `https://feed.infoq.com/Devops/articles/` | Curated DevOps articles |

### Architecture Fit

✅ **Fits existing `RSS_FEEDS` array.** Standard RSS feed.

```ts
{ url: "https://devops.com/feed/", category: "devops", feedFile: "09-devops-news.md" },
```

### Auth & Rate Limits

| Requirement | Detail |
|-------------|--------|
| Authentication | None |
| Rate limits | None observed |
| Content frequency | ~20–40 posts per month |

### Recommendation

Use `https://devops.com/feed/` as the primary DevOps feed. It is the broadest and most active source. For a multi-feed approach, also consider adding `https://aws.amazon.com/blogs/devops/feed/` and `https://devblogs.microsoft.com/devops/feed/` under the same `devops` category — these three feeds together cover the DevOps landscape comprehensively.

---

## Summary Table

| # | Source | Plan's Claim | Correct Feed URL(s) | Fits RSS_FEEDS? | Auth Required? | Risk |
|---|--------|-------------|---------------------|-----------------|----------------|------|
| 1 | **WordPress News** | `wordpress.org/news/feed/` | ✅ Correct | ✅ Yes | None | Low |
| 2 | **WooCommerce/Storefront** | `woocommerce.com/blog/feed/` (unverified) | `developer.woocommerce.com/feed/` or `automattic.com/news/feed/` | ✅ Yes | None | Low |
| 3 | **CVE.org** | `cvefeed.io/rss/` (third-party) | **RSS workaround:** `cvefeed.io/rss/` (temporary). **Official:** NVD API v2.0 (JSON, needs new ingester + API key) | ⚠️ RSS only; JSON needs new ingester | API key for NVD | Medium |
| 4 | **GitHub trending** | "Already covered" ❌ (manual file only) | `mshibanami.github.io/GitHubTrendingRSS/{daily,weekly,monthly}/all.xml` | ✅ Yes | None | Low |
| 5 | **Docker News** | `docker.com/feed/` | ✅ Correct — also has sub-category feeds | ✅ Yes | None | Low |
| 6 | **DevOps News** | `devops.com/feed/` | ✅ Correct | ✅ Yes | None | Low |

## Implementation Notes

### Can Add to RSS_FEEDS Immediately (No Code Changes)

| Feed URL | Category | Feed File |
|----------|----------|-----------|
| `https://wordpress.org/news/feed/` | devops | `09-devops-news.md` |
| `https://developer.woocommerce.com/feed/` | devops | `09-devops-news.md` |
| `https://www.docker.com/feed/` | devops | `09-devops-news.md` |
| `https://devops.com/feed/` | devops | `09-devops-news.md` |
| `https://mshibanami.github.io/GitHubTrendingRSS/daily/all.xml` | github | `04-github-trending.md` |
| `https://mshibanami.github.io/GitHubTrendingRSS/weekly/all.xml` | github | `04-github-trending.md` |
| `https://mshibanami.github.io/GitHubTrendingRSS/monthly/all.xml` | github | `04-github-trending.md` |
| `https://cvefeed.io/rss/` (temporary) | security | `08-security-alerts.md` |

### Requires a New Ingester (CVE)

Build a new `src/ingesters/nvd/index.ts` that:
1. Calls `https://services.nvd.nist.gov/rest/json/cves/2.0` with API key
2. Parses the JSON response
3. Calls existing `upsertEntry()` for each CVE
4. Respects the 2-hour polling interval recommended by NIST

Pattern to follow: `src/ingesters/hacker-news/index.ts` — it already does JSON fetch → parse → upsert.

### Category Consolidation

All devops-related feeds (WordPress, WooCommerce, Docker, DevOps.com) can share `09-devops-news.md` as a single feed file. This keeps the `docs/feeds/` directory manageable. If per-source granularity is desired later, splitting into separate files is a zero-cost refactor.

## Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| mshibanami/GitHubTrendingRSS repo archived | Low | Medium | Monitor repo health; RSSHub is a backup option |
| cvefeed.io goes offline | Medium | Low | Only a temporary solution; official NVD ingester is the long-term fix |
| NVD API key management | Low | Low | Single key, stored in env var, documented in README |
| WordPress feed format change | Low | Low | Existing parser handles standard RSS/Atom; changes would break many consumers |
| DevOps sub-feeds overlap content | Medium | Low | DB dedup by URL handles duplicates; no functional issue |
