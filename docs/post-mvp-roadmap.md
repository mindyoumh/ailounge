# Post-MVP Roadmap

## Tier 1: Automation & Intelligence (highest value)

| Feature | Why |
|---------|-----|
| **Scheduled Ingestion** | Currently manual (`npm run ingest`). GitHub Actions cron or a lightweight scheduler makes it self-running. Already called out in README as the next step. |
| **CVE/Security Alerts** | When `POST /api/watchlist/[id]/cve` finds a new vulnerability — push notifications via email, Slack, or Google Chat |
| **AI Daily Digest** | LLM-generated summary of the day's top stories, version changes, and security alerts — emailed or posted to Google Chat |
| **Watchlist Auto-Suggest** | Scan incoming feed items for package names not yet tracked, suggest adding them to watchlist |

## Tier 2: Dashboard Enhancements

| Feature | Why |
|---------|-----|
| **User Preferences** | Per-user feed filters, custom dashboard layout, saved searches, notification toggles |
| **Trend Charts** | Time-series: feed volume over time, source reliability scoring, category growth trends |
| **Export & Reports** | PDF/CSV export for the briefing page, weekly digest PDF, shareable dashboard snapshots |
| **Feed Source Expansion** | Add Dev.to, Medium, Reddit, Twitter/X — more data = better intelligence |

## Tier 3: Quality & Infrastructure

| Feature | Why |
|---------|-----|
| **Test Suite** | Currently zero tests. Unit tests for ingesters + lib, integration tests for API routes |
| **CI/CD Pipeline** | Auto-build, type-check, test, deploy on push |
| **Performance** | ISR for static pages, DB query optimization, lazy-load heavy chart components |
| **i18n** | Multi-language support if targeting broader audience |

## Tier 4: Polish

| Feature | Why |
|---------|-----|
| **Onboarding flow** | Guided first-run experience for new users |
| **Command palette expansion** | Add more actions (jump to watchlist, run ingest, open specific feed) |
| **Empty states** | Better illustrations/messaging when data is missing |
| **Mobile responsive pass** | Verify and polish all pages on 375px screens |
