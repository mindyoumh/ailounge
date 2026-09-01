# PR #25 Review — Deferred Backlog (Medium / Low)

**PR:** https://github.com/mindyoumh/my-ailounge/pull/25 (`feat/supabase-migration` → `main`)
**Reviewed:** 2026-07-24 · Two-pass review (Sonnet + Opus) + engineering gate
**Scope:** 231 files, +30,519 / −389 — fork consolidation (Supabase migration, ingestion pipelines, log analysis, watchlist, RBAC)

Critical/High findings were fixed in-branch (see commit). Items below are deferred per severity policy — do **not** implement in this PR unless a maintainer promotes them.

---

## Fixed in this PR (context)

- **H1 — Broken access control.** `logs POST`, `prompts POST`, `prompts/[id] PATCH+DELETE`, `ingest POST` used the `serviceClient` (service-role key, **RLS bypassed**) with no role check, while `docs/rls-policies.sql` marks those operations `is_lead_or_dev()`. Any authenticated user (including `intern`) could write/delete. Added `requireRole(req, ["lead","dev"])` mirroring the sibling `[id]` routes.
- **H2 — Cron schedules malformed.** All three ingest workflows put `*/N` in the **day-of-month** field (`0 0 */6 * *`), so ingestion ran days apart, not the documented 4h/12h/6h. Corrected to hour-field intervals (`0 */4 * * *`, `0 */12 * * *`, `0 */6 * * *`).

---

## Medium

### M1 — PostgREST `.or()` filter injection in prompts search
- **Issue:** `app/api/prompts/route.ts` builds `query.or(\`title.ilike.%${search}%,description.ilike.%${search}%\`)` with unescaped user input interpolated into the PostgREST filter grammar.
- **Impact:** A crafted `search` (containing `,` or operator tokens) can inject additional filter clauses against the `prompts` table — boolean enumeration / query manipulation. Low data-exposure (prompts are public-read) but a genuine injection pattern that can widen if copied elsewhere.
- **Recommendation:** Use value-bound `.ilike(column, pattern)` (as `feed/route.ts` does) instead of the `.or()` string form, or strip `,`/`.`/`()` from `search` before interpolation.
- **Priority:** Medium
- **Refs:** PR #25 · `app/api/prompts/route.ts:18`

### M2 — Large CSV upload buffered fully in memory
- **Issue:** `app/api/logs/route.ts` does `await file.text()` then `parseLogCsv(...)`; `next.config.ts` caps body at `150mb` but the whole file + parsed arrays sit in memory.
- **Impact:** A large Zoho/Acuity CSV can OOM or time out a serverless function (Netlify function memory ceiling). Now lead/dev-gated, so lower abuse surface, but still a robustness/availability risk.
- **Recommendation:** Stream-parse the CSV (row-by-row) and/or enforce a row/byte cap with a clear 413 response; consider offloading heavy parses to a background job.
- **Priority:** Medium
- **Refs:** PR #25 · `app/api/logs/route.ts`, `next.config.ts`

### M3 — Floating async work in watchlist create
- **Issue:** `app/api/watchlist/route.ts` POST fires `retroactivelyScore(...)` and `fetchLatestVersion(...).then(...)` without awaiting; the version-fetch chain has no `.catch`.
- **Impact:** On serverless, work after the response returns may be killed mid-flight (version/CVE never persisted); an unhandled rejection can crash the function instance.
- **Recommendation:** `await` the enrichment, move it to a queue/cron, or use the platform's `waitUntil`; add `.catch` to every detached promise.
- **Priority:** Medium
- **Refs:** PR #25 · `app/api/watchlist/route.ts`

### M4 — Raw error detail returned to clients
- **Issue:** Several routes return `err.message` / `String(err)` in the JSON body (`ingest`, `prompts`, `watchlist`).
- **Impact:** Leaks DB/stack internals to the client (information disclosure).
- **Recommendation:** Return a generic message; log full detail server-side only.
- **Priority:** Medium
- **Refs:** PR #25 · `app/api/ingest/route.ts`, `app/api/prompts/route.ts`

### M5 — Inconsistent role gating on watchlist/[id] PATCH
- **Issue:** `watchlist/[id]` DELETE is `requireRole`-gated but PATCH is not. RLS marks `watchlist` update as `authenticated` (any), so current behavior is defensible — but the split is easy to misread.
- **Impact:** Low functional risk; maintainability/clarity.
- **Recommendation:** Add an inline comment stating PATCH is intentionally open per RLS `watchlist_items_update = authenticated`, or gate it if leads-only editing is desired.
- **Priority:** Medium
- **Refs:** PR #25 · `app/api/watchlist/[id]/route.ts`

---

## Low

### L1 — `next` redirect param not validated (auth callback)
- **Issue:** `app/auth/callback/route.ts` redirects to `\`${origin}${next}\`` with `next` taken from the query string unchecked.
- **Impact:** Same-origin prefix keeps it from being a classic open redirect, but `next` values like `//host` or absolute URLs cause surprising/broken redirects.
- **Recommendation:** Accept `next` only when it matches `^/[^/]` (single leading slash, no `//`); otherwise fall back to `/feed`.
- **Priority:** Low
- **Refs:** PR #25 · `app/auth/callback/route.ts`

### L2 — Public-route matching uses `startsWith`
- **Issue:** `proxy.ts` treats a path as public if it `startsWith` a public prefix, so `/login-x`, `/api/statsX`, `/intern-tasks-y` also bypass auth.
- **Impact:** Low today (no such routes exist) but a foot-gun for future routes.
- **Recommendation:** Match `pathname === r || pathname.startsWith(r + "/")`.
- **Priority:** Low
- **Refs:** PR #25 · `proxy.ts`

### L3 — Verbose `console.*` in server code
- **Issue:** ~60 `console.log`/`console.error` calls across `src/` and `app/`.
- **Impact:** Noisy logs, potential minor detail leakage into platform logs.
- **Recommendation:** Route through a small logger with levels; drop debug lines.
- **Priority:** Low
- **Refs:** PR #25

### L4 — Verify `experimental.proxyClientMaxBodySize` is a real Next 16 option
- **Issue:** `next.config.ts` sets `experimental.proxyClientMaxBodySize: "150mb"`.
- **Impact:** If the key is not recognized by Next 16 it is a silent no-op and uploads fall back to default limits — undermining the large-CSV feature.
- **Recommendation:** Confirm against Next 16 docs; if invalid, use the correct route-handler body-size mechanism.
- **Priority:** Low
- **Refs:** PR #25 · `next.config.ts`
