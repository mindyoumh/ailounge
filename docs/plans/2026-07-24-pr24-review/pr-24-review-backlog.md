# PR #24 Review — Deferred Findings Backlog

**PR:** [#24 — Developer Intelligence Feed Dashboard Foundation](https://github.com/mindyoumh/my-ailounge/pull/24)
**Branch:** `feat/developer-intelligence-feed` → `main`
**Reviewed:** 2026-07-24
**Scope:** 174 files, +23,271 / −246
**Source review:** [`docs/archives/2026-07-24/pr-24-review.md`](../../archives/2026-07-24/pr-24-review.md)

Critical/High findings were fixed in-branch (commit `ec996c1` — build repair). The
items below are **Medium/Low** and are intentionally **not** implemented in this PR.
Each is a candidate for a follow-up ticket.

---

## MEDIUM

### M1 — No authentication/authorization on any API route
- **Issue:** Every route under `app/api/**` is unauthenticated. Mutating and
  destructive endpoints are fully open: `POST/PATCH/DELETE /api/feed[/id]`,
  `PATCH/DELETE /api/prompts[/id]`, `/api/watchlist[/id]`, `/api/repo-radar[/id]`,
  `POST /api/ingest`, `POST /api/repo-radar/refresh`.
- **Impact:** Anyone who can reach the app can read, modify, or delete all data and
  trigger outbound fan-out. Data-loss and abuse risk once exposed beyond localhost.
- **Recommendation:** Introduce an auth layer (session/service token, or the
  Supabase-auth work already prototyped on the `feat/supabase-migration` branch) and
  gate all mutations. Until then, keep the deployment private/localhost-only.
- **Priority:** Medium (foundation PR has no auth framework yet; building one is out of
  this PR's scope). Elevate to High before any public exposure.

### M2 — No size/row limit on CSV upload
- **Issue:** `POST /api/logs` reads the whole upload via `file.text()` and inserts
  every parsed row. No max file size, no row cap (`app/api/logs/route.ts`,
  `components/logs/csv-upload.tsx`).
- **Impact:** A large CSV exhausts server memory (full file + parsed array + all rows
  in one transaction). Denial-of-service / OOM.
- **Recommendation:** Enforce a byte-size cap client- and server-side, cap parsed rows,
  and stream/batch inserts.
- **Priority:** Medium.

### M3 — Schema split; `getDb()` does not create the full schema
- **Issue:** `src/db/client.ts` `BOOT_SQL` creates only `watchlist_items` and the
  `log_*` tables. `feed_items`, `kv_store`, `prompts`, `repo_radar_items` exist **only**
  in `src/db/schema.ts` `migrate()`. `watchlist_items`/`log_*` are **duplicated** across
  both files.
- **Impact:** On a fresh `data/dashboard.db`, hitting `/feed`, `/prompts`,
  `/repo-radar`, or `/api/stats` before `migrate()` has run throws
  `no such table: feed_items`. Duplicated DDL invites schema drift.
- **Recommendation:** Make `migrate()` the single source of truth and have `getDb()`
  ensure the full schema (or run `migrate()` lazily on first connect). Remove the
  duplicated DDL from `BOOT_SQL`.
- **Priority:** Medium (documented "run ingest first" flow mitigates; one-time setup).

### M4 — CI ingestion workflow is broken and non-functional
- **Issue:** `.github/workflows/ingest.yml` pins `node-version: '18'` and runs
  `npx ts-node ...`, but the code uses `node:sqlite` (requires Node ≥ 22.5) and the
  package scripts use `tsx`. Additionally the SQLite DB written during a CI run is
  ephemeral — it is never persisted or committed, so the Node ingesters are effectively
  no-ops.
- **Impact:** Scheduled/dispatched ingestion runs crash; even if fixed, results are
  discarded.
- **Recommendation:** Decide the persistence model first (commit a DB artifact, push to
  a hosted DB, or drop CI ingestion). Then bump to Node 22 and switch `ts-node` → `tsx`.
- **Priority:** Medium (does not affect app build/runtime; needs a design decision, not
  a one-line bump).

### M5 — Unauthenticated GitHub fan-out, no rate limiting
- **Issue:** `POST /api/repo-radar/refresh` → `refreshAll()` makes ~4 GitHub calls per
  active repo (14 seeds → ~56 calls) with **no token** (`src/lib/repo-radar.ts`). No
  rate limiting on the endpoint.
- **Impact:** One refresh nearly exhausts the 60/hr unauthenticated limit; the endpoint
  can be hammered to amplify outbound traffic.
- **Recommendation:** Support `GITHUB_TOKEN` (5,000/hr), batch/throttle calls, and add
  endpoint rate limiting.
- **Priority:** Medium.

### M6 — Zero automated tests
- **Issue:** No unit/integration/E2E tests anywhere in the PR. High-logic units are
  untested: `src/lib/log-parser.ts`, ingester dedup, the PDF builder
  (`app/api/logs/[id]/export/pdf/route.ts`, 785 lines), and all API routes.
- **Impact:** Regression risk on every future change; violates the repo's 80% coverage
  standard.
- **Recommendation:** Add a Vitest harness; start with `log-parser`, `markdown` trim
  logic, and the feed/logs API routes; add Playwright for the `/feed` and `/logs` flows.
- **Priority:** Medium.

### M7 — No `engines` field pinning Node ≥ 22.5
- **Issue:** `package.json` has no `engines`. `node:sqlite` requires Node ≥ 22.5.
- **Impact:** Fresh clone on older Node fails with a cryptic `Cannot find module
  'node:sqlite'`.
- **Recommendation:** Add `"engines": { "node": ">=22.5" }` and document it in the setup
  README.
- **Priority:** Medium.

---

## LOW

### L1 — PDF `Content-Disposition` filename not fully quote-safe
- `app/api/logs/[id]/export/pdf/route.ts:433` interpolates `filename` into the header.
  `sanitize()` strips CR/LF (no header injection) but keeps `"`, so a crafted uploaded
  filename could break out of the quoted `filename` param. Cosmetic; escape/strip `"`.

### L2 — `limit`/`offset` parsed without NaN guard
- `app/api/feed/route.ts:14-15` (and sibling routes): `parseInt("abc")` → `NaN` →
  `LIMIT NaN` bind error. Coerce with a fallback (`Number.isFinite` check).

### L3 — Manual-feeds ingester still wired despite "removed" claim
- PR body says the manual ingester was removed, but `ingest:manual`
  (`package.json`) and the `manual-feeds` workflow step remain. Reconcile scope/docs.

### L4 — HN ingester assumes non-null title
- `src/ingesters/hacker-news/index.ts:34` `item.title.replace(...)` throws if the HN
  API returns a null title. Guard with `(item.title ?? "")`.

### L5 — Double sort per pattern in log-parser
- `src/lib/log-parser.ts:253-254` calls `.sort()[0]` then `.sort().reverse()[0]` on the
  same array — mutates and sorts twice. Sort once, read both ends.

---

## Fixed in this PR (for reference)

- **CRITICAL** — `app/logs/page.tsx` imported a non-existent
  `@/components/logs/severity-filter`; component added.
- **CRITICAL** — `app/api/logs/[id]/trend/route.ts` imported a non-existent
  `@/src/db/supabase-client`; route ported to the `node:sqlite` `getDb()` client.
- Result: `next build` compiles all 30 routes (commit `ec996c1`).
