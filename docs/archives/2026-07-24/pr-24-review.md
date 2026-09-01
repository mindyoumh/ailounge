# PR #24 Review — Developer Intelligence Feed Dashboard Foundation

**PR:** [#24](https://github.com/mindyoumh/my-ailounge/pull/24) · `feat/developer-intelligence-feed` → `main`
**Reviewed:** 2026-07-24 · **Scope:** 174 files, +23,271 / −246
**Method:** 2 review passes (broad + deep) + independent role gate.

---

## Headline: the PR did not build. It does now.

`next build` on the submitted branch failed with two build-breaking imports. Both were
fixed, a clean build verified (all 30 routes compile), and committed to the branch.

| # | Severity | Finding | Resolution |
|---|----------|---------|------------|
| C1 | **Critical** | `app/logs/page.tsx:27` imports `@/components/logs/severity-filter` — file never added. | Created the component (all/high/medium/low select, matching the `value`/`onChange` contract). |
| C2 | **Critical** | `app/api/logs/[id]/trend/route.ts:2` imports `@/src/db/supabase-client` — module absent (Supabase leftover in a `node:sqlite` PR). | Rewrote route on `getDb()` with a parameterized query, consistent with the sibling `errors` route. |

Fix commit: `ec996c1`. Backlog commit: `9eb10df`.

---

## Security posture — sound

- All SQL is **parameterized**; every PATCH route builds `SET` clauses from a **fixed
  column whitelist** (no injection via column names).
- No committed secrets or DB files (`.gitignore` covers `.env`, `*.db`,
  `service-account.json`).
- No `eval` / `child_process` / `dangerouslySetInnerHTML`.
- Ingesters hit fixed URLs (no SSRF); external data is React-rendered (XSS-safe).
- `strict: true`, no `ignoreBuildErrors` masking — the two build errors were real.

No further Critical/High findings.

---

## Deferred findings (Medium / Low)

Full detail: `docs/plans/2026-07-24-pr24-review/pr-24-review-backlog.md`.

**Medium**
- **M1** — No auth/authorization on any API route; mutating & destructive endpoints are
  fully open.
- **M2** — No size/row limit on CSV upload (`/api/logs` reads whole file into memory) —
  OOM/DoS.
- **M3** — Schema split: `client.ts` `BOOT_SQL` omits `feed_items`, `kv_store`,
  `prompts`, `repo_radar_items` (only in `schema.ts` `migrate()`) and duplicates the
  rest. Fresh DB + page load before `migrate()` → `no such table`.
- **M4** — CI ingestion broken: `ingest.yml` pins Node 18 + `ts-node`, but code needs
  Node ≥ 22.5 (`node:sqlite`) and scripts use `tsx`; the CI SQLite DB is ephemeral.
- **M5** — Unauthenticated GitHub fan-out (~56 calls/refresh vs 60/hr limit); no rate
  limiting.
- **M6** — Zero automated tests across 23k lines.
- **M7** — No `engines` field pinning Node ≥ 22.5.

**Low**
- **L1** — PDF `Content-Disposition` filename keeps `"` (quote break-out; no CRLF
  injection).
- **L2** — `limit`/`offset` parsed without NaN guard.
- **L3** — Manual-feeds ingester still wired despite "removed" claim in PR body.
- **L4** — HN ingester assumes non-null `title`.
- **L5** — `log-parser` double-sorts timestamps per pattern.

---

## Engineering Gate

**Senior Engineer.** Clean ingesters → sqlite → API → UI separation; parameterized
access. Chief debt: dual schema sources (M3). `node:sqlite` is fine for a local internal
dashboard, a ceiling for multi-user/hosted. 785-line PDF route is a lot of layout logic
in one file. Acceptable foundation; M1/M3 are near-term must-dos.

**QA / Test Engineer.** Zero tests is the biggest gap. Highest-risk untested units:
`log-parser`, ingester dedup, PDF builder. Edge cases open: fresh-DB missing tables (M3),
oversized CSV (M2), non-numeric paging (L2), null HN title (L4). Build green; runtime
unexercised. Establish Vitest + Playwright before further feature work.

**Technical Writer.** Docs unusually thorough (per-dir READMEs, build spec, plans). Two
accuracy gaps: manual ingester "removed" claim (L3); setup docs omit Node ≥ 22.5 (M7) and
the "run `npm run ingest` / `db:migrate` before first load" step (M3).

**Team Lead.** Acceptance criteria met at foundation level; app compiles. Large but
coherent scope. Not production-ready as a public service (no auth, no rate limiting, no
tests, ephemeral CI). Reasonable merge as an **internal/localhost foundation**.

---

## Merge recommendation: **Approve with conditions**

- Blocking build breakers — **resolved** (commit `ec996c1`, build verified).
- Fast-follow before non-localhost exposure: **M1** (auth), **M3** (first-run crash).
- Track M2, M4–M7, L1–L5 from the backlog doc.
