# `/api/logs` — Log Analysis API

REST API for uploading CSV log files and retrieving parsed analysis results. Supports Zoho and Acuity error log formats.

## Routes

### `POST /api/logs` — Upload & analyze a CSV

- **Content-Type**: `multipart/form-data`
- **Body**: `file` field, `.csv` only
- **Source detection**: `acuity_*.csv` → `"acuity"`, otherwise `"zoho"`
- **Pipeline**: CSV → `csv-parse/sync` → `parseLogCsv()` → column detection → error extraction → pattern grouping → anomaly detection → Supabase PostgreSQL inserts (4 tables)
- **Response** `201`: `{ id, source, total_rows, error_count, ... }`
- **Errors** `400`: "No file provided", "Only CSV files are accepted", or parse failure message

### `GET /api/logs` — List all analyses

- Returns all `log_analyses` rows ordered by `uploaded_at DESC`
- **Response**: `{ analyses: [...] }`

### `GET /api/logs/[id]` — Single analysis detail

- Returns analysis metadata plus counts of related errors, patterns, and anomalies
- **Response**: `{ id, filename, source, ..., errorCount, patternCount, anomalyCount }`
- **Error** `404`: analysis not found

### `DELETE /api/logs/[id]` — Remove analysis

- **Role required:** `lead` or `dev` (gated by `requireRole(request, ["lead", "dev"])`)
- Cascading delete — removes the analysis row plus all related `log_errors`, `log_patterns`, `log_anomalies` rows
- **Response**: `{ ok: true }`
- **Error** `404`: analysis not found

### `GET /api/logs/[id]/errors` — Error rows (paginated)

- **Query params**: `limit` (default 50, max 200), `offset` (default 0), `is_error` (optional filter: `"0"` or `"1"`)
- Ordered by `timestamp DESC, id DESC`
- **Response**: `{ items: [...], total, limit, offset }`

### `GET /api/logs/[id]/patterns` — Grouped error patterns

- Returns patterns ordered by count descending
- Each pattern: `pattern_key`, `sample_message`, `count`, `first_seen`, `last_seen`, `severity`
- **Response**: `{ patterns: [...] }`

### `GET /api/logs/[id]/patterns/[pid]` — Pattern drill-down detail

- Returns per-pattern overview: timeline, method breakdown, and a sample error message
- `pid` is the URL-encoded `pattern_key` (contains `{var}` placeholders)
- **Matching strategy**: tries exact `pattern_key` match first, then falls back to `error_type LIKE` with `{var}` → `%`
- **Response**: `{ timeline: [...], methods: [...], sampleError: { ... } }`
- **Timeline**: error counts grouped by day for this pattern
- **Methods**: method name + count for errors matching this pattern

### `GET /api/logs/[id]/patterns/[pid]/errors` — Pattern-specific error rows (paginated)

- **Query params**: `limit` (default 50, max 200), `offset` (default 0)
- Returns error rows matching the pattern, ordered by `timestamp DESC`
- Uses same two-step matching strategy (exact `pattern_key`, fallback `error_type LIKE`)
- **Response**: `{ items: [...], total, limit, offset }`

### `GET /api/logs/[id]/trend` — Daily error trend

- Returns daily error counts grouped by date for the analysis
- Used by the `ErrorTrendChart` component for the Nivo bar chart
- **Response**: `{ trend: [{ date: string, count: number }] }`

### `GET /api/logs/[id]/anomalies` — Statistical spikes

- Returns anomalies ordered by deviation descending
- Each anomaly: `description`, `severity`, `detected_at`, `error_count`, `expected_count`, `deviation`
- Detection method: daily error counts > 2σ above mean; >3σ flagged as `"high"`, 2-3σ as `"medium"`
- **Response**: `{ anomalies: [...] }`

### `GET /api/logs/[id]/export/pdf` — Download PDF report

- Generates a professional multi-page PDF report server-side using `pdf-lib`
- **No query params** — always generates the full report
- **Report structure** (3 pages per analysis):
  - **Page 1** — Executive Brief: report header (filename, source, date range, generated timestamp), file information lines, executive summary (bullet points), key metrics grid (6 metrics: total rows, error count, error rate, unique patterns, anomaly spikes, avg errors/day), top findings table (#, severity label, finding title, count, first seen), recommendations (bullet points)
  - **Page 2** — Patterns & Anomalies: full top findings table (all patterns), anomaly spikes table (date, description, errors, baseline, × fold)
  - **Page 3** — Methods & Statistics: methods table (method name + count), supporting statistics table (11 metrics), end-of-report marker
- **Response** `200`: `Content-Type: application/pdf` with `Content-Disposition: attachment`
- **Error** `404`: analysis not found
- **Dependencies**: `pdf-lib` (StandardFonts Helvetica + Helvetica-Bold)

## Schema Notes

All routes use `getDb()` singleton (`@/src/db/client`). Related tables cascade on delete:

- `log_errors` → `analysis_id` references `log_analyses(id) ON DELETE CASCADE`
- `log_patterns` → `analysis_id` references `log_analyses(id) ON DELETE CASCADE`
- `log_anomalies` → `analysis_id` references `log_analyses(id) ON DELETE CASCADE`
