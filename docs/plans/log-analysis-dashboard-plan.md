# Log Analysis Dashboard

## Background

My-AILounge ingests engineering intelligence from multiple sources (Hacker News, RSS, GitHub Trending, manual feeds) into a unified dashboard. Sir Bo identified an opportunity to extend this capability to internal operational logs — specifically Acuity and Zoho logs — enabling the team to detect recurring issues, surface anomalies, and derive operational insights from the same dashboard interface used for engineering intelligence.

## Current Status

- **Shipped:** `/logs` (`app/logs/page.tsx`) and `components/logs/*` run a full pipeline — CSV upload, parsing, pattern detection, anomaly/spike detection, per-analysis executive summaries, and dynamic widgets (overview cards, error trend chart, source breakdown, pattern drill-down) — covering Phase 1 and Phase 2 scope, plus JSON/CSV/PDF export.
- **Not yet built:** the Future Enhancements below — additional log formats beyond Acuity/Zoho, scheduled/automated ingestion, cross-upload historical trend analysis, and feed-pipeline integration.
- **Open Questions:** still unresolved as written below; notably, no CSV size/row cap is enforced yet.

## Problem Statement

Engineering teams at Mind You spend time manually sifting through Acuity and Zoho logs to identify recurring errors, track down root causes, and understand system health. There is no centralized view that aggregates these logs, detects patterns, or highlights anomalies. Without this, incident response is reactive, error trends go unnoticed, and operational insights are buried in raw log files.

## Goals

- Provide a centralized dashboard for log analysis alongside existing engineering intelligence feeds.
- Identify recurring errors, top errors, patterns, and anomalies from Acuity and Zoho logs.
- Support CSV upload for ad-hoc analysis without requiring database changes.
- Generate executive summaries and dashboard metrics automatically from uploaded data.
- Keep analysis local — no external log aggregation services.

## Non-Goals

- Real-time log streaming or live tailing.
- Integration with external monitoring services (Datadog, Sentry, etc.).
- Log collection agents or daemons running on servers.
- Persistent storage of raw log data in the database.
- Authentication or multi-user access controls.
- Alerting or notification system.

## Phase 1 Scope

**Static demonstration dashboard** — hardcoded example metrics to validate the dashboard structure before building the analysis pipeline.

Deliverables:

- A new section or page in the dashboard dedicated to log analysis.
- Hardcoded example metrics demonstrating the widget layout.
- Widgets showing: total errors, top error types, error trend over time, sample error list.
- No CSV upload or real log parsing.

Success criteria:

- Dashboard renders correctly alongside existing engineering intelligence views.
- Widgets clearly communicate the intended structure for Phase 2.
- Team can evaluate and provide feedback on layout and metric choices.

## Phase 2 Scope

**Functional log analysis pipeline** — CSV upload, parsing, pattern detection, and metric generation.

Deliverables:

- CSV upload UI accepting Acuity and Zoho log exports.
- Backend parser that reads CSV columns, normalizes log entries, and extracts error types, timestamps, and frequency.
- Pattern detection framework that classifies errors by type, frequency, and time window.
- Anomaly detection identifying outliers (e.g., error spikes outside standard deviation).
- Executive summary generation — plain-English overview of the log analysis results.
- Dynamic dashboard widgets populated from analysis results.

Success criteria:

- Uploading a CSV produces dashboard metrics within seconds.
- Pattern detection identifies recurring errors with >80% accuracy on known test data.
- Executive summary is coherent and useful for a non-technical reader.

## Proposed Dashboard Widgets

| Widget                  | Description                                             | Phase |
| ----------------------- | ------------------------------------------------------- | ----- |
| **Error Overview Card** | Total errors, unique error types, time range covered    | 1     |
| **Top Errors Table**    | Ranked list of error messages by frequency              | 1     |
| **Error Trend Chart**   | Errors over time (bar or line chart by day/hour)        | 1     |
| **Error Timeline**      | Chronological feed of individual error entries          | 1     |
| **Pattern Groups**      | Grouped recurring errors with count and first/last seen | 2     |
| **Anomaly Alerts**      | Spikes or drops outside expected range flagged inline   | 2     |
| **Executive Summary**   | Generated plain-text summary of findings                | 2     |
| **Source Breakdown**    | Acuity vs Zoho split with per-source stats              | 2     |

## Pattern Detection Framework

The framework classifies and groups log entries using rule-based heuristics:

- **Exact match grouping**: Identical error messages grouped together with occurrence count, first/last seen timestamps.
- **Normalized grouping**: Error messages normalized by removing variable content (timestamps, IDs, numbers) then matched for similarity. Messages differing only in runtime values are grouped as the same pattern.
- **Temporal clustering**: Errors grouped by time window (hour, day) to identify bursts and trends.
- **Frequency baseline**: Rolling average of errors per time window. Anomalies flagged when count exceeds 2 standard deviations from baseline.

## Future Enhancements

- Support for additional log formats beyond Acuity and Zoho.
- Scheduled log ingestion via file drop or API.
- Persistent log storage and historical trend analysis across uploads.
- Integration with existing feed ingestion pipeline for operational alerts.
- Export analysis results as PDF or shareable link.

## Risks and Considerations

- **CSV format variability**: Acuity and Zoho may change export formats. The parser should validate columns and provide clear error messages on format mismatch.
- **Data size**: Large log exports could slow down parsing. Consider chunking or worker-based processing.
- **False positives in anomaly detection**: Simple statistical methods may flag expected patterns (e.g., daily deployment bursts) as anomalies. Tune baselines per source.
- **PII in logs**: Logs may contain user identifiers. The dashboard should not display raw PII in aggregated views.
- **Scope creep**: Phase 2 is substantial. Resist adding sources or features until the core pipeline is stable.

## Open Questions

1. Should Phase 2 include a database table for analysis results, or keep results in-memory / session-only?
2. What is the expected maximum CSV file size and row count?
3. Should the executive summary be generated client-side or via an API route with a template engine?
4. Should pattern grouping rules be configurable (e.g., regex-based exclusions) or hardcoded?
5. Where should the static Phase 1 dashboard live — as a new page route, a section on the existing dashboard, or a tab within a page?
6. Should the dashboard support downloading analysis results as JSON or CSV?
