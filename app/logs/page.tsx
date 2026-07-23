"use client";

import { Suspense, useCallback, useEffect, useState } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import {
  ScrollText,
  Plus,
  Trash2,
  ChevronLeft,
  AlertTriangle,
  Loader2,
  BarChart3,
  Download,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { CsvUpload } from "@/components/logs/csv-upload";
import { OverviewCards } from "@/components/logs/overview-cards";
import { ErrorTrendChart } from "@/components/logs/error-trend-chart";
import { SourceBreakdown } from "@/components/logs/source-breakdown";
import { Skeleton } from "@/components/ui/skeleton";
import { PatternDrillDown } from "@/components/logs/pattern-drilldown";
import { SeverityLegend } from "@/components/logs/severity-legend";
import { DateFilter, getDateRange } from "@/components/logs/date-filter";
import { SeverityFilter } from "@/components/logs/severity-filter";
import { PatternSearch } from "@/components/logs/pattern-search";
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from "@/components/ui/dropdown-menu";

interface Analysis {
  id: number;
  filename: string;
  source: string;
  uploaded_at: string;
  total_rows: number;
  error_count: number;
  unique_errors: number;
  time_range_start: string;
  time_range_end: string;
  methods: string;
  executive_summary: string;
  errorCount?: number;
  patternCount?: number;
  anomalyCount?: number;
  dailyCounts?: { day: string; count: number }[];
}

interface Pattern {
  id: number;
  pattern_key: string;
  sample_message: string;
  count: number;
  first_seen: string;
  last_seen: string;
  severity: string;
}

interface AnomalyPattern {
  pattern_key: string;
  raw_sample: string;
  count: number;
  percentage: number;
}

interface Anomaly {
  id: number;
  description: string;
  severity: string;
  detected_at: string;
  error_count: number;
  expected_count: number;
  deviation: number;
  top_patterns?: AnomalyPattern[];
}

function severityBadge(severity: string) {
  const colors: Record<string, string> = {
    high: "bg-red-500/10 text-red-600 dark:text-red-400 border-red-500/30",
    medium:
      "bg-amber-500/10 text-amber-600 dark:text-amber-400 border-amber-500/30",
    low: "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border-emerald-500/30",
  };
  return colors[severity] || colors.low;
}

export const dynamic = "force-dynamic";

export default function LogsPage() {
  return (
    <Suspense
      fallback={
        <div className="min-h-screen bg-background">
          <div className="mx-auto max-w-7xl px-6 py-8 space-y-2">
            <Skeleton className="h-8 w-48" />
            <Skeleton className="h-32 w-full" />
            <Skeleton className="h-32 w-full" />
          </div>
        </div>
      }
    >
      <LogsContent />
    </Suspense>
  );
}

function LogsContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const analysisId = searchParams.get("id");

  const [analyses, setAnalyses] = useState<Analysis[]>([]);
  const [loading, setLoading] = useState(true);
  const [detail, setDetail] = useState<Analysis | null>(null);
  const [patterns, setPatterns] = useState<Pattern[]>([]);
  const [anomalies, setAnomaly] = useState<Anomaly[]>([]);
  const [trendDailyCounts, setTrendDailyCounts] = useState<{ day: string; count: number }[]>([]);
  const [trendPeriod, setTrendPeriod] = useState("all");
  const [patternsPeriod, setPatternsPeriod] = useState("all");
  const [patternsSeverity, setPatternsSeverity] = useState("all");
  const [patternsSearch, setPatternsSearch] = useState("");
  const [anomaliesSearch, setAnomaliesSearch] = useState("");
  const [anomaliesPeriod, setAnomaliesPeriod] = useState("all");
  const [trendCustomRange, setTrendCustomRange] = useState<{ from: string; to: string } | null>(null);
  const [patternsCustomRange, setPatternsCustomRange] = useState<{ from: string; to: string } | null>(null);
  const [anomaliesCustomRange, setAnomaliesCustomRange] = useState<{ from: string; to: string } | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [showUpload, setShowUpload] = useState(false);
  const [deleting, setDeleting] = useState<number | null>(null);

  const fetchAnalyses = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch("/api/logs");
      const data = await res.json();
      setAnalyses(data.analyses || []);
    } finally {
      setLoading(false);
    }
  }, []);

  const getEffectiveRange = (period: string, customRange: { from: string; to: string } | null): { from_date?: string; to_date?: string } => {
    if (period === "custom" && customRange) {
      return {
        from_date: new Date(customRange.from + "T00:00:00").toISOString(),
        to_date: new Date(customRange.to + "T23:59:59").toISOString(),
      };
    }
    return getDateRange(period);
  };

  const fetchDetail = useCallback(async (id: number) => {
    setLoadingDetail(true);
    try {
      const res = await fetch(`/api/logs/${id}`);
      if (res.ok) setDetail(await res.json());
    } finally {
      setLoadingDetail(false);
    }
  }, []);

  const fetchTrendData = useCallback(async (id: number) => {
    const dateRange = getEffectiveRange(trendPeriod, trendCustomRange);
    const params = new URLSearchParams();
    if (dateRange.from_date) params.set("from_date", dateRange.from_date);
    if (dateRange.to_date) params.set("to_date", dateRange.to_date);
    const qs = params.toString();
    const res = await fetch(`/api/logs/${id}/trend${qs ? `?${qs}` : ""}`);
    if (res.ok) {
      const data = await res.json();
      setTrendDailyCounts(data.dailyCounts || []);
    }
  }, [trendPeriod, trendCustomRange]);

  const fetchPatterns = useCallback(async (id: number) => {
    const dateRange = getEffectiveRange(patternsPeriod, patternsCustomRange);
    const params = new URLSearchParams();
    if (dateRange.from_date) params.set("from_date", dateRange.from_date);
    if (dateRange.to_date) params.set("to_date", dateRange.to_date);
    const qs = params.toString();
    const res = await fetch(`/api/logs/${id}/patterns${qs ? `?${qs}` : ""}`);
    if (res.ok) {
      const data = await res.json();
      setPatterns(data.patterns || []);
    }
  }, [patternsPeriod, patternsCustomRange]);

  const fetchAnomalies = useCallback(async (id: number) => {
    const dateRange = getEffectiveRange(anomaliesPeriod, anomaliesCustomRange);
    const params = new URLSearchParams();
    if (dateRange.from_date) params.set("from_date", dateRange.from_date);
    if (dateRange.to_date) params.set("to_date", dateRange.to_date);
    const qs = params.toString();
    const res = await fetch(`/api/logs/${id}/anomalies?include_patterns=true${qs ? `&${qs}` : ""}`);
    if (res.ok) {
      const data = await res.json();
      setAnomaly(data.anomalies || []);
    }
  }, [anomaliesPeriod, anomaliesCustomRange]);

  useEffect(() => {
    fetchAnalyses();
  }, [fetchAnalyses]);

  useEffect(() => {
    if (analysisId) {
      fetchDetail(Number(analysisId));
    } else {
      setDetail(null);
      setTrendDailyCounts([]);
      setPatterns([]);
      setAnomaly([]);
    }
  }, [analysisId, fetchDetail]);

  useEffect(() => {
    if (analysisId) fetchTrendData(Number(analysisId));
  }, [analysisId, fetchTrendData]);

  useEffect(() => {
    if (analysisId) fetchPatterns(Number(analysisId));
  }, [analysisId, fetchPatterns]);

  useEffect(() => {
    if (analysisId) fetchAnomalies(Number(analysisId));
  }, [analysisId, fetchAnomalies]);

  const buildExportPayload = () => ({
    id: detail?.id,
    filename: detail?.filename,
    source: detail?.source,
    uploaded_at: detail?.uploaded_at,
    total_rows: detail?.total_rows,
    error_count: detail?.error_count,
    unique_errors: detail?.unique_errors,
    time_range_start: detail?.time_range_start,
    time_range_end: detail?.time_range_end,
    methods: detail?.methods,
    executive_summary: detail?.executive_summary,
    patterns,
    anomalies,
  });

  const handleExportJSON = () => {
    const payload = buildExportPayload();
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `analysis-${detail?.id}-report.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleExportCSV = () => {
    const rows: string[][] = [["pattern_key", "sample_message", "count", "first_seen", "last_seen", "severity"]];
    for (const p of patterns) {
      rows.push([p.pattern_key, p.sample_message, String(p.count), p.first_seen, p.last_seen, p.severity]);
    }
    rows.push([]);
    rows.push(["anomaly_day", "description", "severity", "error_count", "expected_count", "deviation"]);
    for (const a of anomalies) {
      rows.push([a.detected_at, a.description, a.severity, String(a.error_count), String(a.expected_count), String(a.deviation)]);
    }
    const csv = rows.map((r) => r.map((c) => `"${c.replace(/"/g, '""')}"`).join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `analysis-${detail?.id}-report.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleExportPDF = () => {
    window.open(`/api/logs/${detail?.id}/export/pdf`, "_blank");
  };

  const handleDelete = async (id: number) => {
    if (
      !confirm(
        "Delete this analysis? All associated data will be permanently removed.",
      )
    )
      return;
    setDeleting(id);
    try {
      await fetch(`/api/logs/${id}`, { method: "DELETE" });
      if (analysisId === String(id)) router.push("/logs");
      await fetchAnalyses();
    } finally {
      setDeleting(null);
    }
  };

  if (analysisId && detail) {
    let methodsList: { method: string; count: number }[] = [];
    try {
      methodsList = JSON.parse(detail.methods || "[]");
    } catch {}

    const filteredPatterns = patterns
      .filter((p) => patternsSeverity === "all" || p.severity === patternsSeverity)
      .filter((p) => {
        if (!patternsSearch) return true;
        const q = patternsSearch.toLowerCase();
        return (
          p.sample_message.toLowerCase().includes(q) ||
          p.pattern_key.toLowerCase().includes(q) ||
          String(p.count).includes(q)
        );
      });

    const filteredAnomalies = !anomaliesSearch
      ? anomalies
      : anomalies.filter((a) => {
          const q = anomaliesSearch.toLowerCase();
          return (
            a.description.toLowerCase().includes(q) ||
            a.severity.toLowerCase().includes(q) ||
            String(a.error_count).includes(q) ||
            String(a.expected_count).includes(q) ||
            a.detected_at.toLowerCase().includes(q) ||
            (a.top_patterns ?? []).some(
              (tp) =>
                tp.raw_sample.toLowerCase().includes(q) ||
                tp.pattern_key.toLowerCase().includes(q),
            )
          );
        });

    const trendData = (trendDailyCounts || []).map((d) => ({
      date: d.day,
      errors: d.count,
    }));

    return (
      <div className="mx-auto max-w-7xl px-4 md:px-6 py-6 md:py-8">
        <div className="mb-6 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Button
              variant="ghost"
              size="icon"
              onClick={() => router.push("/logs")}
              className="h-8 w-8"
            >
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <div>
              <h1 className="text-xl font-bold tracking-tight">
                {detail.filename}
              </h1>
              <p className="text-xs text-muted-foreground">
                {detail.source} &middot;{" "}
                {new Date(detail.uploaded_at).toLocaleString()}
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" size="sm">
                  <Download className="h-4 w-4" />
                  Export
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem onClick={handleExportJSON}>
                  Export as JSON
                </DropdownMenuItem>
                <DropdownMenuItem onClick={handleExportCSV}>
                  Export as CSV
                </DropdownMenuItem>
                <DropdownMenuItem onClick={handleExportPDF}>
                  Export as PDF
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
            <Button
              variant="destructive"
              size="sm"
              onClick={() => handleDelete(detail.id)}
              disabled={deleting === detail.id}
            >
              {deleting === detail.id ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Trash2 className="h-4 w-4" />
              )}
              Delete
            </Button>
          </div>
        </div>

        <OverviewCards
          totalRows={detail.total_rows}
          errorCount={detail.error_count}
          uniqueErrors={detail.unique_errors}
          timeRangeStart={detail.time_range_start}
          timeRangeEnd={detail.time_range_end}
        />

        <div className="mt-6 grid gap-6 md:grid-cols-2">
          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm font-medium">Error Trend</CardTitle>
                <DateFilter value={trendPeriod} onChange={setTrendPeriod} customDateRange={trendCustomRange} onCustomDateRangeApply={setTrendCustomRange} />
              </div>
            </CardHeader>
            <CardContent>
              <ErrorTrendChart data={trendData} />
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium">
                Source Breakdown
              </CardTitle>
            </CardHeader>
            <CardContent>
              <SourceBreakdown
                acuityErrors={
                  detail.source === "acuity" ? detail.error_count : 0
                }
                zohoErrors={detail.source === "zoho" ? detail.error_count : 0}
              />
            </CardContent>
          </Card>
        </div>

        <div className="mt-6 grid gap-6 md:grid-cols-2">
          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm font-medium">
                  Top Error Patterns
                </CardTitle>
                <div className="flex items-center gap-2">
                  <SeverityFilter value={patternsSeverity} onChange={setPatternsSeverity} />
                  <DateFilter value={patternsPeriod} onChange={setPatternsPeriod} customDateRange={patternsCustomRange} onCustomDateRangeApply={setPatternsCustomRange} />
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <PatternSearch value={patternsSearch} onChange={setPatternsSearch} placeholder="Search error patterns..." />
              <SeverityLegend className="mt-3 mb-3" />
              {filteredPatterns.length === 0 ? (
                patterns.length === 0 ? (
                  <p className="py-8 text-center text-sm text-muted-foreground">
                    No patterns detected
                  </p>
                ) : (
                  <div className="py-8 text-center">
                    <p className="text-sm text-muted-foreground">No matching patterns found</p>
                    <p className="text-xs text-muted-foreground/60 mt-1">Try adjusting the search, severity filter, or date range.</p>
                  </div>
                )
              ) : (
                <div className="space-y-2">
                  {filteredPatterns.slice(0, 10).map((p) => (
                  <button
                    key={p.id}
                    className="w-full rounded-lg border px-3 py-2 text-left transition-colors hover:border-accent-vibrant/30"
                    onClick={() => {
                      const params = new URLSearchParams(searchParams.toString());
                      params.set("pattern", String(p.id));
                      router.push(`/logs?${params.toString()}`);
                    }}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <p className="min-w-0 flex-1 truncate text-sm font-medium">
                        {p.sample_message}
                      </p>
                      <Badge
                        className={`shrink-0 text-[10px] ${severityBadge(p.severity)}`}
                      >
                        {p.count}
                      </Badge>
                    </div>
                    <p className="mt-0.5 text-[11px] text-muted-foreground">
                      {p.first_seen.substring(0, 10)} —{" "}
                      {p.last_seen.substring(0, 10)}
                    </p>
                  </button>
                ))}
              </div>
              )}
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm font-medium">
                  Anomaly Spikes
                </CardTitle>
                <DateFilter value={anomaliesPeriod} onChange={setAnomaliesPeriod} customDateRange={anomaliesCustomRange} onCustomDateRangeApply={setAnomaliesCustomRange} />
              </div>
            </CardHeader>
            <CardContent>
              <PatternSearch value={anomaliesSearch} onChange={setAnomaliesSearch} placeholder="Search anomalies..." />
              {filteredAnomalies.length === 0 ? (
                anomalies.length === 0 ? (
                  <p className="py-8 text-center text-sm text-muted-foreground">
                    No anomalies detected
                  </p>
                ) : (
                  <div className="py-8 text-center">
                    <p className="text-sm text-muted-foreground">No matching anomalies found</p>
                    <p className="text-xs text-muted-foreground/60 mt-1">Try adjusting the search or date range.</p>
                  </div>
                )
              ) : (
                <div className="space-y-2 mt-3">
                  {filteredAnomalies.map((a, i) => (
                  <div
                    key={i}
                    className="rounded-lg border px-3 py-2"
                  >
                    <div className="flex items-start gap-3">
                      <AlertTriangle
                        className={`mt-0.5 h-4 w-4 shrink-0 ${a.severity === "high" ? "text-red-500" : "text-amber-500"}`}
                      />
                      <div className="min-w-0 flex-1">
                        <p className="text-sm font-medium">{a.description}</p>
                        <p className="text-xs text-muted-foreground mt-0.5">
                          {a.error_count} errors vs{" "}
                          {Math.round(a.expected_count)} expected
                        </p>
                      </div>
                      <Badge
                        className={`shrink-0 text-[10px] ${severityBadge(a.severity)}`}
                      >
                        {a.severity}
                      </Badge>
                    </div>
                    {a.top_patterns && a.top_patterns.length > 0 && (
                      <>
                        <Separator className="my-1.5" />
                        <div className="space-y-0.5">
                          <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1">
                            Top Patterns
                          </p>
                          {a.top_patterns.map((p, j) => (
                            <div key={j} className="space-y-0.5">
                              <div className="flex items-center justify-between text-[11px]">
                                <span className="min-w-0 flex-1 truncate mr-2" title={p.pattern_key}>
                                  {p.pattern_key}
                                </span>
                                <span className="shrink-0 tabular-nums text-muted-foreground">
                                  {p.count} ({p.percentage}%)
                                </span>
                              </div>
                              <p className="truncate text-[10px] text-muted-foreground/60 italic pl-1" title={p.raw_sample}>
                                {p.raw_sample}
                              </p>
                            </div>
                          ))}
                        </div>
                      </>
                    )}
                  </div>
                ))}
              </div>
              )}
            </CardContent>
          </Card>
        </div>

        {detail.executive_summary && (
          <Card className="mt-6">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium">
                Executive Summary
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-muted-foreground leading-relaxed">
                {detail.executive_summary}
              </p>
            </CardContent>
          </Card>
        )}

        {methodsList.length > 0 && (
          <Card className="mt-6">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium">Methods</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-1">
                {methodsList.map((m, i) => (
                  <div
                    key={i}
                    className="flex items-center justify-between text-sm"
                  >
                    <span className="font-mono text-xs truncate">
                      {m.method}
                    </span>
                    <span className="tabular-nums text-muted-foreground ml-4">
                      {m.count}
                    </span>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        )}

        <PatternDrillDown analysisId={analysisId} />
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-7xl px-4 md:px-6 py-6 md:py-8">
      <div className="flex items-start justify-between gap-4 flex-wrap mb-6">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <ScrollText className="h-5 w-5 text-accent-vibrant shrink-0" />
            <h1 className="text-3xl font-bold tracking-tight">Log Analysis</h1>
          </div>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <button
            onClick={() => setShowUpload(!showUpload)}
            className="inline-flex items-center gap-1.5 rounded-lg border border-border bg-card px-3 py-2 text-xs font-medium text-muted-foreground transition-all duration-200 hover:bg-accent hover:text-foreground"
          >
            <Plus className="h-3.5 w-3.5" />
            New Analysis
          </button>
        </div>
      </div>

      {showUpload && (
        <Card className="mb-6">
          <CardContent className="pt-6">
            <CsvUpload
              onAnalyzed={() => {
                setShowUpload(false);
                fetchAnalyses();
              }}
            />
          </CardContent>
        </Card>
      )}

      {loading ? (
        <div className="space-y-3">
          {[1, 2, 3].map((i) => (
            <Skeleton key={i} className="h-20 w-full rounded-lg" />
          ))}
        </div>
      ) : analyses.length === 0 ? (
        <Card>
          <CardContent className="py-12 text-center">
            <BarChart3 className="mx-auto mb-3 h-8 w-8 text-muted-foreground" />
            <p className="text-sm text-muted-foreground">No analyses yet</p>
            <p className="mt-1 text-xs text-muted-foreground">
              Upload an Acuity or Zoho CSV log export to get started
            </p>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-3">
          {analyses.map((a) => (
            <Card
              key={a.id}
              className="cursor-pointer transition-colors hover:border-accent-vibrant/30"
              onClick={() => router.push(`/logs?id=${a.id}`)}
            >
              <CardContent className="flex items-center justify-between py-4">
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <p className="truncate text-sm font-medium">{a.filename}</p>
                    <Badge variant="secondary" className="text-[10px]">
                      {a.source}
                    </Badge>
                  </div>
                  <p className="mt-0.5 text-xs text-muted-foreground">
                    {a.total_rows.toLocaleString()} rows &middot;{" "}
                    {a.error_count.toLocaleString()} errors &middot;{" "}
                    {new Date(a.uploaded_at).toLocaleDateString()}
                  </p>
                </div>
                <div className="flex items-center gap-2 shrink-0 ml-4">
                  <Badge
                    className={
                      a.error_count > 0
                        ? "bg-red-500/10 text-red-600 dark:text-red-400"
                        : "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400"
                    }
                  >
                    {a.error_count > 0
                      ? `${((a.error_count / a.total_rows) * 100).toFixed(1)}% errors`
                      : "No errors"}
                  </Badge>
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDelete(a.id);
                    }}
                    disabled={deleting === a.id}
                    className="rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-destructive/10 hover:text-destructive"
                  >
                    {deleting === a.id ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Trash2 className="h-4 w-4" />
                    )}
                  </button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
