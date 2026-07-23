import { NextRequest, NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";
import { parseLogCsv } from "@/src/lib/log-parser";
import { requireRole } from "@/src/lib/auth-helpers";

export async function GET() {
  const { data: rows } = await serviceClient
    .from("log_analyses")
    .select("*")
    .order("uploaded_at", { ascending: false });
  return NextResponse.json({ analyses: rows ?? [] });
}

export async function POST(req: NextRequest) {
  const denied = await requireRole(req, ["lead", "dev"]);
  if (denied) return denied;

  const formData = await req.formData();
  const file = formData.get("file") as File | null;

  if (!file) {
    return NextResponse.json({ error: "No file provided" }, { status: 400 });
  }

  if (!file.name.toLowerCase().endsWith(".csv")) {
    return NextResponse.json({ error: "Only CSV files are accepted" }, { status: 400 });
  }

  const csvText = await file.text();

  let result;
  try {
    result = parseLogCsv(csvText, file.name);
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : "Failed to parse CSV";
    return NextResponse.json({ error: msg }, { status: 400 });
  }

  const source = file.name.toLowerCase().startsWith("acuity") ? "acuity" : "zoho";
  const now = new Date().toISOString();

  const { data: analysisData, error: analysisError } = await serviceClient
    .from("log_analyses")
    .insert({
      filename: file.name,
      source,
      total_rows: result.analysis.total_rows,
      error_count: result.analysis.error_count,
      unique_errors: result.analysis.unique_errors,
      time_range_start: result.analysis.time_range_start,
      time_range_end: result.analysis.time_range_end,
      methods: result.analysis.methods,
      executive_summary: result.analysis.executive_summary,
      uploaded_at: now,
    })
    .select("id")
    .single();

  if (analysisError || !analysisData) {
    return NextResponse.json({ error: analysisError?.message ?? "Failed to create analysis" }, { status: 500 });
  }

  const analysisId = analysisData.id;

  const errorRows = result.errors.map((err) => ({
    analysis_id: analysisId,
    source,
    method: err.method,
    action: err.action,
    content: err.content,
    error_type: err.error_type.substring(0, 500),
    pattern_key: err.pattern_key,
    error_code: err.error_code,
    raw_message: err.raw_message.substring(0, 1000),
    timestamp: err.timestamp,
    is_error: err.is_error ? 1 : 0,
  }));

  const patternRows = result.patterns.map((pat) => ({
    analysis_id: analysisId,
    source,
    pattern_key: pat.pattern_key,
    sample_message: pat.sample_message.substring(0, 500),
    count: pat.count,
    first_seen: pat.first_seen,
    last_seen: pat.last_seen,
    severity: pat.severity,
  }));

  const anomalyRows = result.anomalies.map((anom) => ({
    analysis_id: analysisId,
    source,
    description: anom.description,
    severity: anom.severity,
    detected_at: anom.detected_at,
    error_count: anom.error_count,
    expected_count: anom.expected_count,
    deviation: anom.deviation,
  }));

  const inserts = [];
  if (errorRows.length > 0) inserts.push(serviceClient.from("log_errors").insert(errorRows));
  if (patternRows.length > 0) inserts.push(serviceClient.from("log_patterns").insert(patternRows));
  if (anomalyRows.length > 0) inserts.push(serviceClient.from("log_anomalies").insert(anomalyRows));
  await Promise.all(inserts);

  return NextResponse.json({ id: analysisId, source, ...result.analysis }, { status: 201 });
}
