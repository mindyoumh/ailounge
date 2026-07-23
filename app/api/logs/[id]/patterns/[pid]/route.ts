import { NextRequest, NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";

export const dynamic = "force-dynamic";

function patternToLike(pid: string): string {
  return pid.replace(/[%_]/g, "\\$&").replace(/\{var\}/g, "%");
}

async function resolvePatternKey(
  analysisId: number,
  pid: string,
): Promise<string> {
  const num = parseInt(pid, 10);
  if (!isNaN(num)) {
    const { data: row } = await serviceClient
      .from("log_patterns")
      .select("pattern_key")
      .eq("id", num)
      .eq("analysis_id", analysisId)
      .single();
    if (row?.pattern_key) return row.pattern_key;
  }
  return pid;
}

async function queryPatternRows(
  analysisId: number,
  patternKey: string,
): Promise<Array<{
  id: number;
  method: string;
  action: string;
  content: string;
  error_type: string;
  error_code: string;
  raw_message: string;
  timestamp: string;
  is_error: number;
}>> {
  const { data: byKey } = await serviceClient
    .from("log_errors")
    .select("id, method, action, content, error_type, error_code, raw_message, timestamp, is_error")
    .eq("analysis_id", analysisId)
    .eq("pattern_key", patternKey)
    .order("timestamp")
    .limit(5000);

  if (byKey && byKey.length > 0) return byKey as any;

  const prefix = patternToLike(patternKey.substring(0, 400)) + "%";
  const { data: byPrefix } = await serviceClient
    .from("log_errors")
    .select("id, method, action, content, error_type, error_code, raw_message, timestamp, is_error")
    .eq("analysis_id", analysisId)
    .like("pattern_key", prefix)
    .order("timestamp")
    .limit(5000);

  if (byPrefix && byPrefix.length > 0) return byPrefix as any;

  const like = patternToLike(patternKey);
  const { data: byLike } = await serviceClient
    .from("log_errors")
    .select("id, method, action, content, error_type, error_code, raw_message, timestamp, is_error")
    .eq("analysis_id", analysisId)
    .like("error_type", like)
    .order("timestamp")
    .limit(5000);

  return (byLike ?? []) as any;
}

function describePattern(pid: string): string {
  const noVar = pid.replace(/\{var\}/g, "");
  const key = noVar.replace(/-/g, " ").trim();
  return key.charAt(0).toUpperCase() + key.slice(1) || pid;
}

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string; pid: string }> },
) {
  const { id, pid } = await params;
  const numId = Number(id);

  const { data: analysis } = await serviceClient
    .from("log_analyses")
    .select("id")
    .eq("id", numId)
    .single();
  if (!analysis) {
    return NextResponse.json({ error: "Analysis not found" }, { status: 404 });
  }

  const patternKey = await resolvePatternKey(numId, pid);
  const rows = await queryPatternRows(numId, patternKey);
  if (rows.length === 0) {
    return NextResponse.json({ error: "Pattern not found" }, { status: 404 });
  }

  const methodCount = new Map<string, number>();
  for (const r of rows) {
    if (r.method) methodCount.set(r.method, (methodCount.get(r.method) || 0) + 1);
  }
  const methods = Array.from(methodCount.entries()).map(([method, count]) => ({ method, count }));

  const dateCount = new Map<string, number>();
  for (const r of rows) {
    const d = r.timestamp ? r.timestamp.substring(0, 10) : "unknown";
    dateCount.set(d, (dateCount.get(d) || 0) + 1);
  }
  const timeline = Array.from(dateCount.entries())
    .map(([date, count]) => ({ date, count }))
    .sort((a, b) => a.date.localeCompare(b.date));

  return NextResponse.json({
    pattern_key: patternKey,
    description: describePattern(patternKey),
    total: rows.length,
    errors_only: rows.filter((r) => r.is_error).length,
    timeline,
    methods,
  });
}
