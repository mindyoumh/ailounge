import { NextRequest, NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";

export const dynamic = "force-dynamic";

function patternToLike(pid: string): string {
  return pid.replace(/[%_]/g, "\\$&").replace(/\{var\}/g, "%");
}

async function resolvePatternKey(
  analysisId: number,
  pid: string,
): Promise<string | null> {
  const num = parseInt(pid, 10);
  if (!isNaN(num)) {
    const { data: row } = await serviceClient
      .from("log_patterns")
      .select("pattern_key")
      .eq("id", num)
      .eq("analysis_id", analysisId)
      .single();
    if (row) return row.pattern_key;
  }
  return pid;
}

async function countMatchingRows(
  analysisId: number,
  patternKey: string,
): Promise<number> {
  const { count: byKey } = await serviceClient
    .from("log_errors")
    .select("*", { count: "exact", head: true })
    .eq("analysis_id", analysisId)
    .eq("pattern_key", patternKey);
  if (byKey && byKey > 0) return byKey;

  const prefix = patternToLike(patternKey.substring(0, 400)) + "%";
  const { count: byPrefix } = await serviceClient
    .from("log_errors")
    .select("*", { count: "exact", head: true })
    .eq("analysis_id", analysisId)
    .like("pattern_key", prefix);
  if (byPrefix && byPrefix > 0) return byPrefix;

  const like = patternToLike(patternKey);
  const { count: byLike } = await serviceClient
    .from("log_errors")
    .select("*", { count: "exact", head: true })
    .eq("analysis_id", analysisId)
    .like("error_type", like);
  return byLike ?? 0;
}

async function queryErrorRows(
  analysisId: number,
  patternKey: string,
  limit: number,
  offset: number,
) {
  const { data: byKey } = await serviceClient
    .from("log_errors")
    .select("id, method, action, content, error_type, error_code, raw_message, timestamp, is_error")
    .eq("analysis_id", analysisId)
    .eq("pattern_key", patternKey)
    .order("timestamp")
    .range(offset, offset + limit - 1);

  if (byKey && byKey.length > 0) return byKey as any[];

  const prefix = patternToLike(patternKey.substring(0, 400)) + "%";
  const { data: byPrefix } = await serviceClient
    .from("log_errors")
    .select("id, method, action, content, error_type, error_code, raw_message, timestamp, is_error")
    .eq("analysis_id", analysisId)
    .like("pattern_key", prefix)
    .order("timestamp")
    .range(offset, offset + limit - 1);

  if (byPrefix && byPrefix.length > 0) return byPrefix as any[];

  const like = patternToLike(patternKey);
  const { data: byLike } = await serviceClient
    .from("log_errors")
    .select("id, method, action, content, error_type, error_code, raw_message, timestamp, is_error")
    .eq("analysis_id", analysisId)
    .like("error_type", like)
    .order("timestamp")
    .range(offset, offset + limit - 1);

  return (byLike ?? []) as any[];
}

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ id: string; pid: string }> },
) {
  const { id, pid } = await params;
  const url = new URL(req.url);
  const page = Math.max(1, parseInt(url.searchParams.get("page") || "1", 10));
  const limit = Math.min(100, Math.max(1, parseInt(url.searchParams.get("limit") || "50", 10)));
  const offset = (page - 1) * limit;
  const numId = Number(id);

  const { data: analysis } = await serviceClient
    .from("log_analyses")
    .select("id")
    .eq("id", numId)
    .single();
  if (!analysis) {
    return NextResponse.json({ error: "Analysis not found" }, { status: 404 });
  }

  const patternKey = (await resolvePatternKey(numId, pid)) ?? pid;
  const total = await countMatchingRows(numId, patternKey);
  if (total === 0) {
    return NextResponse.json({ error: "Pattern not found" }, { status: 404 });
  }

  const rows = await queryErrorRows(numId, patternKey, limit, offset);

  return NextResponse.json({
    rows,
    pagination: {
      page,
      limit,
      total,
      total_pages: Math.ceil(total / limit),
    },
  });
}
