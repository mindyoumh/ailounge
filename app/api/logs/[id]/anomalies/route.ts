import { NextRequest, NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";
import { normalizeMessage } from "@/src/lib/log-parser";

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const { id } = await params;
  const numId = Number(id);
  const includePatterns = req.nextUrl.searchParams.get("include_patterns") === "true";
  const fromDate = req.nextUrl.searchParams.get("from_date");
  const toDate = req.nextUrl.searchParams.get("to_date");

  let query = serviceClient
    .from("log_anomalies")
    .select("*")
    .eq("analysis_id", numId);

  if (fromDate && toDate) {
    query = query.gte("detected_at", fromDate).lte("detected_at", toDate);
  }

  const { data: rows } = await query.order("deviation", { ascending: false });

  const anomalies = (rows ?? []) as Record<string, unknown>[];

  if (includePatterns && anomalies.length > 0) {
    for (const a of anomalies) {
      const detectedAt = a.detected_at as string;
      const { data: topErrors } = await serviceClient
        .from("log_errors")
        .select("error_type")
        .eq("analysis_id", numId)
        .eq("is_error", 1)
        .gte("timestamp", detectedAt?.substring(0, 10) + "T00:00:00")
        .lt("timestamp", detectedAt?.substring(0, 10) + "T23:59:59");

      const typeCount = new Map<string, number>();
      for (const e of topErrors ?? []) {
        typeCount.set(e.error_type, (typeCount.get(e.error_type) ?? 0) + 1);
      }
      const sorted = Array.from(typeCount.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3);

      a.top_patterns = sorted.map(([errorType, cnt]) => ({
        pattern_key: normalizeMessage(errorType),
        raw_sample: errorType,
        count: cnt,
        percentage: parseFloat(((cnt / (a.error_count as number)) * 100).toFixed(1)),
      }));
    }
  }

  return NextResponse.json({ anomalies });
}
