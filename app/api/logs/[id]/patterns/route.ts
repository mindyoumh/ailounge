import { NextRequest, NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const { id } = await params;
  const fromDate = req.nextUrl.searchParams.get("from_date");
  const toDate = req.nextUrl.searchParams.get("to_date");

  if (fromDate && toDate) {
    const numId = Number(id);
    const { data: errorRows } = await serviceClient
      .from("log_errors")
      .select("pattern_key, error_type, raw_message, timestamp")
      .eq("analysis_id", numId)
      .eq("is_error", 1)
      .gte("timestamp", fromDate)
      .lte("timestamp", toDate);

    const patternMap = new Map<string, {
      count: number;
      first_seen: string;
      last_seen: string;
      typeCounts: Map<string, number>;
    }>();

    for (const row of errorRows ?? []) {
      const key = row.pattern_key || "unknown";
      if (!patternMap.has(key)) {
        patternMap.set(key, {
          count: 0,
          first_seen: row.timestamp ?? "",
          last_seen: row.timestamp ?? "",
          typeCounts: new Map(),
        });
      }
      const entry = patternMap.get(key)!;
      entry.count++;
      if (row.timestamp && row.timestamp < entry.first_seen) entry.first_seen = row.timestamp;
      if (row.timestamp && row.timestamp > entry.last_seen) entry.last_seen = row.timestamp;
      if (row.error_type) {
        entry.typeCounts.set(row.error_type, (entry.typeCounts.get(row.error_type) ?? 0) + 1);
      }
    }

    const totalFiltered = errorRows?.length ?? 0;

    const patterns = Array.from(patternMap.entries())
      .map(([pattern_key, data], index) => {
        const sorted = Array.from(data.typeCounts.entries()).sort((a, b) => b[1] - a[1]);
        const sample = sorted.length > 0 ? sorted[0][0] : "Unknown";
        const ratio = totalFiltered > 0 ? data.count / totalFiltered : 0;
        return {
          id: -(index + 1),
          pattern_key,
          sample_message: sample.substring(0, 500),
          count: data.count,
          first_seen: data.first_seen,
          last_seen: data.last_seen,
          severity: ratio > 0.05 ? "high" : ratio > 0.01 ? "medium" : "low",
        };
      })
      .sort((a, b) => b.count - a.count);

    return NextResponse.json({ patterns });
  }

  const { data: rows } = await serviceClient
    .from("log_patterns")
    .select("*")
    .eq("analysis_id", Number(id))
    .order("count", { ascending: false });

  return NextResponse.json({ patterns: rows ?? [] });
}
