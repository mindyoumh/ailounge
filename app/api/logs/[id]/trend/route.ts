import { NextRequest, NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const { id } = await params;
  const numId = Number(id);
  const fromDate = req.nextUrl.searchParams.get("from_date");
  const toDate = req.nextUrl.searchParams.get("to_date");

  let query = serviceClient
    .from("log_errors")
    .select("timestamp")
    .eq("analysis_id", numId)
    .eq("is_error", 1)
    .order("timestamp");

  if (fromDate && toDate) {
    query = query.gte("timestamp", fromDate).lte("timestamp", toDate);
  }

  const { data: errorRows } = await query;

  const dailyCounts: { day: string; count: number }[] = [];
  const dayMap = new Map<string, number>();
  for (const row of errorRows ?? []) {
    const day = row.timestamp ? row.timestamp.substring(0, 10) : "unknown";
    dayMap.set(day, (dayMap.get(day) ?? 0) + 1);
  }
  for (const [day, count] of dayMap) {
    dailyCounts.push({ day, count });
  }
  dailyCounts.sort((a, b) => a.day.localeCompare(b.day));

  return NextResponse.json({ dailyCounts });
}
