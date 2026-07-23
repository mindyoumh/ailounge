import { NextRequest, NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";
import { requireRole } from "@/src/lib/auth-helpers";

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const { id } = await params;
  const numId = Number(id);

  const { data: analysis } = await serviceClient
    .from("log_analyses")
    .select("*")
    .eq("id", numId)
    .single();

  if (!analysis) {
    return NextResponse.json({ error: "Analysis not found" }, { status: 404 });
  }

  const [
    { count: errorCount },
    { count: patternCount },
    { count: anomalyCount },
    { data: errorRows },
  ] = await Promise.all([
    serviceClient.from("log_errors").select("*", { count: "exact", head: true }).eq("analysis_id", numId).eq("is_error", 1),
    serviceClient.from("log_patterns").select("*", { count: "exact", head: true }).eq("analysis_id", numId),
    serviceClient.from("log_anomalies").select("*", { count: "exact", head: true }).eq("analysis_id", numId),
    serviceClient.from("log_errors").select("timestamp").eq("analysis_id", numId).eq("is_error", 1).order("timestamp"),
  ]);

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

  return NextResponse.json({
    ...analysis,
    errorCount: errorCount ?? 0,
    patternCount: patternCount ?? 0,
    anomalyCount: anomalyCount ?? 0,
    dailyCounts,
  });
}

export async function DELETE(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const err = await requireRole(req, ["lead", "dev"]);
  if (err) return err;

  const { id } = await params;
  const numId = Number(id);

  const { data: existing } = await serviceClient
    .from("log_analyses")
    .select("id")
    .eq("id", numId)
    .single();

  if (!existing) {
    return NextResponse.json({ error: "Analysis not found" }, { status: 404 });
  }

  await serviceClient.from("log_analyses").delete().eq("id", numId);
  return NextResponse.json({ ok: true });
}
