import { NextRequest, NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";
import { checkVulnerabilities } from "@/src/lib/cve-matcher";

export async function POST(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const { id } = await params;
  const numId = Number(id);

  const { data: item } = await serviceClient
    .from("watchlist_items")
    .select("name, ecosystem")
    .eq("id", numId)
    .single();

  if (!item) {
    return NextResponse.json({ error: "Item not found" }, { status: 404 });
  }

  const result = await checkVulnerabilities(item.name, item.ecosystem ?? "npm");

  const payload = {
    lastChecked: new Date().toISOString(),
    totalCount: result.totalCount,
    highestSeverity: result.highestSeverity,
    summaryText: result.summaryText,
    cves: result.cves,
  };

  await serviceClient
    .from("watchlist_items")
    .update({
      known_vulns: JSON.stringify(payload),
      risk_level: result.highestSeverity === "CRITICAL" || result.highestSeverity === "HIGH"
        ? "high"
        : result.highestSeverity === "MODERATE"
          ? "medium"
          : undefined,
    })
    .eq("id", numId);

  const { data: updated } = await serviceClient
    .from("watchlist_items")
    .select("*")
    .eq("id", numId)
    .single();

  return NextResponse.json({ ok: true, cves: result.cves, item: updated });
}
