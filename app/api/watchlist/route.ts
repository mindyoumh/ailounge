import { NextRequest, NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";
import { retroactivelyScore } from "@/src/lib/retroactive-scorer";
import { checkVulnerabilities, severityToRiskLevel } from "@/src/lib/cve-matcher";
import { detectEcosystem } from "@/src/lib/ecosystem-detector";
import { fetchLatestVersion } from "@/src/lib/version-fetcher";

export async function GET() {
  const { data: items } = await serviceClient
    .from("watchlist_items")
    .select("*")
    .order("category")
    .order("name");
  return NextResponse.json({ items });
}

export async function POST(req: NextRequest) {
  const body = await req.json();
  const { name, category, ecosystem, installed_version, latest_version, risk_level, risk_reason, upgrade_notes, known_vulns, migration_link } = body;

  if (!name) {
    return NextResponse.json({ error: "name is required" }, { status: 400 });
  }

  const validRisk = ["low", "medium", "high"];
  if (risk_level && !validRisk.includes(risk_level)) {
    return NextResponse.json({ error: "risk_level must be low, medium, or high" }, { status: 400 });
  }

  const { data, error } = await serviceClient.from("watchlist_items").insert({
    name,
    category: category || null,
    ecosystem: ecosystem || detectEcosystem(name),
    installed_version: installed_version || null,
    latest_version: latest_version || null,
    risk_level: risk_level || "low",
    risk_reason: risk_reason || null,
    upgrade_notes: upgrade_notes || null,
    known_vulns: known_vulns || null,
    migration_link: migration_link || null,
  }).select("id").single();

  if (error) {
    if (error.code === "23505") {
      return NextResponse.json({ error: "Item with this name already exists" }, { status: 409 });
    }
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  retroactivelyScore({ name: body.name, category: body.category || null })
    .catch((e) => console.error("retroactive scoring failed", e));

  const detectedEcosystem = ecosystem || detectEcosystem(name);

  fetchLatestVersion(name, detectedEcosystem).then(async (version) => {
    if (version) {
      await serviceClient
        .from("watchlist_items")
        .update({ latest_version: version })
        .eq("id", data.id);
    }
  }).catch((e) => console.error("version fetch failed", e));

  checkVulnerabilities(name, detectedEcosystem).then(async (result) => {
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
        risk_level: severityToRiskLevel(result.highestSeverity),
      })
      .eq("id", data.id);
  }).catch((e) => console.error("CVE check failed", e));

  return NextResponse.json({ ok: true, id: data.id }, { status: 201 });
}
