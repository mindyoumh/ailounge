import { NextRequest, NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";
import { fetchLatestVersion } from "@/src/lib/version-fetcher";

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

  const version = await fetchLatestVersion(item.name, item.ecosystem ?? "npm");

  if (!version) {
    return NextResponse.json({ error: "Could not fetch version" }, { status: 502 });
  }

  await serviceClient
    .from("watchlist_items")
    .update({ latest_version: version, updated_at: new Date().toISOString() })
    .eq("id", numId);

  const { data: updated } = await serviceClient
    .from("watchlist_items")
    .select("*")
    .eq("id", numId)
    .single();

  return NextResponse.json({ ok: true, version, item: updated });
}
