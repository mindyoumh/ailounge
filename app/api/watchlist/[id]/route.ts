import { NextRequest, NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";
import { requireRole } from "@/src/lib/auth-helpers";

const VALID_FIELDS = [
  "name", "category", "ecosystem", "installed_version", "latest_version",
  "risk_level", "risk_reason", "upgrade_notes", "known_vulns", "migration_link",
] as const;

export async function PATCH(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const body = await req.json();

  const { data: item } = await serviceClient.from("watchlist_items").select("*").eq("id", Number(id)).single();
  if (!item) {
    return NextResponse.json({ error: "Item not found" }, { status: 404 });
  }

  if (body.risk_level !== undefined) {
    const validRisk = ["low", "medium", "high"];
    if (!validRisk.includes(body.risk_level)) {
      return NextResponse.json({ error: "risk_level must be low, medium, or high" }, { status: 400 });
    }
  }

  const updates: Record<string, string | number | null> = {};
  for (const field of VALID_FIELDS) {
    if (body[field] !== undefined) updates[field] = body[field] as string | number;
  }

  if (Object.keys(updates).length === 0) {
    return NextResponse.json({ error: "No fields to update" }, { status: 400 });
  }

  updates.updated_at = new Date().toISOString();
  await serviceClient.from("watchlist_items").update(updates).eq("id", Number(id));

  const { data: updated } = await serviceClient.from("watchlist_items").select("*").eq("id", Number(id)).single();
  return NextResponse.json({ ok: true, item: updated });
}

export async function DELETE(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const err = await requireRole(req, ["lead", "dev"]);
  if (err) return err;

  const { id } = await params;

  const { data: item } = await serviceClient.from("watchlist_items").select("id").eq("id", Number(id)).single();
  if (!item) {
    return NextResponse.json({ error: "Item not found" }, { status: 404 });
  }

  await serviceClient.from("watchlist_items").delete().eq("id", Number(id));
  return NextResponse.json({ ok: true });
}
