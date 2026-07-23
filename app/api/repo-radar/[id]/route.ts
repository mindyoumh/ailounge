import { NextRequest, NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";
import { requireRole } from "@/src/lib/auth-helpers";

export const dynamic = "force-dynamic";

export async function PATCH(request: Request, { params }: { params: Promise<{ id: string }> }) {
  try {
    const { id } = await params;
    const body = await request.json();

    const { data: item } = await serviceClient
      .from("repo_radar_items")
      .select("*")
      .eq("id", Number(id))
      .single();

    if (!item) {
      return Response.json({ error: "Not found" }, { status: 404 });
    }

    const allowed = ["notes", "is_active"];
    const updates: Record<string, string | number | null> = {};

    for (const field of allowed) {
      if (body[field] !== undefined) {
        updates[field] = body[field] as string | number;
      }
    }

    if (Object.keys(updates).length === 0) {
      return Response.json({ error: "No valid fields provided" }, { status: 400 });
    }

    updates.updated_at = new Date().toISOString();
    await serviceClient.from("repo_radar_items").update(updates).eq("id", Number(id));

    const { data: updated } = await serviceClient
      .from("repo_radar_items")
      .select("*")
      .eq("id", Number(id))
      .single();

    return Response.json({ ok: true, item: updated });
  } catch (err) {
    return Response.json({ error: String(err) }, { status: 500 });
  }
}

export async function DELETE(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const err = await requireRole(request, ["lead", "dev"]);
  if (err) return err;

  try {
    const { id } = await params;

    const { data: item } = await serviceClient
      .from("repo_radar_items")
      .select("id")
      .eq("id", Number(id))
      .single();

    if (!item) {
      return NextResponse.json({ error: "Not found" }, { status: 404 });
    }

    await serviceClient.from("repo_radar_items").delete().eq("id", Number(id));
    return NextResponse.json({ ok: true });
  } catch (err) {
    return NextResponse.json({ error: String(err) }, { status: 500 });
  }
}
