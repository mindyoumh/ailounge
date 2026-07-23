import { NextRequest } from "next/server";
import { serviceClient } from "@/src/db/service-client";
import { requireRole } from "@/src/lib/auth-helpers";

export const dynamic = "force-dynamic";

export async function GET(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const { data: item } = await serviceClient.from("prompts").select("*").eq("id", Number(id)).single();
  if (!item) {
    return Response.json({ error: "Prompt not found" }, { status: 404 });
  }
  return Response.json({ item });
}

export async function PATCH(request: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const denied = await requireRole(request, ["lead", "dev"]);
  if (denied) return denied;

  try {
    const { id } = await params;
    const body = await request.json();

    const { data: existing } = await serviceClient.from("prompts").select("id").eq("id", Number(id)).single();
    if (!existing) {
      return Response.json({ error: "Prompt not found" }, { status: 404 });
    }

    const fields = ["title", "content", "category", "description", "input_fields", "output_description", "model_recommendation", "is_featured"];
    const updates: Record<string, string | number | null> = {};

    for (const field of fields) {
      if (body[field] !== undefined) updates[field] = body[field] as string | number;
    }

    if (Object.keys(updates).length === 0) {
      return Response.json({ error: "No fields to update" }, { status: 400 });
    }

    updates.updated_at = new Date().toISOString();
    await serviceClient.from("prompts").update(updates).eq("id", Number(id));

    const { data: item } = await serviceClient.from("prompts").select("*").eq("id", Number(id)).single();
    return Response.json({ ok: true, item });
  } catch (err) {
    return Response.json({ error: err instanceof Error ? err.message : String(err) }, { status: 500 });
  }
}

export async function DELETE(request: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  const denied = await requireRole(request, ["lead", "dev"]);
  if (denied) return denied;

  const { id } = await params;
  const { data: existing } = await serviceClient.from("prompts").select("id").eq("id", Number(id)).single();
  if (!existing) {
    return Response.json({ error: "Prompt not found" }, { status: 404 });
  }
  await serviceClient.from("prompts").delete().eq("id", Number(id));
  return Response.json({ ok: true });
}
