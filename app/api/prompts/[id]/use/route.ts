import { serviceClient } from "@/src/db/service-client";

export const dynamic = "force-dynamic";

export async function POST(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;

  const { data: existing } = await serviceClient.from("prompts").select("id, usage_count").eq("id", Number(id)).single();
  if (!existing) {
    return Response.json({ error: "Prompt not found" }, { status: 404 });
  }

  await serviceClient
    .from("prompts")
    .update({
      usage_count: (existing.usage_count ?? 0) + 1,
      updated_at: new Date().toISOString(),
    })
    .eq("id", Number(id));

  const { data: item } = await serviceClient.from("prompts").select("*").eq("id", Number(id)).single();
  return Response.json({ ok: true, item });
}
