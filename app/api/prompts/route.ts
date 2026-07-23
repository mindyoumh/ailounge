import { NextRequest } from "next/server";
import { serviceClient } from "@/src/db/service-client";
import { requireRole } from "@/src/lib/auth-helpers";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const category = searchParams.get("category");
  const search = searchParams.get("search");
  const source = searchParams.get("source");

  let query = serviceClient.from("prompts").select("*");

  if (category) query = query.eq("category", category);
  if (source) query = query.eq("source", source);
  if (search) {
    query = query.or(`title.ilike.%${search}%,description.ilike.%${search}%`);
  }

  query = query
    .order("source", { ascending: false }) // curated first (curated < community alphabetically? No — use custom order via raw SQL)
    .order("usage_count", { ascending: false })
    .order("created_at", { ascending: false });

  const { data: items } = await query;

  // Sort curated before other sources (workaround for Supabase ordering limitation)
  if (items) {
    items.sort((a, b) => {
      if (a.source === "curated" && b.source !== "curated") return -1;
      if (a.source !== "curated" && b.source === "curated") return 1;
      if (a.usage_count !== b.usage_count) return (b.usage_count ?? 0) - (a.usage_count ?? 0);
      return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
    });
  }

  return Response.json({ items });
}

export async function POST(request: NextRequest) {
  const denied = await requireRole(request, ["lead", "dev"]);
  if (denied) return denied;

  try {
    const body = await request.json();
    const { title, content, category, description, input_fields, output_description, model_recommendation } = body;

    if (!title || !content || !category) {
      return Response.json({ error: "title, content, and category are required" }, { status: 400 });
    }

    const { data, error } = await serviceClient.from("prompts").insert({
      title,
      content,
      category,
      source: "curated",
      description: description ?? null,
      input_fields: input_fields ?? null,
      output_description: output_description ?? null,
      model_recommendation: model_recommendation ?? null,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    }).select().single();

    if (error) throw error;

    return Response.json({ ok: true, item: data }, { status: 201 });
  } catch (err) {
    return Response.json({ error: err instanceof Error ? err.message : String(err) }, { status: 500 });
  }
}
