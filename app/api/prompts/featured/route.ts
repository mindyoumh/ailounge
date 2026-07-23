import { serviceClient } from "@/src/db/service-client";

export const dynamic = "force-dynamic";

export async function GET() {
  const { count } = await serviceClient
    .from("prompts")
    .select("*", { count: "exact", head: true })
    .eq("is_featured", 1)
    .eq("source", "curated");

  if (!count || count === 0) {
    const { data } = await serviceClient
      .from("prompts")
      .select("*")
      .eq("source", "curated")
      .order("usage_count", { ascending: false })
      .limit(1)
      .single();
    return Response.json({ item: data ?? null });
  }

  const dayOfYear = Math.floor(
    (Date.now() - new Date().getTimezoneOffset() * 60000) / 86400000,
  );
  const offset = dayOfYear % count;
  const { data } = await serviceClient
    .from("prompts")
    .select("*")
    .eq("is_featured", 1)
    .eq("source", "curated")
    .order("id")
    .range(offset, offset);

  return Response.json({ item: (data ?? [])[0] ?? null });
}
