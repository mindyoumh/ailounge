import { NextRequest, NextResponse } from "next/server";
import { getServerClient } from "@/src/db/server-client";

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);

  const source = searchParams.get("source");
  const category = searchParams.get("category");
  const tag = searchParams.get("tag");
  const q = searchParams.get("q");
  const is_read_param = searchParams.get("is_read");
  const is_pinned_param = searchParams.get("is_pinned");
  const sort = searchParams.get("sort");
  const min_score = searchParams.get("min_score");
  const limit = Math.min(parseInt(searchParams.get("limit") || "50"), 200);
  const offset = Math.max(parseInt(searchParams.get("offset") || "0"), 0);

  const { client: supabase } = getServerClient(req);
  const { data: { user } } = await supabase.auth.getUser();
  const userId = user?.id;

  let query = supabase
    .from("feed_items")
    .select("*", { count: "exact" })
    .neq("source", "manual");

  if (source && source !== "manual") query = query.eq("source", source);
  if (category) query = query.eq("category", category);
  if (tag) query = query.ilike("tags", `%${tag}%`);
  if (q) query = query.ilike("title", `%${q}%`);

  if (min_score) {
    const val = parseInt(min_score);
    if (!isNaN(val)) query = query.gte("ai_relevance_score", val);
  }

  if (sort === "relevance") {
    query = query
      .order("ai_relevance_score", { ascending: false, nullsFirst: false })
      .order("published_at", { ascending: false });
  } else {
    query = query
      .order("published_at", { ascending: false })
      .order("fetched_at", { ascending: false });
  }

  if (userId && is_pinned_param === "1") {
    const { data: pinnedStates } = await supabase
      .from("user_feed_states")
      .select("feed_item_id")
      .eq("user_id", userId)
      .eq("is_pinned", 1);

    const pinnedIds = (pinnedStates ?? []).map((p) => p.feed_item_id);
    if (pinnedIds.length === 0) {
      return NextResponse.json({ items: [], total: 0, limit, offset });
    }
    query = query.in("id", pinnedIds);
  }

  if (userId && is_read_param === "0") {
    const { data: readStates } = await supabase
      .from("user_feed_states")
      .select("feed_item_id")
      .eq("user_id", userId)
      .eq("is_read", 1);

    const readIds = (readStates ?? []).map((r) => r.feed_item_id);
    if (readIds.length > 0) {
      query = query.not("id", "in", `(${readIds.join(",")})`);
    }
  }

  query = query.range(offset, offset + limit - 1);

  const { data: items, count: total } = await query;

  if (!userId) {
    return NextResponse.json({
      items: (items ?? []).map((item) => ({ ...item, is_pinned: 0, is_read: 0 })),
      total, limit, offset,
    });
  }

  const itemIds = (items ?? []).map((i) => i.id);
  if (itemIds.length === 0) {
    return NextResponse.json({ items: [], total, limit, offset });
  }

  const { data: states } = await supabase
    .from("user_feed_states")
    .select("*")
    .eq("user_id", userId)
    .in("feed_item_id", itemIds);

  const stateMap = new Map(
    (states ?? []).map((s) => [s.feed_item_id, s]),
  );

  let mapped = (items ?? []).map((item) => {
    const state = stateMap.get(item.id);
    return { ...item, is_pinned: state?.is_pinned ?? 0, is_read: state?.is_read ?? 0 };
  });

  if (is_read_param === "0" || is_read_param === "1") {
    const val = parseInt(is_read_param);
    mapped = mapped.filter((item) => item.is_read === val);
  }

  return NextResponse.json({ items: mapped, total, limit, offset });
}

export async function POST(req: NextRequest) {
  const { client: supabase } = getServerClient(req);
  const { data: { user } } = await supabase.auth.getUser();
  if (!user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = await req.json();
  const { title, url, category, source, summary, tags, score } = body;

  if (!title || !url) {
    return NextResponse.json({ error: "title and url are required" }, { status: 400 });
  }

  const { data, error } = await supabase.from("feed_items").insert({
    source: source || "feed",
    category: category || "general",
    title,
    url,
    summary: summary || null,
    tags: tags || null,
    score: score || null,
    published_at: new Date().toISOString(),
    fetched_at: new Date().toISOString(),
  }).select("id").single();

  if (error) {
    if (error.code === "23505") {
      return NextResponse.json({ error: "Duplicate entry (source + url already exists)" }, { status: 409 });
    }
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  return NextResponse.json({ ok: true, id: data.id }, { status: 201 });
}
