import { NextRequest, NextResponse } from "next/server";
import { getServerClient } from "@/src/db/server-client";
import { recalcEngagementForItem } from "@/src/lib/engagement-scorer";

export async function PATCH(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const { client: supabase } = getServerClient(req);
  const { data: { user } } = await supabase.auth.getUser();
  if (!user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = await req.json();
  const itemId = Number(id);

  const { data: item } = await supabase.from("feed_items").select("id").eq("id", itemId).single();
  if (!item) {
    return NextResponse.json({ error: "Item not found" }, { status: 404 });
  }

  const updates: Record<string, string | number | boolean | null> = {};

  for (const field of ["title", "summary", "tags", "category", "score"] as const) {
    if (body[field] !== undefined) updates[field] = body[field];
  }

  if (Object.keys(updates).length > 0) {
    await supabase.from("feed_items").update(updates).eq("id", itemId);
  }

  if (body.is_read !== undefined || body.is_pinned !== undefined) {
    const stateFields: Record<string, number> = {};
    if (body.is_read !== undefined) stateFields.is_read = body.is_read ? 1 : 0;
    if (body.is_pinned !== undefined) stateFields.is_pinned = body.is_pinned ? 1 : 0;

    await supabase.from("user_feed_states").upsert({
      user_id: user.id,
      feed_item_id: itemId,
      ...stateFields,
    }, {
      onConflict: "user_id, feed_item_id",
    });

    await recalcEngagementForItem(itemId);
  }

  const { data: updated } = await supabase
    .from("feed_items")
    .select("*, user_feed_states!left(is_read, is_pinned)")
    .eq("id", itemId)
    .eq("user_feed_states.user_id", user.id)
    .single();

  const states = updated?.user_feed_states as Record<string, unknown> | null;

  return NextResponse.json({
    ok: true,
    item: {
      ...updated,
      is_pinned: states?.is_pinned ?? 0,
      is_read: states?.is_read ?? 0,
      user_feed_states: undefined,
    },
  });
}

export async function DELETE(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const { client: supabase } = getServerClient(_req);
  const { data: { user } } = await supabase.auth.getUser();
  if (!user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const itemId = Number(id);

  const { data: item } = await supabase.from("feed_items").select("id").eq("id", itemId).single();
  if (!item) {
    return NextResponse.json({ error: "Item not found" }, { status: 404 });
  }

  await supabase.from("user_feed_states").delete().eq("feed_item_id", itemId).eq("user_id", user.id);
  await supabase.from("feed_items").delete().eq("id", itemId);

  return NextResponse.json({ ok: true });
}
