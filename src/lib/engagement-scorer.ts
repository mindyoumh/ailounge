import { serviceClient } from "../db/service-client";

const PIN_MULTIPLIER = 5;
const READ_MULTIPLIER = 1;
const MAX_SCORE = 100;

async function getEngagementCounts(itemIds: number[]): Promise<Map<number, { pins: number; reads: number }>> {
  const { data: states } = await serviceClient
    .from("user_feed_states")
    .select("feed_item_id, is_pinned, is_read")
    .in("feed_item_id", itemIds);

  const map = new Map<number, { pins: number; reads: number }>();
  for (const s of states ?? []) {
    const cur = map.get(s.feed_item_id) ?? { pins: 0, reads: 0 };
    if (s.is_pinned) cur.pins++;
    if (s.is_read) cur.reads++;
    map.set(s.feed_item_id, cur);
  }
  return map;
}

export async function recalcEngagementForItem(itemId: number): Promise<void> {
  const { data } = await serviceClient
    .from("feed_items")
    .select("relevance_base")
    .eq("id", itemId)
    .single();

  if (!data || data.relevance_base === null) return;

  const counts = await getEngagementCounts([itemId]);
  const c = counts.get(itemId) ?? { pins: 0, reads: 0 };
  const boost = c.pins * PIN_MULTIPLIER + c.reads * READ_MULTIPLIER;
  const final = Math.min(data.relevance_base + boost, MAX_SCORE);

  await serviceClient
    .from("feed_items")
    .update({ ai_relevance_score: final })
    .eq("id", itemId);
}

export async function recalcAllEngagementScores(): Promise<number> {
  const { data: items } = await serviceClient
    .from("feed_items")
    .select("id, relevance_base")
    .not("relevance_base", "is", null);

  if (!items || items.length === 0) return 0;

  const counts = await getEngagementCounts(items.map((i) => i.id));

  for (const item of items) {
    const c = counts.get(item.id) ?? { pins: 0, reads: 0 };
    const boost = c.pins * PIN_MULTIPLIER + c.reads * READ_MULTIPLIER;
    const final = Math.min((item.relevance_base ?? 0) + boost, MAX_SCORE);
    await serviceClient
      .from("feed_items")
      .update({ ai_relevance_score: final })
      .eq("id", item.id);
  }

  return items.length;
}
