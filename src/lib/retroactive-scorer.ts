import { serviceClient } from "../db/service-client";
import { scoreRelevance } from "./relevance-scorer";
import { recalcEngagementForItem } from "./engagement-scorer";

const MIN_NAME_LENGTH = 3;

export async function retroactivelyScore(opts: {
  name: string;
  category: string | null;
}): Promise<{ updated: number }> {
  const { name, category } = opts;

  const filters: string[] = [];

  if (name && name.length >= MIN_NAME_LENGTH) {
    const p = `%${name}%`;
    filters.push(`title.ilike.${p}`, `summary.ilike.${p}`, `tags.ilike.${p}`);
  }

  if (category) {
    filters.push(`category.eq.${category}`);
  }

  if (filters.length === 0) return { updated: 0 };

  const { data: items } = await serviceClient
    .from("feed_items")
    .select("id, title, summary, tags, category")
    .or(filters.join(","));

  if (!items || items.length === 0) return { updated: 0 };

  const scored = await Promise.all(
    items.map(async (item) => {
      const relevance = await scoreRelevance({
        title: item.title,
        summary: item.summary,
        tags: item.tags,
        category: item.category,
      });
      return { id: item.id, relevance };
    }),
  );

  const toUpdate = scored.filter((s) => s.relevance !== null);
  if (toUpdate.length === 0) return { updated: 0 };

  await Promise.all(
    toUpdate.map(async (s) => {
      await serviceClient
        .from("feed_items")
        .update({
          relevance_base: s.relevance!.score,
          ai_relevance_score: s.relevance!.score,
          ai_relevance_label: s.relevance!.label,
          ai_relevance_reason: s.relevance!.reason,
        })
        .eq("id", s.id);

      await recalcEngagementForItem(s.id);
    }),
  );

  return { updated: toUpdate.length };
}
