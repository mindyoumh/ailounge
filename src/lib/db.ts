import { serviceClient } from "../db/service-client";
import { scoreRelevance } from "./relevance-scorer";
import { recalcEngagementForItem } from "./engagement-scorer";

export interface IngestEntry {
  source:       string;
  category:     string;
  title:        string;
  url:          string;
  summary?:     string;
  tags?:        string;
  score?:       number;
  published_at: string;
}

export async function upsertEntry(entry: IngestEntry): Promise<"inserted" | "skipped"> {
  const { data: existing } = await serviceClient
    .from("feed_items")
    .select("id")
    .eq("source", entry.source)
    .eq("url", entry.url)
    .maybeSingle();

  if (existing) return "skipped";

  const { data: inserted, error } = await serviceClient
    .from("feed_items")
    .insert({
      source: entry.source,
      category: entry.category,
      title: entry.title,
      url: entry.url,
      summary: entry.summary ?? null,
      tags: entry.tags ?? null,
      score: entry.score ?? null,
      published_at: entry.published_at,
      fetched_at: new Date().toISOString(),
    })
    .select("id")
    .single();

  if (error) {
    if (error.code === "23505") return "skipped";
    return "skipped";
  }

  const relevance = await scoreRelevance({
    title: entry.title,
    summary: entry.summary,
    tags: entry.tags,
    category: entry.category,
  });

  if (relevance) {
    await serviceClient
      .from("feed_items")
      .update({
        ai_relevance_score: relevance.score,
        ai_relevance_label: relevance.label,
        ai_relevance_reason: relevance.reason,
        relevance_base: relevance.score,
      })
      .eq("id", inserted.id);

    await recalcEngagementForItem(inserted.id);
  }

  return "inserted";
}
