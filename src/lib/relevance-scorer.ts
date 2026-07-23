import { serviceClient } from "../db/service-client";

export interface RelevanceResult {
  score: number;
  label: string;
  reason: string;
}

let watchlistCache: Array<{ name: string; category: string | null }> | null = null;
let cacheTimer: ReturnType<typeof setTimeout> | null = null;

async function getWatchlist(): Promise<Array<{ name: string; category: string | null }>> {
  if (watchlistCache) return watchlistCache;

  const { data } = await serviceClient.from("watchlist_items").select("name, category");
  watchlistCache = data ?? [];

  if (cacheTimer) clearTimeout(cacheTimer);
  cacheTimer = setTimeout(() => {
    watchlistCache = null;
  }, 600_000);

  return watchlistCache;
}

export async function scoreRelevance(item: {
  title: string;
  summary?: string | null;
  tags?: string | null;
  category: string;
}): Promise<RelevanceResult | null> {
  const watchlist = await getWatchlist();
  if (watchlist.length === 0) return null;

  const searchText = `${item.title} ${item.summary ?? ""} ${item.tags ?? ""}`.toLowerCase();

  let best: RelevanceResult | null = null;

  for (const wl of watchlist) {
    const name = wl.name.toLowerCase();

    if (searchText.includes(name)) {
      if (!best || 80 > best.score) {
        best = { score: 80, label: wl.name, reason: `Mentions "${wl.name}" — in your stack` };
      }
      continue;
    }

    const words = name.split(/\s+/).filter((w) => w.length > 2);
    if (words.length === 0) continue;

    const matchedWords = words.filter((w) => searchText.includes(w));
    if (matchedWords.length >= Math.ceil(words.length * 0.5)) {
      if (!best || 60 > best.score) {
        best = { score: 60, label: wl.name, reason: `Partially matches "${wl.name}" — in your stack` };
      }
    }
  }

  if (!best || best.score < 50) {
    for (const wl of watchlist) {
      if (!wl.category) continue;
      if (item.category.toLowerCase() === wl.category.toLowerCase()) {
        if (!best || 40 > best.score) {
          best = { score: 40, label: wl.category, reason: `Category "${wl.category}" is in your stack` };
        }
      }
    }
  }

  return best;
}
