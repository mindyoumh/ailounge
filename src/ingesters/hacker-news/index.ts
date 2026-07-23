import { appendToFeed } from "../../lib/markdown";
import { upsertEntry } from "../../lib/db";

interface HNItem {
  objectID:    string;
  title:       string;
  url?:        string;
  points:      number;
  author:      string;
  created_at:  string;
  story_text?: string;
}

const HN_API = "https://hn.algolia.com/api/v1/search_by_date?tags=story&hitsPerPage=20";

export async function ingestHackerNews(): Promise<void> {
  console.log("\n📰 Hacker News Ingester");
  console.log(`   Fetching from: ${HN_API}\n`);

  let inserted = 0;
  let skipped = 0;

  try {
    const res = await fetch(HN_API);
    if (!res.ok) {
      console.error(`❌ HN API returned ${res.status}`);
      return;
    }

    const data = await res.json() as { hits: HNItem[] };

    for (const item of data.hits) {
      const url = item.url || `https://news.ycombinator.com/item?id=${item.objectID}`;
      const title = item.title.replace(/Ask HN: |Show HN: |Tell HN: /, "");
      const publishedAt = item.created_at.split("T")[0];
      const tags = `hn, discussion`;

      // Write to markdown
      appendToFeed("05-hacker-news.md", title, url, publishedAt, tags);

      // Write to DB
      const result = await upsertEntry({
        source: "hn",
        category: "hn",
        title,
        url,
        summary: item.story_text?.substring(0, 500) || undefined,
        tags,
        score: item.points,
        published_at: publishedAt,
      });

      if (result === "inserted") inserted++;
      else skipped++;
    }

    console.log(`   → ${inserted} inserted, ${skipped} skipped\n`);
    console.log("✅ Hacker News ingestion complete\n");

  } catch (err) {
    console.error("❌ Failed to fetch Hacker News:", err);
  }
}

if (require.main === module) {
  ingestHackerNews().catch(console.error);
}
