import { appendToFeed } from "../../lib/markdown";
import { upsertEntry } from "../../lib/db";

const TRENDING_FEEDS = [
  { url: "https://mshibanami.github.io/GitHubTrendingRSS/daily/all.xml", period: "daily" },
  { url: "https://mshibanami.github.io/GitHubTrendingRSS/weekly/all.xml", period: "weekly" },
  { url: "https://mshibanami.github.io/GitHubTrendingRSS/monthly/all.xml", period: "monthly" },
];

const MIN_DATE = "2026-01-01";

export async function ingestGithubTrending(): Promise<void> {
  console.log("\n⭐ GitHub Trending Ingester");

  const allItems: Array<{ title: string; url: string; summary: string; publishedAt: string }> = [];

  for (const feed of TRENDING_FEEDS) {
    console.log(`   Fetching ${feed.period}...`);

    try {
      const res = await fetch(feed.url);
      if (!res.ok) {
        console.warn(`     ⚠️  HTTP ${res.status}, skipping`);
        continue;
      }

      const xml = await res.text();
      const items = parseTrendingRSS(xml);
      console.log(`     → ${items.length} items`);
      allItems.push(...items);
    } catch (err) {
      console.error(`     ❌ ${err}`);
    }
  }

  const seen = new Set<string>();
  const unique = allItems.filter((item) => {
    if (seen.has(item.url)) return false;
    seen.add(item.url);
    return true;
  });

  console.log(`   ${unique.length} unique after dedup\n`);

  let inserted = 0;
  let skipped = 0;

  for (const item of unique) {
    const result = await upsertEntry({
      source: "github_trending",
      category: "github",
      title: item.title,
      url: item.url,
      summary: item.summary,
      tags: "github, trending, opensource",
      published_at: item.publishedAt,
    });

    if (item.publishedAt >= MIN_DATE) {
      appendToFeed("04-github-trending.md", item.title, item.url, item.publishedAt, "github, trending, opensource");
    }

    if (result === "inserted") inserted++;
    else skipped++;
  }

  console.log(`   → ${inserted} inserted, ${skipped} skipped`);
  console.log("✅ GitHub Trending ingestion complete\n");
}

function parseTrendingRSS(xml: string): Array<{ title: string; url: string; summary: string; publishedAt: string }> {
  const items: Array<{ title: string; url: string; summary: string; publishedAt: string }> = [];
  const itemRegex = /<(?:item|entry)>([\s\S]*?)<\/(?:item|entry)>/g;
  let match: RegExpExecArray | null;

  while ((match = itemRegex.exec(xml)) !== null) {
    const block = match[1];

    const title = extractTag(block, "title");
    const link = extractLink(block);
    const pubDate = extractTag(block, "pubDate") || extractTag(block, "published") || extractTag(block, "updated");
    const description = extractTag(block, "description") || extractTag(block, "summary");

    if (!title || !link) continue;

    items.push({
      title: decodeHtml(title).trim(),
      url: link.trim(),
      summary: description ? decodeHtml(description).substring(0, 500).trim() : "",
      publishedAt: parseDate(pubDate),
    });
  }

  return items;
}

function extractTag(xml: string, tag: string): string | null {
  const regex = new RegExp(`<${tag}[^>]*><!\\[CDATA\\[([\\s\\S]*?)\\]\\]><\\/${tag}>|<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i");
  const match = xml.match(regex);
  return match ? (match[1] ?? match[2] ?? "").trim() : null;
}

function extractLink(block: string): string | null {
  const rssMatch = block.match(/<link>(https?:\/\/.+?)<\/link>/);
  if (rssMatch) return rssMatch[1];
  const atomMatch = block.match(/<link[^>]*href="(https?:\/\/.+?)"[^>]*\/?>/);
  if (atomMatch) return atomMatch[1];
  return null;
}

function parseDate(dateStr: string | null): string {
  if (!dateStr) return new Date().toISOString().split("T")[0];
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) return new Date().toISOString().split("T")[0];
  return d.toISOString().split("T")[0];
}

function decodeHtml(text: string): string {
  return text
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&#x2F;/g, "/");
}
