import { appendToFeed } from "../../lib/markdown";
import { upsertEntry } from "../../lib/db";
import { RSS_FEEDS, RSSFeedConfig } from "./feeds";

const MIN_DATE = "2026-01-01";

export async function ingestRss(feeds?: RSSFeedConfig[]): Promise<void> {
  const targets = feeds ?? RSS_FEEDS;

  console.log("\n📡 RSS Ingester");
  console.log(`   ${targets.length} feed(s) configured\n`);

  let totalInserted = 0;
  let totalSkipped = 0;

  for (const feed of targets) {
    console.log(`  📄 ${feed.category} ← ${feed.url}`);

    try {
      const res = await fetch(feed.url);
      if (!res.ok) {
        console.warn(`     ⚠️  HTTP ${res.status}, skipping`);
        continue;
      }

      const xml = await res.text();
      const items = parseRSS(xml, feed);

      let feedInserted = 0;
      let feedSkipped = 0;

      for (const item of items) {
        // Always archive to DB
        const result = await upsertEntry({
          source: "rss",
          category: feed.category,
          title: item.title,
          url: item.url,
          summary: item.summary,
          tags: item.tags,
          published_at: item.publishedAt,
        });

        // Only write to markdown if recent enough
        if (item.publishedAt >= MIN_DATE) {
          appendToFeed(feed.feedFile, item.title, item.url, item.publishedAt, item.tags);
        }

        if (result === "inserted") feedInserted++;
        else feedSkipped++;
      }

      console.log(`     → ${feedInserted} inserted, ${feedSkipped} skipped`);
      totalInserted += feedInserted;
      totalSkipped += feedSkipped;

    } catch (err) {
      console.error(`     ❌ Error: ${err}`);
    }
  }

  console.log(`\n✅ RSS ingestion complete — ${totalInserted} new, ${totalSkipped} duplicates\n`);
}

interface RSSItem {
  title:       string;
  url:         string;
  summary?:    string;
  tags:        string;
  publishedAt: string;
}

function parseRSS(xml: string, config: RSSFeedConfig): RSSItem[] {
  const items: RSSItem[] = [];

  // Simple RSS / Atom parser without external deps
  // Matches <item> (RSS 2.0) and <entry> (Atom) patterns
  const itemRegex = /<(?:item|entry)>([\s\S]*?)<\/(?:item|entry)>/g;
  let match: RegExpExecArray | null;

  while ((match = itemRegex.exec(xml)) !== null) {
    const block = match[1];

    const title = extractTag(block, "title");
    const link = extractLink(block);
    const pubDate = extractTag(block, "pubDate") || extractTag(block, "published") || extractTag(block, "updated");
    const summary = extractTag(block, "description") || extractTag(block, "summary") || extractTag(block, "content");

    if (!title || !link) continue;

    items.push({
      title: decodeHtml(title).trim(),
      url: link.trim(),
      summary: summary ? decodeHtml(summary).substring(0, 500).trim() : undefined,
      tags: config.category,
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
  // RSS: <link>URL</link>
  const rssMatch = block.match(/<link>(https?:\/\/.+?)<\/link>/);
  if (rssMatch) return rssMatch[1];

  // Atom: <link href="URL" />
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

if (require.main === module) {
  ingestRss().catch(console.error);
}
