import fs from "fs";
import path from "path";
import { upsertEntry, IngestEntry } from "../../lib/db";

const FEEDS_DIR = path.join(process.cwd(), "docs", "feeds");

const FILE_CATEGORY_MAP: Record<string, string> = {
  "01-ai-news.md":         "ai",
  "02-cloud-news.md":      "cloud",
  "03-django-news.md":     "django",
  "04-github-trending.md": "github",
  "05-hacker-news.md":     "hn",
  "06-nextjs-news.md":     "nextjs",
  "07-rumors.md":          "rumors",
  "08-security-alerts.md": "security",
  "09-devops-news.md":     "devops",
  "10-github-news.md":     "github",
};

function parseEntry(line: string): Pick<IngestEntry, "title" | "url" | "published_at" | "tags"> | null {
  const trimmed = line.trim();
  if (!trimmed.startsWith("- [")) return null;

  const pattern = /^-\s+\[(.+?)\]\((https?:\/\/.+?)\)\s*\|\s*(\d{4}-\d{2}-\d{2})\s*\|\s*(.+)$/;
  const match = trimmed.match(pattern);

  if (!match) {
    console.warn(`  ⚠️  Skipping malformed line: ${trimmed.substring(0, 80)}...`);
    return null;
  }

  const [, title, url, date, rawTags] = match;
  return {
    title: title.trim(),
    url: url.trim(),
    published_at: date.trim(),
    tags: rawTags.trim().toLowerCase(),
  };
}

export async function ingestManualFeeds(): Promise<void> {
  console.log("\n📂 Manual Feeds Ingester");
  console.log(`   Reading from: ${FEEDS_DIR}\n`);

  if (!fs.existsSync(FEEDS_DIR)) {
    console.error(`❌ Feeds directory not found: ${FEEDS_DIR}`);
    return;
  }

  let totalInserted = 0;
  let totalSkipped = 0;
  let totalErrors = 0;

  for (const [filename, category] of Object.entries(FILE_CATEGORY_MAP)) {
    const filePath = path.join(FEEDS_DIR, filename);

    if (!fs.existsSync(filePath)) {
      console.warn(`  ⚠️  File not found, skipping: ${filename}`);
      continue;
    }

    console.log(`  📄 ${filename} (category: ${category})`);

    let fileInserted = 0;
    let fileSkipped = 0;

    try {
      const content = fs.readFileSync(filePath, "utf-8");
      const lines = content.split("\n");

      for (const line of lines) {
        const parsed = parseEntry(line);
        if (!parsed) continue;

        const result = await upsertEntry({
          source: "manual",
          category,
          title: parsed.title,
          url: parsed.url,
          tags: parsed.tags,
          published_at: parsed.published_at,
        });

        if (result === "inserted") {
          fileInserted++;
          console.log(`     ✅ Inserted: ${parsed.title.substring(0, 60)}`);
        } else {
          fileSkipped++;
        }
      }

      console.log(`     → ${fileInserted} inserted, ${fileSkipped} already existed\n`);
      totalInserted += fileInserted;
      totalSkipped += fileSkipped;

    } catch (err) {
      console.error(`  ❌ Error processing ${filename}:`, err);
      totalErrors++;
    }
  }

  console.log("─".repeat(50));
  console.log(`✅ Manual feeds ingestion complete`);
  console.log(`   Inserted : ${totalInserted}`);
  console.log(`   Skipped  : ${totalSkipped} (already in DB)`);
  console.log(`   Errors   : ${totalErrors}`);
  console.log("─".repeat(50) + "\n");
}

if (require.main === module) {
  ingestManualFeeds().catch(console.error);
}
