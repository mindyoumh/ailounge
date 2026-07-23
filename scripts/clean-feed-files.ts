import fs from "fs";
import path from "path";
import { createClient } from "@supabase/supabase-js";
import { config } from "dotenv";
config({ path: ".env.local" });

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!;
const supabase = createClient(supabaseUrl, supabaseKey);

const FEEDS_DIR = path.join(process.cwd(), "docs", "feeds");
const MIN_DATE = "2026-01-01";

interface FeedFileEntry {
  title: string;
  url: string;
  published_at: string;
  tags: string;
  category: string;
  source: string;
}

const FILE_FILTERS: Record<string, (row: FeedFileEntry) => boolean> = {
  "01-ai-news.md": (r) => r.category === "ai",
  "02-cloud-news.md": (r) => r.category === "cloud",
  "03-django-news.md": (r) => r.category === "django",
  "04-github-trending.md": (r) => r.source === "github_trending",
  "05-hacker-news.md": (r) => r.source === "hn" || r.category === "hn",
  "06-nextjs-news.md": (r) => r.category === "nextjs",
  "07-rumors.md": () => false,
  "08-security-alerts.md": (r) => r.category === "security",
};

function extractHeader(content: string): string {
  const lines = content.split("\n");
  const headerLines: string[] = [];
  for (const line of lines) {
    if (line.startsWith("## ")) break;
    headerLines.push(line);
  }
  return headerLines.join("\n");
}

function getMonthKey(dateStr: string): string {
  const d = new Date(dateStr);
  const months = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
  ];
  return `${months[d.getMonth()]} ${d.getFullYear()}`;  
}

function pad(n: number): string {
  return n < 10 ? "0" + n : String(n);
}

function formatDate(dateStr: string): string {
  const d = new Date(dateStr);
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

export async function cleanFeedFiles(): Promise<void> {
  const { data: rows } = await supabase
    .from("feed_items")
    .select("title, url, published_at, tags, category, source")
    .gte("published_at", MIN_DATE)
    .order("published_at", { ascending: false });

  const feedRows = (rows ?? []) as FeedFileEntry[];

  const filenames = fs.readdirSync(FEEDS_DIR).filter((f) => f.endsWith(".md"));

  for (const filename of filenames) {
    const filter = FILE_FILTERS[filename];
    if (!filter) {
      console.log(`  ⏭️  ${filename} — no filter defined, skipping`);
      continue;
    }

    const filePath = path.join(FEEDS_DIR, filename);
    const content = fs.readFileSync(filePath, "utf-8");
    const header = extractHeader(content);

    const matchingRows = feedRows.filter(filter);

    if (matchingRows.length === 0 && filename !== "07-rumors.md") {
      console.log(`  ⚠️  ${filename} — no matching entries for ${MIN_DATE}+, writing empty`);
    }

    const grouped: Record<string, FeedFileEntry[]> = {};
    for (const row of matchingRows) {
      const key = getMonthKey(row.published_at);
      if (!grouped[key]) grouped[key] = [];
      grouped[key].push(row);
    }

    const monthOrder = Object.keys(grouped).sort((a, b) => {
      return new Date(b).getTime() - new Date(a).getTime();
    });

    const lines: string[] = [header.trim(), ""];
    for (const month of monthOrder) {
      lines.push(`## ${month}`);
      lines.push("");
      for (const entry of grouped[month]) {
        const date = formatDate(entry.published_at);
        const tags = entry.tags || entry.category;
        lines.push(`- [${entry.title}](${entry.url}) | ${date} | ${tags}`);
      }
      lines.push("");
    }

    fs.writeFileSync(filePath, lines.join("\n") + "\n", "utf-8");
    console.log(`  ✅ ${filename} — ${matchingRows.length} entries rewritten`);
  }

  console.log("\n✅ Feed files cleaned successfully!");
}

cleanFeedFiles().catch(console.error);
