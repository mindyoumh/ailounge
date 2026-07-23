import { createClient } from "@supabase/supabase-js";
import { config } from "dotenv";
config({ path: ".env.local" });

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!;
const supabase = createClient(supabaseUrl, supabaseKey);

(async () => {
  const { data: rows } = await supabase
    .from("feed_items")
    .select("source, category, published_at")
    .order("category");

  const map = new Map<string, { source: string; category: string; count: number; earliest: string; latest: string }>();
  for (const row of rows ?? []) {
    const key = `${row.source}|${row.category}`;
    const existing = map.get(key) ?? { source: row.source, category: row.category, count: 0, earliest: "", latest: "" };
    existing.count++;
    if (!existing.earliest || row.published_at < existing.earliest) existing.earliest = row.published_at;
    if (!existing.latest || row.published_at > existing.latest) existing.latest = row.published_at;
    map.set(key, existing);
  }

  console.table(Array.from(map.values()));
})();
