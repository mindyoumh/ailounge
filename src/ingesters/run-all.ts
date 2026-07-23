import { migrate } from "../db/schema";
import { getDb, closeDb } from "../db/client";
import { recalcAllEngagementScores } from "../lib/engagement-scorer";
import { ingestHackerNews } from "./hacker-news/index";
import { ingestGithubTrending } from "./github-trending/index";
import { ingestRss } from "./rss/index";
import { ingestRepoRadar } from "./repo-radar/index";

async function setKv(key: string, value: string): Promise<void> {
  await getDb().from("kv_store").upsert({ key, value }, { onConflict: "key" });
}

async function countBySource(source: string): Promise<number> {
  const { count } = await getDb()
    .from("feed_items")
    .select("*", { count: "exact", head: true })
    .eq("source", source);
  return count ?? 0;
}

async function runTracked(
  label: string,
  source: string,
  fn: () => Promise<void>,
): Promise<{ ok: boolean; inserted: number; elapsed: number }> {
  const before = await countBySource(source);
  const start = Date.now();
  try {
    await fn();
    const inserted = (await countBySource(source)) - before;
    const elapsed = Date.now() - start;
    await setKv(`ingest:last_run:${label}`, new Date().toISOString());
    await setKv(`ingest:status:${label}`, "ok");
    await setKv(`ingest:count:${label}`, String(inserted));
    await setKv(`ingest:elapsed_ms:${label}`, String(elapsed));
    console.log(`  ⏱  ${label} took ${elapsed}ms, inserted ${inserted} new items`);
    return { ok: true, inserted, elapsed };
  } catch (err) {
    const elapsed = Date.now() - start;
    await setKv(`ingest:last_run:${label}`, new Date().toISOString());
    await setKv(`ingest:status:${label}`, "error");
    await setKv(`ingest:count:${label}`, "0");
    await setKv(`ingest:elapsed_ms:${label}`, "0");
    console.error(`  ❌ ${label} failed:`, err);
    return { ok: false, inserted: 0, elapsed };
  }
}

export interface IngestResults {
  results: Record<string, { ok: boolean; inserted: number; elapsed: number }>;
  allOk: boolean;
}

export async function runAll(opts?: { closeDb?: boolean }): Promise<IngestResults> {
  console.log("=".repeat(60));
  console.log("🚀 Running all feed ingestors");
  console.log("=".repeat(60));

  migrate();

  const sourcesEnv = process.env.INGEST_SOURCES;
  const activeSources = sourcesEnv ? new Set(sourcesEnv.split(",").map(s => s.trim())) : null;

  const ALL_SOURCES: [string, string, () => Promise<void>][] = [
    ["hn", "hn", ingestHackerNews],
    ["github_trending", "github_trending", ingestGithubTrending],
    ["rss", "rss", ingestRss],
    ["repo_radar", "repo_radar", ingestRepoRadar],
  ];

  const results: IngestResults["results"] = {};
  for (const [label, source, fn] of ALL_SOURCES) {
    if (activeSources && !activeSources.has(label)) {
      console.log(`  ⏭  ${label} skipped (not in INGEST_SOURCES)`);
      continue;
    }
    results[label] = await runTracked(label, source, fn);
  }

  const allOk = Object.values(results).every((r) => r.ok);

  const engagementCount = sourcesEnv
    ? 0
    : await recalcAllEngagementScores();
  console.log(`  📊 Engagement scores recalculated for ${engagementCount} items`);

  await setKv("ingest:last_run:all", new Date().toISOString());
  await setKv("ingest:status:all", allOk ? "ok" : "degraded");

  console.log("=".repeat(60));

  // Print summary table
  console.log("📊 Ingestion Summary:");
  console.log("  Source           Status    Items  Last Run");
  console.log("  " + "─".repeat(55));
  for (const src of ["hn", "github_trending", "rss", "repo_radar"]) {
    const r = results[src];
    if (!r) {
      console.log(`  ⏭  ${src.padEnd(16)} skipped`);
      continue;
    }
    const count = r.ok ? String(r.inserted) : "ERR";
    const time = new Date().toLocaleTimeString();
    const icon = r.ok ? "✅" : "❌";
    console.log(`  ${icon} ${src.padEnd(16)} ${r.ok ? "ok".padEnd(8) : "error".padEnd(8)} ${count.padEnd(5)} ${time}`);
  }
  console.log("  " + "─".repeat(55));

  const globalStatus = allOk ? "ok" : "degraded";
  console.log(`\n✅ All ingestors complete — global status: ${globalStatus}`);

  if (opts?.closeDb ?? true) closeDb();

  return { results, allOk };
}

const isMain = process.argv[1]?.replace(/\\/g, "/").endsWith("run-all.ts");
if (isMain) {
  runAll().catch((err) => {
    console.error("❌ Fatal error:", err);
    process.exit(1);
  });
}
