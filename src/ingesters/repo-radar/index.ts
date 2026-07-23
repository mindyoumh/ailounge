import { serviceClient } from "../../db/service-client";
import { refreshSingleRepo, type RepoRadarItem } from "../../lib/repo-radar";

export async function ingestRepoRadar(): Promise<void> {
  const { data: items } = await serviceClient
    .from("repo_radar_items")
    .select("*")
    .eq("is_active", 1);

  const repoItems = (items ?? []) as RepoRadarItem[];

  if (repoItems.length === 0) {
    console.log("  ⏭  No repos to refresh");
    return;
  }

  let updated = 0;
  let errors = 0;

  for (const item of repoItems) {
    const result = await refreshSingleRepo(item);
    if (result.ok) updated++;
    else errors++;
  }

  const now = new Date().toISOString();
  const kvEntries = [
    { key: "ingest:last_run:repo_radar", value: now },
    { key: "ingest:status:repo_radar", value: errors === 0 ? "ok" : "degraded" },
    { key: "ingest:count:repo_radar", value: String(repoItems.length) },
    { key: "ingest:elapsed_ms:repo_radar", value: "0" },
  ];
  for (const e of kvEntries) {
    await serviceClient.from("kv_store").upsert(e, { onConflict: "key" });
  }

  console.log(`  📡 repo_radar: refreshed ${updated} repos${errors ? `, ${errors} errors` : ""}`);
}
