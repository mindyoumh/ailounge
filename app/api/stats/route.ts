import { serviceClient } from "@/src/db/service-client";
import { getItemsToday, getItemsThisWeek, getLastGlobalIngestion, getIngestionStatus } from "@/src/lib/analytics";

export const dynamic = "force-dynamic";

export async function GET() {
  const [{ count: totalItems }, { count: stackTotal }, { data: stackRiskData }] = await Promise.all([
    serviceClient.from("feed_items").select("*", { count: "exact", head: true }).neq("source", "manual"),
    serviceClient.from("watchlist_items").select("*", { count: "exact", head: true }),
    serviceClient.from("watchlist_items").select("risk_level"),
  ]);

  const [lastIngest, itemsToday, itemsThisWeek, statuses] = await Promise.all([
    getLastGlobalIngestion(),
    getItemsToday(),
    getItemsThisWeek(),
    getIngestionStatus(),
  ]);
  const errors = statuses.filter((s) => s.status === "error").length;

  const stackHigh = (stackRiskData ?? []).filter((r) => r.risk_level === "high").length;
  const stackMedium = (stackRiskData ?? []).filter((r) => r.risk_level === "medium").length;
  const stackLow = (stackRiskData ?? []).filter((r) => r.risk_level === "low").length;

  return Response.json({
    totalItems: totalItems ?? 0,
    lastIngest,
    itemsToday,
    itemsThisWeek,
    errors,
    stackTotal: stackTotal ?? 0,
    stackHigh,
    stackMedium,
    stackLow,
  });
}
