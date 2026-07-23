import { serviceClient } from "@/src/db/service-client";
import { getIngestionStatus } from "@/src/lib/analytics";

export const dynamic = "force-dynamic";

export async function GET() {
  const statuses = await getIngestionStatus();

  const { data: allStatus } = await serviceClient
    .from("kv_store")
    .select("value")
    .eq("key", "ingest:status:all")
    .single();
  const { data: allLastRun } = await serviceClient
    .from("kv_store")
    .select("value")
    .eq("key", "ingest:last_run:all")
    .single();

  const errors = statuses.filter((s) => s.status === "error").length;

  return Response.json({
    sources: statuses,
    summary: {
      status: allStatus?.value ?? null,
      lastRun: allLastRun?.value ?? null,
      errors,
      total: statuses.length,
    },
  });
}
