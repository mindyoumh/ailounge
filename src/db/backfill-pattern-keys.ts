import { getDb } from "./client";
import { normalizeMessage } from "../lib/log-parser";

const db = getDb();

async function main() {
  const { data, error } = await db
    .from("log_errors")
    .select("id,error_type")
    .is("pattern_key", null);

  if (error) {
    console.error(error);
    process.exit(1);
  }

  const nullRows = (data ?? []) as { id: number; error_type: string | null }[];

  if (nullRows.length === 0) {
    console.log("No rows to backfill.");
    process.exit(0);
  }

  console.log(`Backfilling ${nullRows.length} rows...`);

  let updated = 0;
  for (const row of nullRows) {
    const key = normalizeMessage(row.error_type || "unknown");
    const { error: updateError } = await db
      .from("log_errors")
      .update({ pattern_key: key })
      .eq("id", row.id);

    if (updateError) {
      console.error(updateError);
      process.exit(1);
    }

    updated++;
  }

  console.log(`Updated ${updated} rows.`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
