import { NextRequest, NextResponse } from "next/server";
import { runAll } from "@/src/ingesters/run-all";
import { requireRole } from "@/src/lib/auth-helpers";

export async function POST(req: NextRequest) {
  const denied = await requireRole(req, ["lead", "dev"]);
  if (denied) return denied;

  try {
    const result = await runAll({ closeDb: false });
    return NextResponse.json(result);
  } catch (err) {
    return NextResponse.json(
      { error: String(err) },
      { status: 500 },
    );
  }
}
