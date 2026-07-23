import { NextRequest, NextResponse } from "next/server";
import { serviceClient } from "@/src/db/service-client";

export async function GET(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> },
) {
  const { id } = await params;
  const url = new URL(req.url);
  const limit = Math.min(parseInt(url.searchParams.get("limit") || "50"), 200);
  const offset = Math.max(parseInt(url.searchParams.get("offset") || "0"), 0);
  const isError = url.searchParams.get("is_error");

  const numId = Number(id);
  let query = serviceClient.from("log_errors").select("*", { count: "exact" }).eq("analysis_id", numId);

  if (isError === "0" || isError === "1") {
    query = query.eq("is_error", parseInt(isError));
  }

  query = query.order("timestamp", { ascending: false }).order("id", { ascending: false }).range(offset, offset + limit - 1);

  const { data: rows, count } = await query;

  return NextResponse.json({ items: rows ?? [], total: count ?? 0, limit, offset });
}
