import { NextRequest, NextResponse } from "next/server";
import { PACKAGE_SUGGESTIONS, type PackageSuggestion } from "@/src/config/package-suggestions";

export const dynamic = "force-dynamic";

export async function GET(req: NextRequest) {
  const q = req.nextUrl.searchParams.get("q")?.toLowerCase().trim();

  if (!q || q.length < 1) {
    return NextResponse.json({ results: [] });
  }

  // 1. Filter curated list
  const curated: (PackageSuggestion & { source: string })[] = PACKAGE_SUGGESTIONS
    .filter((p) => p.name.toLowerCase().includes(q))
    .map((p) => ({ ...p, source: "curated" }));

  const curatedNames = new Set(curated.map((p) => p.name.toLowerCase()));

  // 2. Query npm registry for additional results (skip if curated already has many)
  let npmResults: (PackageSuggestion & { source: string })[] = [];

  if (curated.length < 8) {
    try {
      const res = await fetch(
        `https://registry.npmjs.org/-/v1/search?text=${encodeURIComponent(q)}&size=5`,
        { signal: AbortSignal.timeout(3000) },
      );
      if (res.ok) {
        const data = await res.json();
        npmResults = (data.objects ?? [])
          .map((o: { package: { name: string; description?: string } }) => ({
            name: o.package.name,
            ecosystem: "npm" as const,
            source: "npm" as const,
          }))
          .filter((p: PackageSuggestion) => !curatedNames.has(p.name.toLowerCase()));
      }
    } catch {
      // npm search failed — skip
    }
  }

  // 3. Merge: curated first, then npm, deduplicated, max 10
  const merged = [...curated, ...npmResults].slice(0, 10);

  return NextResponse.json({ results: merged });
}
