import { serviceClient } from "@/src/db/service-client";
import { fetchRepoInfo, fetchLatestRelease, fetchRecentPRs, fetchRecentIssues, detectBreakingChanges, detectSecurityAdvisory } from "@/src/lib/repo-radar";

export const dynamic = "force-dynamic";

export async function GET() {
  const { data: items } = await serviceClient
    .from("repo_radar_items")
    .select("*")
    .eq("is_active", 1)
    .order("stars", { ascending: false });
  return Response.json({ items });
}

export async function POST(request: Request) {
  try {
    const { owner, repo } = await request.json();

    if (!owner || !repo) {
      return Response.json({ error: "owner and repo are required" }, { status: 400 });
    }

    const fullName = `${owner}/${repo}`.toLowerCase();

    const { data: existing } = await serviceClient
      .from("repo_radar_items")
      .select("id")
      .eq("full_name", fullName)
      .maybeSingle();

    if (existing) {
      return Response.json({ error: "Repo already tracked" }, { status: 409 });
    }

    const info = await fetchRepoInfo(owner, repo);
    const release = await fetchLatestRelease(owner, repo);
    const prs = await fetchRecentPRs(owner, repo);
    const issuesOpened = await fetchRecentIssues(owner, repo);

    let breaking: string | null = null;
    let security: string | null = null;
    if (release?.body) {
      breaking = detectBreakingChanges(release.body);
      security = detectSecurityAdvisory(release.body);
    }

    const now = new Date().toISOString();
    const { data, error } = await serviceClient.from("repo_radar_items").insert({
      owner: owner.toLowerCase(),
      repo: repo.toLowerCase(),
      full_name: fullName,
      description: info.description,
      url: `https://github.com/${fullName}`,
      language: info.language,
      stars: info.stargazers_count,
      stars_gained: info.stargazers_count,
      latest_release: release?.tag_name ?? null,
      latest_release_url: release?.html_url ?? null,
      latest_release_date: release?.published_at ?? null,
      latest_release_body: release?.body ?? null,
      breaking_changes: breaking,
      security_advisory: security,
      open_issues: info.open_issues_count,
      open_prs: prs.opened + prs.merged,
      prs_opened_7d: prs.opened,
      prs_merged_7d: prs.merged,
      issues_opened_7d: issuesOpened,
      last_activity_at: info.pushed_at,
      last_refreshed_at: now,
      created_at: now,
      updated_at: now,
    }).select().single();

    if (error) throw error;

    return Response.json({ ok: true, item: data }, { status: 201 });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    if (message.includes("rate limit")) {
      return Response.json({ error: message }, { status: 429 });
    }
    if (message.includes("404")) {
      return Response.json({ error: "Repository not found. Check the owner/repo name." }, { status: 404 });
    }
    return Response.json({ error: message }, { status: 500 });
  }
}
