import { serviceClient } from "../db/service-client";

export interface RepoRadarItem {
  id: number;
  owner: string;
  repo: string;
  full_name: string;
  description: string | null;
  url: string;
  language: string | null;
  stars: number;
  stars_gained: number;
  latest_release: string | null;
  latest_release_url: string | null;
  latest_release_date: string | null;
  latest_release_body: string | null;
  breaking_changes: string | null;
  security_advisory: string | null;
  open_issues: number;
  open_prs: number;
  prs_opened_7d: number;
  prs_merged_7d: number;
  issues_opened_7d: number;
  issue_spike: number;
  last_activity_at: string | null;
  notes: string | null;
  is_active: number;
  last_refreshed_at: string | null;
  created_at: string;
  updated_at: string;
}

interface GitHubRepo {
  description: string | null;
  language: string | null;
  stargazers_count: number;
  open_issues_count: number;
  pushed_at: string;
}

interface GitHubRelease {
  tag_name: string;
  html_url: string;
  body: string | null;
  published_at: string;
}

interface GitHubPR {
  state: string;
  merged_at: string | null;
  created_at: string;
  pull_request?: { merged_at: string | null };
}

const GITHUB_API = "https://api.github.com";

const userAgent = "my-ailounge/1.0";

async function githubFetch(path: string): Promise<Response> {
  const headers: Record<string, string> = {
    "User-Agent": userAgent,
    Accept: "application/vnd.github.v3+json",
  };
  const token = process.env.GH_ACCESS_TOKEN;
  if (token) headers.Authorization = `Bearer ${token}`;
  const res = await fetch(`${GITHUB_API}${path}`, { headers });
  if (res.status === 403) {
    const rateLimit = res.headers.get("X-RateLimit-Remaining");
    const reset = res.headers.get("X-RateLimit-Reset");
    throw new Error(
      rateLimit === "0"
        ? `GitHub API rate limit reached. Resets at ${new Date(Number(reset) * 1000).toLocaleTimeString()}.`
        : `GitHub API returned 403`,
    );
  }
  if (!res.ok) throw new Error(`GitHub API ${res.status}: ${res.statusText}`);
  return res;
}

export async function fetchRepoInfo(owner: string, repo: string): Promise<GitHubRepo> {
  const res = await githubFetch(`/repos/${owner}/${repo}`);
  const data = await res.json();
  return {
    description: data.description,
    language: data.language,
    stargazers_count: data.stargazers_count,
    open_issues_count: data.open_issues_count,
    pushed_at: data.pushed_at,
  };
}

export async function fetchLatestRelease(owner: string, repo: string): Promise<GitHubRelease | null> {
  try {
    const res = await githubFetch(`/repos/${owner}/${repo}/releases/latest`);
    const data = await res.json();
    return {
      tag_name: data.tag_name,
      html_url: data.html_url,
      body: data.body,
      published_at: data.published_at,
    };
  } catch {
    return null;
  }
}

export async function fetchRecentPRs(owner: string, repo: string, days = 7): Promise<{ opened: number; merged: number }> {
  try {
    const since = new Date(Date.now() - days * 86400000).toISOString();
    const res = await githubFetch(
      `/repos/${owner}/${repo}/pulls?state=all&per_page=50&sort=updated&direction=desc`,
    );
    const prs: GitHubPR[] = await res.json();
    let opened = 0;
    let merged = 0;
    for (const pr of prs) {
      if (pr.created_at >= since) opened++;
      if (pr.merged_at && pr.merged_at >= since) merged++;
    }
    return { opened, merged };
  } catch {
    return { opened: 0, merged: 0 };
  }
}

export async function fetchRecentIssues(owner: string, repo: string, days = 7): Promise<number> {
  try {
    const since = new Date(Date.now() - days * 86400000).toISOString();
    const res = await githubFetch(
      `/repos/${owner}/${repo}/issues?state=all&per_page=50&sort=created&direction=desc`,
    );
    const issues: { created_at: string; pull_request?: unknown }[] = await res.json();
    return issues.filter((i) => !i.pull_request && i.created_at >= since).length;
  } catch {
    return 0;
  }
}

const BREAKING_KEYWORDS = [
  "BREAKING CHANGE", "breaking change", "BREAKING CHANGES",
  "migration required", "MIGRATION REQUIRED",
  "deprecated", "DEPRECATED",
  "removed", "REMOVED",
  "no longer supported", "NOT SUPPORTED",
  "backwards incompatible", "BACKWARDS INCOMPATIBLE",
  "breaking:", "BREAKING:",
];

const SECURITY_KEYWORDS = [
  "CVE", "CVE-", "security", "SECURITY",
  "vulnerability", "VULNERABILITY",
  "security advisory", "SECURITY ADVISORY",
  "patch security", "security patch",
  "XSS", "CSRF", "SQL injection",
  "authentication bypass", "privilege escalation",
  "remote code execution", "RCE",
  "denial of service", "DoS",
];

export function detectBreakingChanges(body: string | null): string | null {
  if (!body) return null;
  const lines = body.split("\n");
  for (const line of lines) {
    const trimmed = line.trim();
    if (BREAKING_KEYWORDS.some((kw) => trimmed.includes(kw))) {
      const match = trimmed.match(/[A-Z].*?(?:\.|$)/);
      return match ? match[0].trim() : trimmed.slice(0, 120);
    }
  }
  return null;
}

export function detectSecurityAdvisory(body: string | null): string | null {
  if (!body) return null;
  const lines = body.split("\n");
  for (const line of lines) {
    const trimmed = line.trim();
    if (SECURITY_KEYWORDS.some((kw) => trimmed.includes(kw))) {
      const match = trimmed.match(/[A-Z].*?(?:\.|$)/);
      return match ? match[0].trim() : trimmed.slice(0, 120);
    }
  }
  return null;
}

export async function refreshSingleRepo(item: RepoRadarItem): Promise<{
  ok: boolean;
  error?: string;
  stars?: number;
  stars_gained?: number;
  new_release?: string;
  breaking?: string | null;
  security?: string | null;
  spike?: boolean;
}> {
  try {
    const info = await fetchRepoInfo(item.owner, item.repo);
    const release = await fetchLatestRelease(item.owner, item.repo);
    const prs = await fetchRecentPRs(item.owner, item.repo);
    const issuesOpened = await fetchRecentIssues(item.owner, item.repo);

    const prevStars = item.stars || 0;
    const starsGained = Math.max(0, info.stargazers_count - prevStars);

    let breaking: string | null = null;
    let security: string | null = null;
    if (release?.body) {
      breaking = detectBreakingChanges(release.body);
      security = detectSecurityAdvisory(release.body);
    }

    const prevIssues7d = item.issues_opened_7d || 0;
    const spike = prevIssues7d > 0 && issuesOpened > prevIssues7d * 1.5;

    const now = new Date().toISOString();
    await serviceClient
      .from("repo_radar_items")
      .update({
        description: info.description,
        language: info.language,
        stars: info.stargazers_count,
        stars_gained: starsGained,
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
        issue_spike: spike ? 1 : 0,
        last_activity_at: info.pushed_at,
        last_refreshed_at: now,
        updated_at: now,
      })
      .eq("id", item.id);

    return {
      ok: true,
      stars: info.stargazers_count,
      stars_gained: starsGained,
      new_release: release?.tag_name ?? undefined,
      breaking,
      security,
      spike,
    };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

export async function refreshAll(): Promise<{
  updated: number;
  errors: number;
  results: { name: string; ok: boolean; error?: string; stars?: number; new_release?: string; breaking?: string | null; security?: string | null }[];
}> {
  const { data: items } = await serviceClient
    .from("repo_radar_items")
    .select("*")
    .eq("is_active", 1)
    .order("stars", { ascending: false });

  const repoItems = (items ?? []) as RepoRadarItem[];

  let updated = 0;
  let errors = 0;
  const results: { name: string; ok: boolean; error?: string; stars?: number; new_release?: string; breaking?: string | null; security?: string | null }[] = [];

  for (const item of repoItems) {
    const result = await refreshSingleRepo(item);
    results.push({ ...result, name: item.full_name });
    if (result.ok) updated++;
    else errors++;
  }

  const now = new Date().toISOString();
  const kvEntries = [
    { key: "repo_radar:last_refresh", value: now },
    { key: "repo_radar:status", value: errors === 0 ? "ok" : "degraded" },
    { key: "repo_radar:count", value: String(repoItems.length) },
  ];
  for (const e of kvEntries) {
    await serviceClient.from("kv_store").upsert(e, { onConflict: "key" });
  }

  return { updated, errors, results };
}
