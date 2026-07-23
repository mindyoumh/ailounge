"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import {
  Plus,
  X,
  Trash2,
  Search,
  ShieldCheck,
  ShieldAlert,
  ShieldX,
  Wrench,
  ExternalLink,
  ChevronDown,
  ChevronUp,
  ChevronRight,
  Package,
  GitFork,
  BookOpen,
  RefreshCw,
  FileDown,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { useUser } from "@/components/auth-provider";

type WatchItem = {
  id: number;
  name: string;
  category: string | null;
  ecosystem: string | null;
  installed_version: string | null;
  latest_version: string | null;
  risk_level: string;
  risk_reason: string | null;
  upgrade_notes: string | null;
  known_vulns: string | null;
  migration_link: string | null;
  updated_at: string;
};

type SemVer = { major: number; minor: number; patch: number };

type VersionStatus =
  | { kind: "up-to-date" }
  | { kind: "patch"; behind: number }
  | { kind: "minor"; behind: number }
  | { kind: "major" }
  | { kind: "unknown" };

const CATEGORIES = [
  "framework",
  "database",
  "infra",
  "cloud",
  "ai-sdk",
  "tool",
];

const ECOSYSTEMS = [
  "npm",
  "PyPI",
  "Go",
  "Maven",
  "NuGet",
  "crates.io",
  "RubyGems",
  "Packagist",
];

const RISK_CONFIG: Record<
  string,
  { icon: React.ElementType; label: string; bg: string; text: string }
> = {
  low: {
    icon: ShieldCheck,
    label: "Low",
    bg: "bg-emerald-500/10",
    text: "text-emerald-600 dark:text-emerald-400",
  },
  medium: {
    icon: ShieldAlert,
    label: "Medium",
    bg: "bg-amber-500/10",
    text: "text-amber-600 dark:text-amber-400",
  },
  high: {
    icon: ShieldX,
    label: "High",
    bg: "bg-rose-500/10",
    text: "text-rose-600 dark:text-rose-400",
  },
};

const VERSION_BADGE: Record<string, { bg: string; text: string; label: string }> = {
  "up-to-date": { bg: "bg-emerald-500/10", text: "text-emerald-600 dark:text-emerald-400", label: "Up to date" },
  patch: { bg: "bg-amber-500/10", text: "text-amber-600 dark:text-amber-400", label: "" },
  minor: { bg: "bg-orange-500/10", text: "text-orange-600 dark:text-orange-400", label: "" },
  major: { bg: "bg-rose-500/10", text: "text-rose-600 dark:text-rose-400", label: "Major update" },
  unknown: { bg: "bg-muted", text: "text-muted-foreground", label: "—" },
};

const RESOURCE_LINKS: Record<string, { npm?: string; github?: string; docs?: string }> = {
  nextjs: { npm: "https://www.npmjs.com/package/next", github: "https://github.com/vercel/next.js", docs: "https://nextjs.org/docs" },
  react: { npm: "https://www.npmjs.com/package/react", github: "https://github.com/facebook/react", docs: "https://react.dev" },
  "tailwind css": { npm: "https://www.npmjs.com/package/tailwindcss", github: "https://github.com/tailwindlabs/tailwindcss", docs: "https://tailwindcss.com/docs" },
  typescript: { npm: "https://www.npmjs.com/package/typescript", github: "https://github.com/microsoft/TypeScript", docs: "https://www.typescriptlang.org/docs" },
  supabase: { npm: "https://www.npmjs.com/package/@supabase/supabase-js", github: "https://github.com/supabase/supabase", docs: "https://supabase.com/docs" },
  postgresql: { docs: "https://www.postgresql.org/docs" },
  vite: { npm: "https://www.npmjs.com/package/vite", github: "https://github.com/vitejs/vite", docs: "https://vite.dev" },
  python: { docs: "https://docs.python.org/3" },
  django: { npm: "https://www.npmjs.com/package/django", github: "https://github.com/django/django", docs: "https://docs.djangoproject.com" },
  docker: { docs: "https://docs.docker.com", github: "https://github.com/docker" },
  redis: { github: "https://github.com/redis/redis", docs: "https://redis.io/docs" },
  nginx: { docs: "https://nginx.org/en/docs", github: "https://github.com/nginx/nginx" },
  aws: { docs: "https://docs.aws.amazon.com" },
  "node.js": { npm: "https://www.npmjs.com/package/node", github: "https://github.com/nodejs/node", docs: "https://nodejs.org/docs" },
};

function parseSemver(v: string): SemVer | null {
  const parts = v.replace(/^[vV]/, "").split(".");
  const [major, minor, patch] = parts.map(Number);
  if (isNaN(major)) return null;
  return { major, minor: minor ?? 0, patch: patch ?? 0 };
}

function getVersionStatus(installed: string | null, latest: string | null): VersionStatus {
  if (!installed || !latest) return { kind: "unknown" };
  const a = parseSemver(installed);
  const b = parseSemver(latest);
  if (!a || !b) return { kind: "unknown" };
  if (a.major === b.major && a.minor === b.minor && a.patch === b.patch) return { kind: "up-to-date" };
  if (a.major !== b.major) return { kind: "major" };
  if (a.minor !== b.minor) return { kind: "minor", behind: b.minor - a.minor };
  return { kind: "patch", behind: b.patch - a.patch };
}

function getVersionLabel(status: VersionStatus): string {
  switch (status.kind) {
    case "up-to-date": return "Up to date";
    case "patch": return `${status.behind} patch${status.behind > 1 ? "es" : ""} behind`;
    case "minor": return `${status.behind} minor behind`;
    case "major": return "Major update";
    case "unknown": return "—";
  }
}

function getResourceLinks(name: string) {
  const lower = name.toLowerCase().trim();
  if (RESOURCE_LINKS[lower]) return RESOURCE_LINKS[lower];

  const slug = lower.replace(/\s+/g, "-").replace(/[^a-z0-9-]/g, "");
  return {
    npm: `https://www.npmjs.com/package/${slug}`,
    github: `https://github.com/${slug}/${slug}`,
  };
}

function relativeTime(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  if (days < 30) return `${days}d ago`;
  return new Date(dateStr).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

export const dynamic = "force-dynamic";

export default function WatchlistPage() {
  const { role } = useUser();
  const [items, setItems] = useState<WatchItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showAddForm, setShowAddForm] = useState(false);
  const [deletingId, setDeletingId] = useState<number | null>(null);
  const [sortField, setSortField] = useState<string>("name");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [expandedIds, setExpandedIds] = useState<Set<number>>(new Set());

  const [search, setSearch] = useState("");
  const [addName, setAddName] = useState("");
  const [addEcosystem, setAddEcosystem] = useState("npm");
  const [addCategory, setAddCategory] = useState("framework");
  const [addRisk, setAddRisk] = useState("low");
  const [addRiskReason, setAddRiskReason] = useState("");

  const fetchItems = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch("/api/watchlist");
      if (!res.ok) throw new Error(`Status ${res.status}`);
      const data = await res.json();
      setItems(data.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to fetch");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchItems(); }, [fetchItems]);

  const updateField = async (id: number, field: string, value: string | number) => {
    const res = await fetch(`/api/watchlist/${id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ [field]: value }),
    });
    if (res.ok) {
      const data = await res.json();
      setItems((prev) => prev.map((i) => (i.id === id ? data.item : i)));
    }
  };

  const [refreshingCve, setRefreshingCve] = useState<number | null>(null);
  const [refreshingVer, setRefreshingVer] = useState<number | null>(null);

  const checkCve = async (id: number) => {
    setRefreshingCve(id);
    try {
      const res = await fetch(`/api/watchlist/${id}/cve`, { method: "POST" });
      if (res.ok) {
        const data = await res.json();
        setItems((prev) => prev.map((i) => (i.id === id ? data.item : i)));
      }
    } finally {
      setRefreshingCve(null);
    }
  };

  const checkVersion = async (id: number) => {
    setRefreshingVer(id);
    try {
      const res = await fetch(`/api/watchlist/${id}/version`, { method: "POST" });
      if (res.ok) {
        const data = await res.json();
        setItems((prev) => prev.map((i) => (i.id === id ? data.item : i)));
      }
    } finally {
      setRefreshingVer(null);
    }
  };

  const deleteItem = async (id: number) => {
    setDeletingId(id);
    const res = await fetch(`/api/watchlist/${id}`, { method: "DELETE" });
    if (res.ok) setItems((prev) => prev.filter((i) => i.id !== id));
    setDeletingId(null);
  };

  const addItem = async () => {
    if (!addName.trim()) return;
    const res = await fetch("/api/watchlist", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: addName,
        category: addCategory,
        ecosystem: addEcosystem,
        risk_level: addRisk,
        risk_reason: addRiskReason || null,
      }),
    });
    if (res.ok) {
      setAddName("");
      setAddEcosystem("npm");
      setAddCategory("framework");
      setAddRisk("low");
      setAddRiskReason("");
      setShowAddForm(false);
      fetchItems();
    } else {
      const data = await res.json();
      alert(data.error || "Failed to add item");
    }
  };

  const toggleExpand = (id: number) => {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleSort = (field: string) => {
    if (sortField === field) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else { setSortField(field); setSortDir("asc"); }
  };

  const sorted = [...items].sort((a, b) => {
    const aVal = (a as Record<string, unknown>)[sortField];
    const bVal = (b as Record<string, unknown>)[sortField];
    return sortDir === "asc"
      ? String(aVal ?? "").localeCompare(String(bVal ?? ""))
      : String(bVal ?? "").localeCompare(String(aVal ?? ""));
  });

  const filtered = search
    ? sorted.filter(
        (i) =>
          i.name.toLowerCase().includes(search.toLowerCase()) ||
          (i.category ?? "").toLowerCase().includes(search.toLowerCase()) ||
          (i.upgrade_notes ?? "").toLowerCase().includes(search.toLowerCase()),
      )
    : sorted;

  const counts = { high: 0, medium: 0, low: 0 };
  for (const item of items) {
    if (item.risk_level in counts)
      counts[item.risk_level as keyof typeof counts]++;
  }

  const SortIcon = ({ field }: { field: string }) => {
    if (sortField !== field) return null;
    return sortDir === "asc"
      ? <ChevronUp className="h-3 w-3 ml-0.5 inline" />
      : <ChevronDown className="h-3 w-3 ml-0.5 inline" />;
  };

  return (
    <div className="min-h-screen bg-background">
      <div className="mx-auto max-w-7xl px-4 md:px-6 py-6 md:py-8">
        {/* Header */}
        <div className="flex items-start justify-between gap-4 flex-wrap mb-6">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
              <Wrench className="h-5 w-5 text-accent-vibrant shrink-0" />
              <h1 className="text-3xl font-bold tracking-tight">Stack Watchlist</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1">
              Track versions, risks, and resources across your tech stack
            </p>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <button
              onClick={() => window.open("/api/watchlist/export", "_blank")}
              className="inline-flex items-center gap-1.5 rounded-lg border border-border bg-card px-3 py-2 text-xs font-medium text-muted-foreground transition-all duration-200 hover:bg-accent hover:text-foreground"
            >
              <FileDown className="h-3.5 w-3.5" />
              Export PDF
            </button>
            <button
              onClick={() => setShowAddForm(!showAddForm)}
              className="inline-flex items-center gap-1.5 rounded-lg border border-border bg-card px-3 py-2 text-xs font-medium text-muted-foreground transition-all duration-200 hover:bg-accent hover:text-foreground"
            >
              {showAddForm ? <X className="h-3.5 w-3.5" /> : <Plus className="h-3.5 w-3.5" />}
              {showAddForm ? "Cancel" : "Add Item"}
            </button>
          </div>
        </div>

        {/* Add form */}
        {showAddForm && (
          <Card className="mb-6 border-primary/30 animate-slide-down">
            <CardContent className="pt-6">
              <div className="grid gap-3 sm:grid-cols-5">
                <div>
                  <label className="text-xs font-medium text-muted-foreground mb-1 block">Name</label>
                  <PackageSearchInput value={addName} ecosystem={addEcosystem} onSelect={(name, eco) => { setAddName(name); setAddEcosystem(eco); }} />
                </div>
                <div>
                  <label className="text-xs font-medium text-muted-foreground mb-1 block">Category</label>
                  <Select value={addCategory} onValueChange={setAddCategory}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {CATEGORIES.map((c) => <SelectItem key={c} value={c}>{c}</SelectItem>)}
                    </SelectContent>
                  </Select>
                </div>
                <div>
                  <label className="text-xs font-medium text-muted-foreground mb-1 block">Risk Level</label>
                  <Select value={addRisk} onValueChange={setAddRisk}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="low">Low</SelectItem>
                      <SelectItem value="medium">Medium</SelectItem>
                      <SelectItem value="high">High</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="sm:col-span-2">
                  <label className="text-xs font-medium text-muted-foreground mb-1 block">Risk Reason (optional)</label>
                  <Input
                    placeholder="Why this risk level?"
                    value={addRiskReason}
                    onChange={(e) => setAddRiskReason(e.target.value)}
                  />
                </div>
              </div>
              <div className="flex justify-end gap-2 mt-3">
                <Button variant="outline" onClick={() => setShowAddForm(false)}>Cancel</Button>
                <Button size="sm" onClick={addItem}>Add</Button>
              </div>
            </CardContent>
          </Card>
        )}

        {error && (
          <div className="mb-4 px-4 py-2 rounded-md bg-red-50 dark:bg-red-950 border border-red-200 dark:border-red-800 text-sm text-red-700 dark:text-red-300">
            {error}
          </div>
        )}

        {/* Search */}
        <div className="relative mb-4">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search by name, category, or notes..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="pl-9"
          />
        </div>

        {/* Table */}
        <Card className="animate-fade-in">
          <CardContent className="p-0 overflow-x-auto">
            {loading ? (
              <div className="p-4 space-y-3">
                {Array.from({ length: 6 }).map((_, i) => (
                  <div key={i} className="h-12 bg-muted rounded animate-pulse" style={{ animationDelay: `${i * 50}ms` }} />
                ))}
              </div>
            ) : filtered.length === 0 ? (
              <div className="text-center py-12 text-muted-foreground">
                <Wrench className="h-8 w-8 mx-auto mb-2 opacity-40" />
                <p>{search ? "No items match your search." : "No items tracked yet."}</p>
                <Button size="sm" variant="outline" className="mt-3" onClick={() => setShowAddForm(true)}>
                  <Plus className="h-4 w-4" /> Add your first item
                </Button>
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-8 hidden md:table-cell" />
                    <TableHead className="cursor-pointer select-none w-[160px]" onClick={() => toggleSort("name")}>
                      Name <SortIcon field="name" />
                    </TableHead>
                    <TableHead className="cursor-pointer select-none w-[80px] hidden md:table-cell" onClick={() => toggleSort("category")}>
                      Category <SortIcon field="category" />
                    </TableHead>
                    <TableHead className="w-[220px] hidden md:table-cell">Version Health</TableHead>
                    <TableHead className="cursor-pointer select-none w-[70px]" onClick={() => toggleSort("known_vulns")}>
                      Vulns <SortIcon field="known_vulns" />
                    </TableHead>
                    <TableHead className="cursor-pointer select-none w-[70px]" onClick={() => toggleSort("risk_level")}>
                      Risk <SortIcon field="risk_level" />
                    </TableHead>
                    <TableHead className="cursor-pointer select-none w-[90px] hidden md:table-cell" onClick={() => toggleSort("updated_at")}>
                      Last Checked <SortIcon field="updated_at" />
                    </TableHead>
                    <TableHead className="w-[90px] hidden md:table-cell">Links</TableHead>
                    <TableHead className="w-[36px]" />
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filtered.map((item, index) => {
                    const risk = RISK_CONFIG[item.risk_level] || RISK_CONFIG.low;
                    const RiskIcon = risk.icon;
                    const status = getVersionStatus(item.installed_version, item.latest_version);
                    const vb = VERSION_BADGE[status.kind];
                    const links = getResourceLinks(item.name);
                    const isExpanded = expandedIds.has(item.id);

                    return (
                      <Fragment key={item.id}>
                        <TableRow
                          className={cn(
                            "animate-slide-up transition-colors",
                            deletingId === item.id && "opacity-0 transition-opacity duration-200",
                            isExpanded && "bg-accent/30",
                          )}
                          style={{ animationDelay: `${index * 30}ms` }}
                        >
                          {/* Expand toggle */}
                          <TableCell className="hidden md:table-cell">
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-7 w-7"
                              onClick={() => toggleExpand(item.id)}
                            >
                              <ChevronRight
                                className={cn(
                                  "h-3.5 w-3.5 transition-transform duration-200",
                                  isExpanded && "rotate-90",
                                )}
                              />
                            </Button>
                          </TableCell>

                          {/* Name */}
                          <TableCell className="font-medium text-sm">{item.name}</TableCell>

                          {/* Category */}
                          <TableCell className="hidden md:table-cell">
                            {item.category ? (
                              <Badge variant="secondary" className="text-[10px] px-1.5 py-0">
                                {item.category}
                              </Badge>
                            ) : (
                              <span className="text-xs text-muted-foreground">—</span>
                            )}
                          </TableCell>

                          {/* Version Health */}
                          <TableCell className="hidden md:table-cell">
                            <div className="flex items-center gap-2">
                              <span className="text-xs font-mono text-muted-foreground">
                                {item.installed_version || "—"}
                              </span>
                              <span className="text-[10px] text-muted-foreground">→</span>
                              <span className="text-xs font-mono text-muted-foreground">
                                {item.latest_version || "—"}
                              </span>
                              {status.kind !== "unknown" && (
                                <Badge className={cn("text-[10px] px-1.5 py-0 font-medium", vb.bg, vb.text)}>
                                  {getVersionLabel(status)}
                                </Badge>
                              )}
                            </div>
                          </TableCell>

                          {/* Vulns */}
                          <TableCell>
                            {(() => {
                              if (!item.known_vulns) return <span className="text-xs text-muted-foreground">—</span>;
                              try {
                                const parsed = JSON.parse(item.known_vulns);
                                if (!parsed.cves || parsed.cves.length === 0) return <span className="text-xs text-emerald-600 dark:text-emerald-400 font-mono">0</span>;
                                const sevColors: Record<string, string> = {
                                  CRITICAL: "bg-rose-500/10 text-rose-600 dark:text-rose-400",
                                  HIGH: "bg-rose-500/10 text-rose-600 dark:text-rose-400",
                                  MODERATE: "bg-amber-500/10 text-amber-600 dark:text-amber-400",
                                  LOW: "bg-muted text-muted-foreground",
                                };
                                return (
                                  <span className={cn("inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium font-mono", sevColors[parsed.highestSeverity] ?? "bg-muted text-muted-foreground")}>
                                    {parsed.totalCount} CVE{parsed.totalCount > 1 ? "s" : ""}
                                  </span>
                                );
                              } catch {
                                return <span className="text-xs font-mono text-muted-foreground">{item.known_vulns}</span>;
                              }
                            })()}
                          </TableCell>

                          {/* Risk */}
                          <TableCell>
                            <div className="flex flex-col">
                              <div className="relative group">
                                <span className={cn("inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[11px] font-medium", risk.bg, risk.text)}>
                                  <RiskIcon className="h-3 w-3" />
                                  {risk.label}
                                </span>
                                {item.risk_reason && (
                                  <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 hidden md:group-hover:block z-50 pointer-events-none">
                                    <div className="bg-popover border border-border rounded-md px-3 py-2 text-xs shadow-md whitespace-nowrap max-w-[260px]">
                                      {item.risk_reason}
                                    </div>
                                  </div>
                                )}
                              </div>
                              {item.risk_reason && (
                                <span className="text-[10px] text-muted-foreground mt-0.5 truncate max-w-[70px] md:hidden">
                                  {item.risk_reason}
                                </span>
                              )}
                            </div>
                          </TableCell>

                          {/* Last Checked */}
                          <TableCell className="text-xs text-muted-foreground whitespace-nowrap hidden md:table-cell">
                            {relativeTime(item.updated_at)}
                          </TableCell>

                          {/* Resource Links */}
                          <TableCell className="hidden md:table-cell">
                            <div className="flex items-center gap-0.5">
                              {links.npm && (
                                <a href={links.npm} target="_blank" rel="noopener noreferrer" className="inline-flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground hover:text-accent-vibrant hover:bg-accent/50 transition-colors" title="npm">
                                  <Package className="h-3.5 w-3.5" />
                                </a>
                              )}
                              {links.github && (
                                <a href={links.github} target="_blank" rel="noopener noreferrer" className="inline-flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground hover:text-accent-vibrant hover:bg-accent/50 transition-colors" title="GitHub">
                                  <GitFork className="h-3.5 w-3.5" />
                                </a>
                              )}
                              {links.docs && (
                                <a href={links.docs} target="_blank" rel="noopener noreferrer" className="inline-flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground hover:text-accent-vibrant hover:bg-accent/50 transition-colors" title="Documentation">
                                  <BookOpen className="h-3.5 w-3.5" />
                                </a>
                              )}
                            </div>
                          </TableCell>

                          {/* Delete */}
                          {role !== "intern" && (
                            <TableCell>
                              <Button
                                variant="ghost"
                                size="icon"
                                className="h-7 w-7 text-destructive hover:text-destructive"
                                onClick={() => { if (window.confirm(`Remove "${item.name}" from watchlist?`)) deleteItem(item.id); }}
                              >
                                <Trash2 className="h-3.5 w-3.5" />
                              </Button>
                            </TableCell>
                          )}
                        </TableRow>

                        {/* Expanded panel */}
                        {isExpanded && (
                          <TableRow className="animate-slide-down border-0">
                            <TableCell colSpan={9} className="p-0">
                              <div className="bg-accent/20 border-t border-border px-10 py-4">
                                <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-4">
                                  {/* Installed version */}
                                  <div>
                                    <label className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1 block">Installed</label>
                                    <EditableField value={item.installed_version} onSave={(v) => updateField(item.id, "installed_version", v)} placeholder="—" />
                                  </div>
                                  {/* Latest version */}
                                  <div>
                                    <label className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1 block">Latest</label>
                                    <div className="flex items-center gap-2">
                                      <EditableField value={item.latest_version} onSave={(v) => updateField(item.id, "latest_version", v)} placeholder="—" />
                                      <Button
                                        size="sm"
                                        variant="outline"
                                        className="h-7 text-[10px] gap-1 shrink-0"
                                        onClick={() => checkVersion(item.id)}
                                        disabled={refreshingVer === item.id}
                                      >
                                        <RefreshCw className={cn("h-3 w-3", refreshingVer === item.id && "animate-spin")} />
                                        {refreshingVer === item.id ? "Fetching..." : "Fetch"}
                                      </Button>
                                    </div>
                                  </div>
                                  {/* Ecosystem */}
                                  <div>
                                    <label className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1 block">Ecosystem</label>
                                    <EcosystemSelect value={item.ecosystem ?? "npm"} onChange={(v) => updateField(item.id, "ecosystem", v)} />
                                  </div>
                                  {/* Vulns */}
                                  <div>
                                    <label className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1 block">Known Vulns</label>
                                    <div className="flex items-center gap-2">
                                      <div className="flex-1 text-sm px-1.5 truncate">
                                        {(() => {
                                          if (!item.known_vulns) return <span className="text-muted-foreground/50 italic">Check CVEs</span>;
                                          try {
                                            const parsed = JSON.parse(item.known_vulns);
                                            return (
                                              <span className="text-xs text-muted-foreground">
                                                {parsed.summaryText ?? `${parsed.totalCount} CVE${parsed.totalCount > 1 ? "s" : ""}`}
                                                {parsed.lastChecked && ` — ${relativeTime(parsed.lastChecked)}`}
                                              </span>
                                            );
                                          } catch {
                                            return <span className="text-xs text-muted-foreground">{item.known_vulns}</span>;
                                          }
                                        })()}
                                      </div>
                                      <Button
                                        size="sm"
                                        variant="outline"
                                        className="h-7 text-[10px] gap-1 shrink-0"
                                        onClick={() => checkCve(item.id)}
                                        disabled={refreshingCve === item.id}
                                      >
                                        <RefreshCw className={cn("h-3 w-3", refreshingCve === item.id && "animate-spin")} />
                                        {refreshingCve === item.id ? "Checking..." : "Refresh"}
                                      </Button>
                                    </div>
                                  </div>
                                  {/* Risk Reason */}
                                  <div>
                                    <label className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1 block">Risk Reason</label>
                                    <EditableField value={item.risk_reason} onSave={(v) => updateField(item.id, "risk_reason", v)} placeholder="Add reason..." />
                                  </div>
                                  {/* Upgrade notes */}
                                  <div>
                                    <label className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1 block">Upgrade Notes</label>
                                    <EditableField value={item.upgrade_notes} onSave={(v) => updateField(item.id, "upgrade_notes", v)} placeholder="Add note..." />
                                  </div>
                                  {/* Migration link */}
                                  <div>
                                    <label className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1 block">Migration Link</label>
                                    <div className="flex items-center gap-1">
                                      <EditableField value={item.migration_link} onSave={(v) => updateField(item.id, "migration_link", v)} placeholder="URL..." />
                                      {item.migration_link && (
                                        <a href={item.migration_link} target="_blank" rel="noopener noreferrer" className="shrink-0 inline-flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground hover:text-accent-vibrant hover:bg-accent/50 transition-colors">
                                          <ExternalLink className="h-3.5 w-3.5" />
                                        </a>
                                      )}
                                    </div>
                                  </div>
                                </div>
                              </div>
                            </TableCell>
                          </TableRow>
                        )}
                      </Fragment>
                    );
                  })}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        {/* Footer stats */}
        {items.length > 0 && (
          <div className="animate-fade-in mt-4 flex items-center gap-3 text-xs text-muted-foreground">
            <span>{search ? `${filtered.length} of ${items.length}` : `${items.length} total`}</span>
            <span className="flex items-center gap-1">
              <span className="h-1.5 w-1.5 rounded-full bg-rose-500" /> {counts.high} high
            </span>
            <span className="flex items-center gap-1">
              <span className="h-1.5 w-1.5 rounded-full bg-amber-500" /> {counts.medium} medium
            </span>
            <span className="flex items-center gap-1">
              <span className="h-1.5 w-1.5 rounded-full bg-emerald-500" /> {counts.low} low
            </span>
          </div>
        )}
      </div>
    </div>
  );
}

function Fragment({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}

function PackageSearchInput({
  value,
  ecosystem,
  onSelect,
}: {
  value: string;
  ecosystem: string;
  onSelect: (name: string, eco: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [results, setResults] = useState<{ name: string; ecosystem: string }[]>([]);
  const [loading, setLoading] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  useEffect(() => {
    if (!value.trim()) { setResults([]); setOpen(false); return; }
    setLoading(true);
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = setTimeout(async () => {
      try {
        const res = await fetch(`/api/packages/search?q=${encodeURIComponent(value)}`);
        if (res.ok) {
          const data = await res.json();
          setResults(data.results ?? []);
          setOpen(data.results?.length > 0);
        }
      } catch {} finally {
        setLoading(false);
      }
    }, 200);
    return () => { if (timerRef.current) clearTimeout(timerRef.current); };
  }, [value]);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  return (
    <div ref={ref} className="relative">
      <Input
        placeholder="e.g. Tailwind CSS"
        value={value}
        onChange={(e) => onSelect(e.target.value, ecosystem)}
      />
      {open && (
        <div className="absolute z-50 top-full mt-1 w-full rounded-md border border-border bg-popover shadow-md overflow-hidden">
          {loading ? (
            <div className="px-3 py-2 text-xs text-muted-foreground">Searching...</div>
          ) : results.length === 0 ? (
            <div className="px-3 py-2 text-xs text-muted-foreground">No suggestions</div>
          ) : (
            results.map((r) => (
              <button
                key={`${r.ecosystem}:${r.name}`}
                className="w-full flex items-center justify-between px-3 py-2 text-left text-sm hover:bg-accent transition-colors"
                onClick={() => { onSelect(r.name, r.ecosystem); setOpen(false); }}
              >
                <span>{r.name}</span>
                {r.ecosystem && (
                  <span className="text-[10px] font-mono text-muted-foreground bg-muted px-1.5 py-0.5 rounded">
                    {r.ecosystem}
                  </span>
                )}
              </button>
            ))
          )}
        </div>
      )}
    </div>
  );
}

function EcosystemSelect({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  const [open, setOpen] = useState(false);
  return (
    <Select value={value} onValueChange={onChange} open={open} onOpenChange={setOpen}>
      <SelectTrigger className="h-7 text-xs px-1.5">
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        {ECOSYSTEMS.map((e) => <SelectItem key={e} value={e}>{e}</SelectItem>)}
      </SelectContent>
    </Select>
  );
}

function EditableField({
  value,
  onSave,
  placeholder,
}: {
  value: string | null;
  onSave: (v: string) => void;
  placeholder?: string;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(value ?? "");

  if (editing) {
    return (
      <Input
        className="h-7 text-xs px-1.5"
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onBlur={() => {
          setEditing(false);
          if (draft !== (value ?? "")) onSave(draft);
        }}
        onKeyDown={(e) => {
          if (e.key === "Enter") (e.target as HTMLInputElement).blur();
          if (e.key === "Escape") { setDraft(value ?? ""); setEditing(false); }
        }}
        autoFocus
        placeholder={placeholder}
      />
    );
  }

  return (
    <button
      className="h-7 w-full text-left text-sm px-1.5 rounded hover:bg-accent/50 transition-colors cursor-pointer truncate max-w-[200px]"
      onClick={() => { setDraft(value ?? ""); setEditing(true); }}
      title={value ?? placeholder ?? "Click to edit"}
    >
      {value || <span className="text-muted-foreground/50 italic">{placeholder || "—"}</span>}
    </button>
  );
}
