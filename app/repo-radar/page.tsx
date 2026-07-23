"use client";

import { useState, useEffect, useCallback } from "react";
import {
  RadioTower,
  RefreshCw,
  Plus,
  ExternalLink,
  Star,
  GitPullRequest,
  Bug,
  AlertTriangle,
  ShieldAlert,
  StickyNote,
  Trash2,
  X,
  Clock,
  Activity,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { toast } from "sonner";
import { cn } from "@/lib/utils";
import { useUser } from "@/components/auth-provider";

interface RepoItem {
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
}

const LANGUAGE_STYLES: Record<string, { dot: string; label: string }> = {
  TypeScript: { dot: "bg-blue-500", label: "TypeScript" },
  JavaScript: { dot: "bg-yellow-400", label: "JavaScript" },
  Python: { dot: "bg-amber-500", label: "Python" },
  Rust: { dot: "bg-orange-600", label: "Rust" },
  Go: { dot: "bg-cyan-500", label: "Go" },
  Ruby: { dot: "bg-red-500", label: "Ruby" },
  Java: { dot: "bg-orange-500", label: "Java" },
  "C#": { dot: "bg-green-600", label: "C#" },
  Swift: { dot: "bg-orange-500", label: "Swift" },
  Kotlin: { dot: "bg-purple-500", label: "Kotlin" },
  Shell: { dot: "bg-green-500", label: "Shell" },
  "C++": { dot: "bg-blue-600", label: "C++" },
  C: { dot: "bg-blue-600", label: "C" },
  HTML: { dot: "bg-orange-500", label: "HTML" },
  CSS: { dot: "bg-blue-400", label: "CSS" },
  Vue: { dot: "bg-emerald-500", label: "Vue" },
  Dockerfile: { dot: "bg-blue-400", label: "Docker" },
};

function getLanguageStyle(lang: string | null) {
  if (!lang) return { dot: "bg-gray-400", label: "Unknown" };
  return LANGUAGE_STYLES[lang] ?? { dot: "bg-gray-400", label: lang };
}

function timeAgo(dateStr: string | null): string {
  if (!dateStr) return "N/A";
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  return `${Math.floor(days / 30)}mo ago`;
}

function CardSkeleton() {
  return (
    <div className="rounded-xl border bg-card p-5 space-y-4 animate-pulse">
      <div className="flex items-center justify-between">
        <Skeleton className="h-5 w-40" />
        <Skeleton className="h-4 w-4 rounded-full" />
      </div>
      <div className="flex items-center gap-2">
        <Skeleton className="h-3 w-3 rounded-full" />
        <Skeleton className="h-4 w-20" />
      </div>
      <div className="flex items-center gap-2">
        <Skeleton className="h-4 w-16" />
        <Skeleton className="h-5 w-24 rounded-full" />
      </div>
      <Skeleton className="h-4 w-32" />
      <div className="space-y-2">
        <Skeleton className="h-4 w-full" />
        <Skeleton className="h-4 w-2/3" />
      </div>
      <div className="flex justify-between gap-2">
        <Skeleton className="h-8 flex-1 rounded-md" />
        <Skeleton className="h-8 w-8 rounded-md" />
      </div>
    </div>
  );
}

function ConfirmDialog({
  open,
  title,
  message,
  onConfirm,
  onCancel,
  destructiveLabel = "Delete",
}: {
  open: boolean;
  title: string;
  message: string;
  onConfirm: () => void;
  onCancel: () => void;
  destructiveLabel?: string;
}) {
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div
        className="absolute inset-0 bg-black/40 backdrop-blur-sm"
        onClick={onCancel}
      />
      <div className="relative z-50 w-full max-w-sm rounded-xl border bg-card p-6 shadow-xl animate-scale-in">
        <h3 className="text-lg font-semibold">{title}</h3>
        <p className="mt-2 text-sm text-muted-foreground">{message}</p>
        <div className="mt-4 flex justify-end gap-3">
          <Button variant="outline" size="sm" onClick={onCancel}>
            Cancel
          </Button>
          <Button variant="destructive" size="sm" onClick={onConfirm}>
            {destructiveLabel}
          </Button>
        </div>
      </div>
    </div>
  );
}

export default function RepoRadarPage() {
  const { role } = useUser();
  const [items, setItems] = useState<RepoItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [adding, setAdding] = useState(false);
  const [showAdd, setShowAdd] = useState(false);
  const [addInput, setAddInput] = useState("");
  const [addError, setAddError] = useState("");
  const [rateLimitError, setRateLimitError] = useState("");
  const [deleteTarget, setDeleteTarget] = useState<RepoItem | null>(null);
  const [editingNotes, setEditingNotes] = useState<number | null>(null);
  const [notesText, setNotesText] = useState("");

  const fetchItems = useCallback(async () => {
    try {
      const res = await fetch("/api/repo-radar");
      const data = await res.json();
      setItems(data.items ?? []);
    } catch (err) {
      console.error("Failed to fetch repos:", err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchItems();
  }, [fetchItems]);

  const handleRefresh = async () => {
    setRefreshing(true);
    setRateLimitError("");
    try {
      const res = await fetch("/api/repo-radar/refresh", { method: "POST" });
      const data = await res.json();
      if (!res.ok) {
        if (data.error?.includes("rate limit")) setRateLimitError(data.error);
        toast.error(data.error || "Refresh failed");
        return;
      }
      toast.success(
        `Refreshed ${data.updated} repos${data.errors > 0 ? `, ${data.errors} failed` : ""}`,
      );
      await fetchItems();
    } catch {
      toast.error("Network error during refresh");
    } finally {
      setRefreshing(false);
    }
  };

  const handleAdd = async () => {
    const match = addInput.trim().match(/^([\w.-]+)\/([\w.-]+)$/);
    if (!match) {
      setAddError("Use format: owner/repo (e.g., vercel/next.js)");
      return;
    }
    setAdding(true);
    setAddError("");
    try {
      const res = await fetch("/api/repo-radar", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ owner: match[1], repo: match[2] }),
      });
      const data = await res.json();
      if (!res.ok) {
        setAddError(data.error || "Failed to add repo");
        return;
      }
      toast.success(`Added ${match[1]}/${match[2]}`);
      setAddInput("");
      setShowAdd(false);
      await fetchItems();
    } catch {
      setAddError("Network error");
    } finally {
      setAdding(false);
    }
  };

  const handleDelete = async () => {
    if (!deleteTarget) return;
    try {
      const res = await fetch(`/api/repo-radar/${deleteTarget.id}`, {
        method: "DELETE",
      });
      if (!res.ok) {
        toast.error("Failed to delete");
        return;
      }
      toast.success(`Removed ${deleteTarget.full_name}`);
      setItems((prev) => prev.filter((i) => i.id !== deleteTarget.id));
    } catch {
      toast.error("Network error");
    } finally {
      setDeleteTarget(null);
    }
  };

  const handleSaveNotes = async (id: number) => {
    try {
      await fetch(`/api/repo-radar/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ notes: notesText }),
      });
    } catch {
      // silent
    }
    setEditingNotes(null);
  };

  const lastRefresh = items.length > 0 ? items[0].last_refreshed_at : null;

  return (
    <div className="mx-auto max-w-7xl px-4 md:px-6 py-6 md:py-8">
      <div className="flex items-start justify-between gap-4 flex-wrap mb-6">
        <div className="flex-1 min-w-0">
          <h1 className="text-3xl font-bold tracking-tight">Repo Radar</h1>
          <p className="text-sm text-muted-foreground mt-1">
            {loading ? "Loading..." : `${items.length} repos tracked`}
            {lastRefresh && !loading
              ? ` · Last refreshed ${timeAgo(lastRefresh)}`
              : ""}
          </p>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <button
            onClick={() => setShowAdd(!showAdd)}
            className="inline-flex items-center gap-1.5 rounded-lg border border-border bg-card px-3 py-2 text-xs font-medium text-muted-foreground transition-all duration-200 hover:bg-accent hover:text-foreground"
          >
            <Plus className="h-3.5 w-3.5" />
            Add
          </button>
          <Button
            size="sm"
            onClick={handleRefresh}
            disabled={refreshing || loading}
          >
            <RefreshCw
              className={cn("h-4 w-4", refreshing && "animate-spin")}
            />
            {refreshing ? "Refreshing..." : "Refresh"}
          </Button>
        </div>
      </div>

      {rateLimitError && (
        <div className="mb-6 flex items-center gap-3 rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-800 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-200">
          <AlertTriangle className="h-5 w-5 shrink-0" />
          <span>{rateLimitError}</span>
          <button
            onClick={() => setRateLimitError("")}
            className="ml-auto shrink-0"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
      )}

      {showAdd && (
        <Card className="mb-6 border-primary/30 animate-slide-down">
          <CardContent className="pt-6">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-end">
              <div className="flex-1">
                <label className="mb-1 block text-xs font-medium text-muted-foreground">
                  Repository
                </label>
                <Input
                  placeholder="owner/repo (e.g., vercel/next.js)"
                  value={addInput}
                  onChange={(e) => {
                    setAddInput(e.target.value);
                    setAddError("");
                  }}
                  onKeyDown={(e) => e.key === "Enter" && handleAdd()}
                />
                {addError && (
                  <p className="mt-1 text-xs text-destructive">{addError}</p>
                )}
              </div>
              <Button
                onClick={handleAdd}
                disabled={adding}
                size="sm"
                className="gap-1.5 shrink-0"
              >
                {adding ? (
                  <RefreshCw className="h-4 w-4 animate-spin" />
                ) : (
                  <Plus className="h-4 w-4" />
                )}
                Add
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {loading && (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {Array.from({ length: 6 }).map((_, i) => (
            <div
              key={i}
              className="animate-slide-up"
              style={{ animationDelay: `${i * 50}ms` }}
            >
              <CardSkeleton />
            </div>
          ))}
        </div>
      )}

      {!loading && items.length === 0 && (
        <div className="flex flex-col items-center justify-center py-20 text-center">
          <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-2xl bg-muted">
            <RadioTower className="h-8 w-8 text-muted-foreground" />
          </div>
          <h3 className="text-lg font-semibold">No repos tracked</h3>
          <p className="mt-1 text-sm text-muted-foreground">
            Add your first repo to start monitoring
          </p>
          <Button
            className="mt-4 gap-1.5"
            size="sm"
            onClick={() => setShowAdd(true)}
          >
            <Plus className="h-4 w-4" />
            Add Repository
          </Button>
        </div>
      )}

      {!loading && items.length > 0 && (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {items.map((item, i) => (
            <RepoCard
              key={item.id}
              item={item}
              index={i}
              role={role}
              editingNotes={editingNotes}
              notesText={notesText}
              onStartEdit={(id, notes) => {
                setEditingNotes(id);
                setNotesText(notes ?? "");
              }}
              onNotesChange={setNotesText}
              onSaveNotes={() => handleSaveNotes(item.id)}
              onCancelEdit={() => setEditingNotes(null)}
              onDelete={() => setDeleteTarget(item)}
            />
          ))}
        </div>
      )}

      <ConfirmDialog
        open={!!deleteTarget}
        title="Remove Repository"
        message={`Remove ${deleteTarget?.full_name ?? ""} from tracking?`}
        onConfirm={handleDelete}
        onCancel={() => setDeleteTarget(null)}
      />
    </div>
  );
}

function RepoCard({
  item,
  index,
  role,
  editingNotes,
  notesText,
  onStartEdit,
  onNotesChange,
  onSaveNotes,
  onCancelEdit,
  onDelete,
}: {
  item: RepoItem;
  index: number;
  role: string | null;
  editingNotes: number | null;
  notesText: string;
  onStartEdit: (id: number, notes: string | null) => void;
  onNotesChange: (val: string) => void;
  onSaveNotes: () => void;
  onCancelEdit: () => void;
  onDelete: () => void;
}) {
  const langStyle = getLanguageStyle(item.language);
  const isEditing = editingNotes === item.id;

  return (
    <div
      className="group relative rounded-xl border bg-card p-5 transition-all duration-200 hover:-translate-y-0.5 hover:shadow-md hover:border-accent-vibrant/50 animate-slide-up"
      style={{ animationDelay: `${index * 50}ms` }}
    >
      <div className="mb-3 flex items-start justify-between">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="truncate text-sm font-semibold">
              {item.owner}/
              <span className="text-foreground/80">{item.repo}</span>
            </span>
            <a
              href={item.url}
              target="_blank"
              rel="noopener noreferrer"
              className="shrink-0 text-muted-foreground transition-colors hover:text-foreground"
              aria-label={`Open ${item.full_name} on GitHub`}
            >
              <ExternalLink className="h-3.5 w-3.5" />
            </a>
          </div>
          {item.description && (
            <p className="mt-0.5 line-clamp-2 text-xs text-muted-foreground">
              {item.description}
            </p>
          )}
        </div>
      </div>

      <div className="mb-3 flex items-center justify-between text-xs">
        <div className="flex items-center gap-1.5">
          <span className={cn("h-2.5 w-2.5 rounded-full", langStyle.dot)} />
          <span className="text-muted-foreground">{langStyle.label}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <Star className="h-3.5 w-3.5 fill-amber-400 text-amber-400" />
          <span className="font-medium">{item.stars.toLocaleString()}</span>
          {item.stars_gained > 0 && (
            <span className="rounded-full border border-emerald-200 bg-emerald-50 px-1.5 py-0.5 text-[10px] font-medium text-emerald-700 dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-300">
              +{item.stars_gained}
            </span>
          )}
        </div>
      </div>

      {item.latest_release && (
        <div className="mb-2 flex items-center gap-2 text-xs">
          <a
            href={item.latest_release_url ?? "#"}
            target="_blank"
            rel="noopener noreferrer"
            className="font-mono text-primary underline-offset-2 hover:underline"
          >
            {item.latest_release}
          </a>
          {item.breaking_changes && (
            <span className="inline-flex items-center gap-1 rounded-full bg-destructive/10 px-1.5 py-0.5 text-[10px] font-medium text-destructive">
              <AlertTriangle className="h-3 w-3" />
              Breaking
            </span>
          )}
          {item.security_advisory && (
            <span className="inline-flex items-center gap-1 rounded-full border border-amber-200 bg-amber-50 px-1.5 py-0.5 text-[10px] font-medium text-amber-700 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-300">
              <ShieldAlert className="h-3 w-3" />
              Advisory
            </span>
          )}
        </div>
      )}

      <div className="mb-2 flex items-center gap-3 text-xs text-muted-foreground">
        <div className="flex items-center gap-1">
          <GitPullRequest className="h-3.5 w-3.5" />
          <span>
            <strong className="text-foreground">{item.prs_opened_7d}</strong>{" "}
            opened
          </span>
        </div>
        <span className="text-muted-foreground/40">·</span>
        <div className="flex items-center gap-1">
          <GitPullRequest className="h-3.5 w-3.5 text-emerald-500" />
          <span>
            <strong className="text-foreground">{item.prs_merged_7d}</strong>{" "}
            merged
          </span>
        </div>
        <span className="text-xs text-muted-foreground">/ 7d</span>
      </div>

      <div className="mb-3 flex items-center justify-between text-xs">
        <div className="flex items-center gap-1.5">
          <Bug className="h-3.5 w-3.5 text-muted-foreground" />
          <span className="text-muted-foreground">
            Issues:{" "}
            <strong className="text-foreground">{item.open_issues}</strong>
          </span>
          {item.issue_spike === 1 && (
            <span className="inline-flex items-center gap-1 rounded-full bg-destructive/10 px-1.5 py-0.5 text-[10px] font-medium text-destructive">
              <Activity className="h-3 w-3" />
              Spike
            </span>
          )}
        </div>
        <div className="flex items-center gap-1 text-muted-foreground">
          <Clock className="h-3 w-3" />
          <span>{timeAgo(item.last_activity_at)}</span>
        </div>
      </div>

      <div className="mb-3 border-t" />

      <div className="flex items-center justify-between">
        <div className="min-w-0 flex-1">
          {isEditing ? (
            <div className="flex items-center gap-2">
              <input
                className="w-full rounded-md border bg-background px-2 py-1 text-xs outline-none focus:border-primary"
                value={notesText}
                onChange={(e) => onNotesChange(e.target.value)}
                onBlur={onSaveNotes}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    onSaveNotes();
                  }
                  if (e.key === "Escape") onCancelEdit();
                }}
                autoFocus
                placeholder="Add notes..."
              />
            </div>
          ) : (
            <button
              onClick={() => onStartEdit(item.id, item.notes)}
              className="flex items-center gap-1.5 text-xs text-muted-foreground transition-colors hover:text-foreground"
            >
              <StickyNote className="h-3.5 w-3.5" />
              {item.notes ? (
                <span className="line-clamp-1">{item.notes}</span>
              ) : (
                <span className="italic">Add notes...</span>
              )}
            </button>
          )}
        </div>
        {role !== "intern" && (
          <button
            onClick={onDelete}
            className="ml-2 shrink-0 rounded-md p-1.5 text-muted-foreground md:opacity-0 transition-all hover:bg-destructive/10 hover:text-destructive focus:opacity-100 md:group-hover:opacity-100"
            aria-label={`Remove ${item.full_name}`}
          >
            <Trash2 className="h-4 w-4" />
          </button>
        )}
      </div>
    </div>
  );
}
