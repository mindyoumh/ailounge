"use client";
import { Suspense, useCallback, useEffect, useState } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import {
  Search,
  Pin,
  PinOff,
  Target,
  Check,
  CheckCheck,
  Trash2,
  Plus,
  X,
  ExternalLink,
  RotateCcw,
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
import { cn } from "@/lib/utils";
import { CardSkeleton } from "@/components/ui/skeleton";

type FeedItem = {
  id: number;
  source: string;
  category: string;
  title: string;
  url: string;
  summary: string | null;
  tags: string | null;
  score: number | null;
  published_at: string | null;
  fetched_at: string;
  is_pinned: number;
  is_read: number;
  ai_relevance_score: number | null;
  ai_relevance_label: string | null;
};

type FeedResponse = {
  items: FeedItem[];
  total: number;
  limit: number;
  offset: number;
};

const SOURCES = ["hn", "rss", "github_trending"];
const CATEGORIES = [
  "ai",
  "cloud",
  "devops",
  "django",
  "nextjs",
  "hn",
  "github",
  "security",
  "rumors",
  "general",
];

function formatDate(dateStr: string | null): string {
  if (!dateStr) return "";
  const d = new Date(dateStr);
  return d.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function timeAgo(dateStr: string): string {
  const now = Date.now();
  const then = new Date(dateStr).getTime();
  const diff = now - then;
  const mins = Math.floor(diff / 60000);
  if (mins < 60) return `${mins}m`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h`;
  const days = Math.floor(hrs / 24);
  if (days < 30) return `${days}d`;
  return formatDate(dateStr);
}

const SOURCE_BADGE: Record<string, string> = {
  hn: "bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200",
  rss: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200",
  github_trending:
    "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200",
  manual: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
};

export const dynamic = "force-dynamic";

export default function FeedPage() {
  return (
    <Suspense
      fallback={
        <div className="min-h-screen bg-background">
          <div className="mx-auto max-w-7xl px-6 py-8 space-y-2">
            <CardSkeleton />
            <CardSkeleton />
            <CardSkeleton />
          </div>
        </div>
      }
    >
      <FeedContent />
    </Suspense>
  );
}

function FilterBar({
  source,
  setSource,
  category,
  setCategory,
  q,
  setQ,
  isRead,
  setIsRead,
  isPinned,
  setIsPinned,
  sort,
  setSort,
  hasFilters,
  clearFilters,
  onRefresh,
  total,
}: {
  source: string;
  setSource: (v: string) => void;
  category: string;
  setCategory: (v: string) => void;
  q: string;
  setQ: (v: string) => void;
  isRead: string;
  setIsRead: (v: string) => void;
  isPinned: string;
  setIsPinned: (v: string) => void;
  sort: string;
  setSort: (v: string) => void;
  hasFilters: boolean;
  clearFilters: () => void;
  onRefresh: () => void;
  total: number;
}) {
  return (
    <div className="sticky top-14 z-40 -mx-4 px-4 py-3 mb-4 bg-background/70 backdrop-blur-xl supports-[backdrop-filter]:bg-background/60 border-b">
      <div className="flex flex-wrap gap-2 items-center">
        <div className="relative flex-1 min-w-[140px]">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search title..."
            value={q}
            onChange={(e) => setQ(e.target.value)}
            className="pl-9"
          />
        </div>
        <Select
          value={source}
          onValueChange={(v) => setSource(v === "all" ? "" : v)}
        >
          <SelectTrigger className="w-28 sm:w-32">
            <SelectValue placeholder="Source" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All sources</SelectItem>
            {SOURCES.map((s) => (
              <SelectItem key={s} value={s}>
                {s.replace("_", " ")}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select
          value={category}
          onValueChange={(v) => setCategory(v === "all" ? "" : v)}
        >
          <SelectTrigger className="w-28 sm:w-32">
            <SelectValue placeholder="Category" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All categories</SelectItem>
            {CATEGORIES.map((c) => (
              <SelectItem key={c} value={c}>
                {c}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Select
          value={isRead}
          onValueChange={(v) => setIsRead(v === "all" ? "" : v)}
        >
          <SelectTrigger className="w-24 sm:w-28">
            <SelectValue placeholder="Status" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All</SelectItem>
            <SelectItem value="0">Unread</SelectItem>
            <SelectItem value="1">Read</SelectItem>
          </SelectContent>
        </Select>
        <Select
          value={isPinned}
          onValueChange={(v) => setIsPinned(v === "all" ? "" : v)}
        >
          <SelectTrigger className="w-24 sm:w-28">
            <SelectValue placeholder="Pinned" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All</SelectItem>
            <SelectItem value="1">Pinned</SelectItem>
            <SelectItem value="0">Unpinned</SelectItem>
          </SelectContent>
        </Select>
        <Button
          variant={sort === "relevance" ? "default" : "outline"}
          size="sm"
          className="h-9 gap-1.5"
          onClick={() => setSort(sort === "relevance" ? "" : "relevance")}
          title="Sort by relevance"
        >
          <Target className="h-4 w-4" />
          Relevant
        </Button>
        {hasFilters && (
          <Button variant="ghost" size="sm" onClick={clearFilters}>
            <X className="h-4 w-4" />
            Clear
          </Button>
        )}
        <Button
          variant="ghost"
          size="icon"
          className="h-9 w-9"
          onClick={onRefresh}
          title="Refresh"
        >
          <RotateCcw className="h-4 w-4" />
        </Button>
      </div>
      <p className="text-xs text-muted-foreground mt-2">
        {total} item{total !== 1 ? "s" : ""}
      </p>
    </div>
  );
}

function FeedContent() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const [items, setItems] = useState<FeedItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deletingIds, setDeletingIds] = useState<Set<number>>(new Set());

  const [source, setSource] = useState(searchParams.get("source") || "");
  const [category, setCategory] = useState(searchParams.get("category") || "");
  const [q, setQ] = useState(searchParams.get("q") || "");
  const [isRead, setIsRead] = useState(searchParams.get("is_read") || "");
  const [isPinned, setIsPinned] = useState(searchParams.get("is_pinned") || "");
  const [sort, setSort] = useState(searchParams.get("sort") || "");
  const [page, setPage] = useState(Number(searchParams.get("page")) || 1);
  const pageSize = 50;
  const totalPages = Math.ceil(total / pageSize);

  const [showAddForm, setShowAddForm] = useState(false);
  const [addTitle, setAddTitle] = useState("");
  const [addUrl, setAddUrl] = useState("");
  const [addCategory, setAddCategory] = useState("general");
  const [addTags, setAddTags] = useState("");

  const [lastVisit, setLastVisit] = useState<string | null>(null);

  useEffect(() => {
    const stored = localStorage.getItem("feed_last_visited_at");
    if (stored) setLastVisit(stored);
    localStorage.setItem("feed_last_visited_at", new Date().toISOString());
  }, []);

  const fetchItems = useCallback(
    async () => {
      setLoading(true);
      setError(null);
      const params = new URLSearchParams();
      if (source) params.set("source", source);
      if (category) params.set("category", category);
      if (q) params.set("q", q);
      if (isRead !== "") params.set("is_read", isRead);
      if (isPinned !== "") params.set("is_pinned", isPinned);
      if (sort) params.set("sort", sort);
      params.set("offset", String((page - 1) * pageSize));
      params.set("limit", String(pageSize));

      try {
        const res = await fetch(`/api/feed?${params}`);
        if (!res.ok) throw new Error(`Status ${res.status}`);
        const data: FeedResponse = await res.json();
        setItems(data.items);
        setTotal(data.total);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to fetch");
      } finally {
        setLoading(false);
      }
    },
    [source, category, q, isRead, isPinned, sort, page],
  );

  useEffect(() => {
    fetchItems();
  }, [fetchItems]);

  // Reset to page 1 when filters change
  useEffect(() => {
    setPage(1);
  }, [source, category, q, isRead, isPinned, sort]);

  // Sync filters to URL
  useEffect(() => {
    const params = new URLSearchParams();
    if (source) params.set("source", source);
    if (category) params.set("category", category);
    if (q) params.set("q", q);
    if (isRead) params.set("is_read", isRead);
    if (isPinned) params.set("is_pinned", isPinned);
    if (sort) params.set("sort", sort);
    if (page > 1) params.set("page", String(page));
    const qs = params.toString();
    router.replace(`/feed${qs ? `?${qs}` : ""}`, { scroll: false });
  }, [source, category, q, isRead, isPinned, sort, page, router]);

  const hasFilters = !!(
    source ||
    category ||
    q ||
    isRead !== "" ||
    isPinned !== "" ||
    sort
  );

  const clearFilters = () => {
    setSource("");
    setCategory("");
    setQ("");
    setIsRead("");
    setIsPinned("");
    setSort("");
  };

  const togglePin = async (item: FeedItem) => {
    const res = await fetch(`/api/feed/${item.id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ is_pinned: !item.is_pinned }),
    });
    if (res.ok) {
      setItems(
        items.map((i) =>
          i.id === item.id ? { ...i, is_pinned: i.is_pinned ? 0 : 1 } : i,
        ),
      );
    }
  };

  const toggleRead = async (item: FeedItem) => {
    const res = await fetch(`/api/feed/${item.id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ is_read: !item.is_read }),
    });
    if (res.ok) {
      setItems(
        items.map((i) =>
          i.id === item.id ? { ...i, is_read: i.is_read ? 0 : 1 } : i,
        ),
      );
    }
  };

  const deleteItem = async (id: number) => {
    setDeletingIds((prev) => new Set(prev).add(id));
    const res = await fetch(`/api/feed/${id}`, { method: "DELETE" });
    if (res.ok) {
      setTimeout(() => {
        setItems((prev) => prev.filter((i) => i.id !== id));
        setTotal((prev) => prev - 1);
        setDeletingIds((prev) => {
          const next = new Set(prev);
          next.delete(id);
          return next;
        });
      }, 200);
    } else {
      setDeletingIds((prev) => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
    }
  };

  const addItem = async () => {
    if (!addTitle.trim() || !addUrl.trim()) return;
    const res = await fetch("/api/feed", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        title: addTitle,
        url: addUrl,
        category: addCategory,
        source: "manual",
        tags: addTags || null,
      }),
    });
    if (res.ok) {
      setAddTitle("");
      setAddUrl("");
      setAddCategory("general");
      setAddTags("");
      setShowAddForm(false);
      fetchItems();
    } else {
      const data = await res.json();
      alert(data.error || "Failed to add item");
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <div className="mx-auto max-w-7xl px-4 md:px-6 py-6 md:py-8">
        <div className="flex items-start justify-between gap-4 flex-wrap mb-6">
          <div className="flex-1 min-w-0">
            <h1 className="text-3xl font-bold tracking-tight">Feed</h1>
            <p className="text-sm text-muted-foreground mt-1">
              Developer Intelligence Feed
            </p>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <button
              onClick={() => setShowAddForm(!showAddForm)}
              className="inline-flex items-center gap-1.5 rounded-lg border border-border bg-card px-3 py-2 text-xs font-medium text-muted-foreground transition-all duration-200 hover:bg-accent hover:text-foreground"
            >
              {showAddForm ? (
                <X className="h-3.5 w-3.5" />
              ) : (
                <Plus className="h-3.5 w-3.5" />
              )}
              {showAddForm ? "Cancel" : "Add Item"}
            </button>
          </div>
        </div>

        {showAddForm && (
          <Card className="mb-6 border-primary/30 animate-slide-down">
            <CardContent className="pt-6">
              <div className="grid gap-3">
                <div>
                  <label className="mb-1 block text-xs font-medium text-muted-foreground">Title</label>
                  <Input
                    placeholder="e.g. New AI Framework Released"
                    value={addTitle}
                    onChange={(e) => setAddTitle(e.target.value)}
                  />
                </div>
                <div>
                  <label className="mb-1 block text-xs font-medium text-muted-foreground">URL</label>
                  <Input
                    placeholder="https://example.com/article"
                    value={addUrl}
                    onChange={(e) => setAddUrl(e.target.value)}
                  />
                </div>
                <div className="flex flex-col sm:flex-row gap-3">
                  <div className="flex-1">
                    <label className="mb-1 block text-xs font-medium text-muted-foreground">Category</label>
                    <Select value={addCategory} onValueChange={setAddCategory}>
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {CATEGORIES.map((c) => (
                          <SelectItem key={c} value={c}>
                            {c}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="flex-1">
                    <label className="mb-1 block text-xs font-medium text-muted-foreground">Tags</label>
                    <Input
                      placeholder="e.g. ai, llm, tools"
                      value={addTags}
                      onChange={(e) => setAddTags(e.target.value)}
                    />
                  </div>
                </div>
              </div>
              <div className="flex justify-end gap-2 mt-3">
                <Button variant="outline" onClick={() => setShowAddForm(false)}>
                  Cancel
                </Button>
                <Button size="sm" onClick={addItem}>
                  Add to Feed
                </Button>
              </div>
            </CardContent>
          </Card>
        )}

        <FilterBar
          source={source}
          setSource={setSource}
          category={category}
          setCategory={setCategory}
          q={q}
          setQ={setQ}
          isRead={isRead}
          setIsRead={setIsRead}
          isPinned={isPinned}
          setIsPinned={setIsPinned}
          sort={sort}
          setSort={setSort}
          hasFilters={hasFilters}
          clearFilters={clearFilters}
          onRefresh={() => fetchItems()}
          total={total}
        />

        {lastVisit &&
          items.filter((i) => new Date(i.fetched_at) > new Date(lastVisit))
            .length > 0 && (
            <div className="animate-slide-down mb-4 px-4 py-2.5 rounded-lg bg-blue-50/80 dark:bg-blue-950/50 border border-blue-200 dark:border-blue-800 text-sm text-blue-700 dark:text-blue-300 flex items-center gap-2">
              <CheckCheck className="h-4 w-4 shrink-0" />
              {
                items.filter(
                  (i) => new Date(i.fetched_at) > new Date(lastVisit),
                ).length
              }{" "}
              new since your last visit
            </div>
          )}

        {error && (
          <div className="mb-4 px-4 py-2 rounded-md bg-red-50 dark:bg-red-950 border border-red-200 dark:border-red-800 text-sm text-red-700 dark:text-red-300">
            {error}
          </div>
        )}

        <div className="space-y-2">
          {loading && items.length === 0 ? (
            <div className="space-y-2">
              {Array.from({ length: 5 }).map((_, i) => (
                <CardSkeleton key={i} />
              ))}
            </div>
          ) : items.length === 0 ? (
            <div className="text-center py-12 text-muted-foreground animate-fade-in">
              No items found{hasFilters ? " matching your filters" : ""}.
            </div>
          ) : (
            items.map((item, index) => (
              <div
                key={item.id}
                className={cn(
                  deletingIds.has(item.id) &&
                    "animate-scale-in pointer-events-none opacity-0 scale-95",
                )}
                style={{
                  animation: deletingIds.has(item.id)
                    ? "scale-in 0.2s ease-out reverse"
                    : !loading
                      ? `slide-up 0.3s ${index * 30}ms both ease-out`
                      : undefined,
                }}
              >
                <Card
                  className={cn(
                    "transition-all duration-200 hover:-translate-y-0.5 hover:border-accent-vibrant/30 hover:shadow-md",
                    item.is_read && "opacity-60",
                    item.is_pinned && "border-primary/40 bg-primary/[0.02]",
                  )}
                >
                  <CardContent className="py-3 px-4">
                    <div className="flex items-start gap-3">
                      <div className="flex flex-col gap-1.5 flex-1 min-w-0">
                        <div className="flex items-center gap-2 flex-wrap">
                          <Badge
                            variant="secondary"
                            className={cn(
                              "text-[10px] px-1.5 py-0 font-medium",
                              SOURCE_BADGE[item.source],
                            )}
                          >
                            {item.source.replace("_", " ")}
                          </Badge>
                          <Badge
                            variant="outline"
                            className="text-[10px] px-1.5 py-0"
                          >
                            {item.category}
                          </Badge>
                          {item.ai_relevance_label && (
                            <Badge
                              variant="secondary"
                              className={cn(
                                "text-[10px] px-1.5 py-0 font-medium",
                                item.ai_relevance_score !== null && item.ai_relevance_score >= 70
                                  ? "bg-teal-500/10 text-teal-600 dark:text-teal-400"
                                  : item.ai_relevance_score !== null && item.ai_relevance_score >= 50
                                    ? "bg-amber-500/10 text-amber-600 dark:text-amber-400"
                                    : "bg-blue-500/10 text-blue-600 dark:text-blue-400",
                              )}
                            >
                              <Target className="h-3 w-3 mr-0.5" />
                              {item.ai_relevance_score !== null && `${item.ai_relevance_score} `}
                              {item.ai_relevance_label}
                            </Badge>
                          )}
                          <span className="text-[11px] text-muted-foreground">
                            {formatDate(item.published_at)}
                          </span>
                          {item.score !== null && (
                            <span className="text-[11px] text-muted-foreground ml-auto">
                              Score: {item.score}
                            </span>
                          )}
                        </div>
                        <a
                          href={item.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-sm font-medium leading-snug hover:text-accent-vibrant transition-colors inline-flex items-center gap-1"
                        >
                          {item.title}
                          <ExternalLink className="h-3 w-3 shrink-0 text-muted-foreground" />
                        </a>
                        {item.tags && (
                          <div className="flex gap-1 flex-wrap">
                            {item.tags.split(",").map((tag) => (
                              <span
                                key={tag.trim()}
                                className="text-[10px] px-1.5 py-0.5 rounded bg-muted text-muted-foreground transition-colors hover:bg-accent-vibrant/10 hover:text-accent-vibrant"
                              >
                                {tag.trim()}
                              </span>
                            ))}
                          </div>
                        )}
                        {item.summary && (
                          <p className="text-xs text-muted-foreground line-clamp-2">
                            {item.summary}
                          </p>
                        )}
                      </div>
                      <div className="flex flex-row sm:flex-col gap-1 shrink-0">
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-7 w-7 transition-transform active:scale-90"
                          onClick={() => togglePin(item)}
                          title={item.is_pinned ? "Unpin" : "Pin"}
                        >
                          {item.is_pinned ? (
                            <PinOff className="h-3.5 w-3.5" />
                          ) : (
                            <Pin className="h-3.5 w-3.5" />
                          )}
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-7 w-7 transition-transform active:scale-90"
                          onClick={() => toggleRead(item)}
                          title={item.is_read ? "Mark unread" : "Mark read"}
                        >
                          {item.is_read ? (
                            <CheckCheck className="h-3.5 w-3.5 text-green-500" />
                          ) : (
                            <Check className="h-3.5 w-3.5" />
                          )}
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-7 w-7 text-destructive hover:text-destructive transition-transform active:scale-90"
                          onClick={() => deleteItem(item.id)}
                          title="Delete"
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                        </Button>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </div>
            ))
          )}
        </div>

        {totalPages > 1 && (
          <div className="flex items-center justify-center gap-4 mt-8 pb-8 animate-fade-in">
            <Button
              variant="outline"
              size="sm"
              disabled={page <= 1}
              onClick={() => setPage((p) => p - 1)}
            >
              ← Prev
            </Button>
            <span className="text-sm text-muted-foreground font-mono">
              Page {page} / {totalPages}
            </span>
            <Button
              variant="outline"
              size="sm"
              disabled={page >= totalPages}
              onClick={() => setPage((p) => p + 1)}
            >
              Next →
            </Button>
          </div>
        )}
      </div>
    </div>
  );
}
