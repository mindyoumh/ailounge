import Link from "next/link";
import { ExternalLink } from "lucide-react";
import { cn } from "@/lib/utils";

const SOURCE_COLORS: Record<string, string> = {
  hn: "bg-orange-100 dark:bg-orange-900/50 text-orange-600 dark:text-orange-400",
  github_trending: "bg-purple-100 dark:bg-purple-900/50 text-purple-600 dark:text-purple-400",
  rss: "bg-blue-100 dark:bg-blue-900/50 text-blue-600 dark:text-blue-400",
  repo_radar: "bg-teal-100 dark:bg-teal-900/50 text-teal-600 dark:text-teal-400",
};

interface FeedItem {
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
}

interface FeaturedNewsProps {
  items: FeedItem[];
}

export function FeaturedNews({ items }: FeaturedNewsProps) {
  if (items.length === 0) return null;

  const top = items[0];
  const rest = items.slice(1, 4);

  return (
    <div className="space-y-3">
      {/* Hero card */}
      <Link
        href={top.url}
        target="_blank"
        rel="noopener noreferrer"
        className="group relative flex flex-col overflow-hidden rounded-xl border border-border bg-card transition-all duration-200 hover:shadow-sm"
      >
        <div className="absolute top-0 left-0 right-0 h-0.5 bg-gradient-to-r from-accent-vibrant/60 to-accent-vibrant/20" />
        <div className="p-5">
          <div className="flex items-center gap-2 mb-2">
            <span className={cn("inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium font-mono", SOURCE_COLORS[top.source] ?? "bg-accent/80 text-muted-foreground")}>
              {top.source.replace("_", " ")}
            </span>
            {top.published_at && (
              <span className="text-[11px] text-muted-foreground/60">
                {new Date(top.published_at).toLocaleDateString("en-US", {
                  month: "short",
                  day: "numeric",
                })}
              </span>
            )}
            <span className="text-[11px] text-muted-foreground/40">&middot;</span>
            <span className="inline-flex items-center rounded-full px-1.5 py-0.5 text-[9px] font-medium font-mono bg-amber-100 dark:bg-amber-900/50 text-amber-600 dark:text-amber-400">
              Pinned
            </span>
          </div>
          <h2 className="text-lg font-semibold text-foreground font-display leading-snug tracking-tight group-hover:text-accent-vibrant transition-colors text-balance">
            {top.title}
          </h2>
          {top.summary && (
            <p className="text-sm text-muted-foreground mt-1.5 line-clamp-2 leading-relaxed">
              {top.summary}
            </p>
          )}
          <span className="inline-flex items-center gap-1 text-xs font-medium text-accent-vibrant mt-3 opacity-0 group-hover:opacity-100 transition-opacity">
            Read more <ExternalLink className="h-3 w-3" />
          </span>
        </div>
      </Link>

      {/* Secondary cards */}
      {rest.length > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
          {rest.map((item) => (
            <Link
              key={item.id}
              href={item.url}
              target="_blank"
              rel="noopener noreferrer"
              className="group rounded-xl border border-border bg-card p-3 transition-all duration-200 hover:shadow-sm"
            >
              <div className="flex items-center gap-1.5 mb-1.5">
                <span className={cn("inline-flex items-center rounded-full px-1.5 py-0.5 text-[9px] font-medium font-mono", SOURCE_COLORS[item.source] ?? "bg-accent/80 text-muted-foreground")}>
                  {item.source.replace("_", " ")}
                </span>
              </div>
              <p className="text-sm leading-snug text-foreground/85 group-hover:text-foreground transition-colors line-clamp-2">
                {item.title}
              </p>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}
