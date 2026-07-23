import { ExternalLink } from "lucide-react";
import { cn } from "@/lib/utils";

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

const SOURCE_COLORS: Record<string, string> = {
  hn: "text-orange-600 dark:text-orange-400",
  github_trending: "text-purple-600 dark:text-purple-400",
  rss: "text-blue-600 dark:text-blue-400",
  repo_radar: "text-teal-600 dark:text-teal-400",
};

export function FeedItemCard({ item }: { item: FeedItem }) {
  const sourceColor = SOURCE_COLORS[item.source];
  return (
    <a
      href={item.url}
      target="_blank"
      rel="noopener noreferrer"
      className="group flex items-start gap-3 rounded-lg px-3 py-2 transition-colors hover:bg-accent/40"
    >
      <div className="min-w-0 flex-1">
        <p className="text-sm leading-snug text-foreground/85 group-hover:text-foreground transition-colors line-clamp-2">
          {item.title}
        </p>
        <div className="flex items-center gap-2 mt-1">
          <span className={cn("text-[11px] font-medium font-mono", sourceColor ?? "text-muted-foreground/70")}>
            {item.source.replace("_", " ")}
          </span>
          {item.published_at && (
            <span className="text-[11px] text-muted-foreground/50">
              {new Date(item.published_at).toLocaleDateString("en-US", {
                month: "short",
                day: "numeric",
              })}
            </span>
          )}
        </div>
      </div>
      <ExternalLink className="h-3.5 w-3.5 shrink-0 mt-1 text-muted-foreground/30 group-hover:text-muted-foreground/70 transition-colors" />
    </a>
  );
}
