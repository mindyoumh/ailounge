import Link from "next/link";
import { ChevronRight } from "lucide-react";
import { FeedItemCard } from "./feed-item-card";
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

export type SectionTheme = keyof typeof THEME_STYLES;

const THEME_STYLES = {
  teal: {
    iconContainer: "bg-teal-100 dark:bg-teal-900/50",
    icon: "text-teal-600 dark:text-teal-400",
    badge: "bg-teal-100 dark:bg-teal-900/50 text-teal-600 dark:text-teal-400",
    linkHover: "hover:text-teal-600 dark:hover:text-teal-400",
  },
  purple: {
    iconContainer: "bg-purple-100 dark:bg-purple-900/50",
    icon: "text-purple-600 dark:text-purple-400",
    badge: "bg-purple-100 dark:bg-purple-900/50 text-purple-600 dark:text-purple-400",
    linkHover: "hover:text-purple-600 dark:hover:text-purple-400",
  },
  blue: {
    iconContainer: "bg-blue-100 dark:bg-blue-900/50",
    icon: "text-blue-600 dark:text-blue-400",
    badge: "bg-blue-100 dark:bg-blue-900/50 text-blue-600 dark:text-blue-400",
    linkHover: "hover:text-blue-600 dark:hover:text-blue-400",
  },
  rose: {
    iconContainer: "bg-rose-100 dark:bg-rose-900/50",
    icon: "text-rose-600 dark:text-rose-400",
    badge: "bg-rose-100 dark:bg-rose-900/50 text-rose-600 dark:text-rose-400",
    linkHover: "hover:text-rose-600 dark:hover:text-rose-400",
  },
} as const;

interface FeedSectionProps {
  title: string;
  icon: React.ElementType;
  items: FeedItem[];
  viewAllHref?: string;
  theme?: SectionTheme;
}

export function FeedSection({ title, icon: Icon, items, viewAllHref, theme }: FeedSectionProps) {
  const s = theme ? THEME_STYLES[theme] : null;
  return (
    <div className="rounded-xl border border-border bg-card transition-all duration-200 hover:shadow-sm">
      <div className="flex items-center justify-between px-4 pt-3.5 pb-2">
        <div className="flex items-center gap-2">
          <div className={cn("flex h-6 w-6 items-center justify-center rounded-md", s?.iconContainer ?? "bg-accent/70")}>
            <Icon className={cn("h-3.5 w-3.5", s?.icon ?? "text-muted-foreground")} />
          </div>
          <h3 className="text-sm font-semibold text-foreground font-display">{title}</h3>
          <span className={cn("inline-flex items-center justify-center rounded-full text-[10px] font-semibold h-4 min-w-4 px-1 font-mono", s?.badge ?? "bg-accent/80 text-muted-foreground")}>
            {items.length}
          </span>
        </div>
        {viewAllHref && (
          <Link
            href={viewAllHref}
            className={cn("flex items-center gap-0.5 text-[11px] font-medium transition-colors", s?.linkHover ?? "text-muted-foreground/70 hover:text-foreground")}
          >
            View all
            <ChevronRight className="h-3 w-3" />
          </Link>
        )}
      </div>
      <div className="pb-2">
        {items.length === 0 ? (
          <div className="flex flex-col items-center py-6 text-muted-foreground">
            <Icon className="h-6 w-6 opacity-20" />
            <p className="text-xs mt-1">No items yet</p>
          </div>
        ) : (
          items.map((item) => (
            <FeedItemCard key={item.id} item={item} />
          ))
        )}
      </div>
    </div>
  );
}
