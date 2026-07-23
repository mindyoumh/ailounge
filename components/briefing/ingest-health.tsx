import { cn } from "@/lib/utils";
import { getIngestionStatus, getGlobalIngestionStatus } from "@/src/lib/analytics";
import { serviceClient } from "@/src/db/service-client";
import { Activity, Radio, TrendingUp, Rss, Radar } from "lucide-react";

const SOURCE_CONFIG: Record<string, { icon: React.ElementType; color: string }> = {
  hn: { icon: Radio, color: "text-orange-500" },
  github_trending: { icon: TrendingUp, color: "text-purple-500" },
  rss: { icon: Rss, color: "text-blue-500" },
  repo_radar: { icon: Radar, color: "text-teal-500" },
};

function timeAgo(dateStr: string): string {
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

export async function IngestHealth() {
  const statuses = await getIngestionStatus();
  const errors = statuses.filter((s) => s.status === "error").length;

  return (
    <div className="rounded-xl border border-border bg-card p-4 transition-all duration-200 hover:shadow-sm">
      <div className="flex items-center justify-between mb-2.5">
        <div className="flex items-center gap-2">
          <div className="flex h-6 w-6 items-center justify-center rounded-md bg-accent/70">
            <Activity className="h-3.5 w-3.5 text-muted-foreground" />
          </div>
          <h3 className="text-sm font-semibold text-foreground font-display">Ingestion</h3>
        </div>
        <span
          className={cn(
            "inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium font-mono",
            errors > 0
              ? "bg-red-500/10 text-red-600 dark:text-red-400"
              : "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400",
          )}
        >
          {errors > 0 ? `${errors} error${errors > 1 ? "s" : ""}` : "Healthy"}
        </span>
      </div>
      {statuses.length === 0 ? (
        <p className="text-xs text-muted-foreground py-3 text-center font-mono">
          No data. Run <code className="text-xs bg-muted px-1 rounded">npm run ingest</code>
        </p>
      ) : (
        <div className="space-y-1">
          {statuses.map((s) => {
            const cfg = SOURCE_CONFIG[s.source] ?? { icon: Activity, color: "text-muted-foreground" };
            const Icon = cfg.icon;
            return (
              <div
                key={s.source}
                className="flex items-center justify-between rounded-lg px-2.5 py-1.5 transition-colors hover:bg-accent/40"
              >
                <div className="flex items-center gap-2 min-w-0">
                  <span className={cn(
                    "inline-flex h-1.5 w-1.5 rounded-full shrink-0",
                    s.status === "ok" ? "bg-emerald-400" : s.status === "error" ? "bg-red-400" : "bg-muted-foreground/30",
                  )} />
                  <Icon className={cn("h-3 w-3 shrink-0", cfg.color)} />
                  <span className="text-xs text-foreground/80 truncate">
                    {s.source.replace("_", " ")}
                  </span>
                </div>
                <div className="flex items-center gap-1.5 text-[11px] text-muted-foreground font-mono shrink-0">
                  {s.lastRun && <span>{timeAgo(s.lastRun)}</span>}
                  <span className={cn(
                    "font-medium",
                    s.status === "ok" && "text-emerald-600 dark:text-emerald-400",
                    s.status === "error" && "text-red-600 dark:text-red-400",
                  )}>
                    +{s.count}
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
