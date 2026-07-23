import { getIngestionStatus, getGlobalIngestionStatus } from "@/src/lib/analytics";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Activity } from "lucide-react";

function timeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  if (days < 30) return `${days}d ago`;
  return new Date(dateStr).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
  });
}

export async function AutomationStatus() {
  const statuses = await getIngestionStatus();
  const globalStatus = await getGlobalIngestionStatus();
  const errors = statuses.filter((s) => s.status === "error").length;

  return (
    <Card className="transition-all duration-300 hover:shadow-md">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="flex h-7 w-7 items-center justify-center rounded-md bg-accent-vibrant/10">
              <Activity className="h-4 w-4 text-accent-vibrant" />
            </div>
            <CardTitle className="text-lg">Automation Status</CardTitle>
          </div>
          {globalStatus && (
            <Badge
              variant={
                globalStatus === "ok"
                  ? "secondary"
                  : "destructive"
              }
              className="text-[10px]"
            >
              {errors > 0
                ? `${errors} source${errors > 1 ? "s" : ""} with errors`
                : "All sources healthy"}
            </Badge>
          )}
        </div>
      </CardHeader>
      <CardContent>
        {statuses.length === 0 ? (
          <p className="text-sm text-muted-foreground py-4 text-center">
            No ingestion data yet. Run{" "}
            <code className="text-xs bg-muted px-1 rounded">
              npm run ingest
            </code>{" "}
            to populate.
          </p>
        ) : (
          <div className="space-y-2">
            {statuses.map((s) => (
              <div
                key={s.source}
                className="flex items-center justify-between rounded-lg border bg-card px-3 py-2"
              >
                <div className="flex items-center gap-2 min-w-0">
                  <div
                    className={`h-2 w-2 shrink-0 rounded-full ${
                      s.status === "ok"
                        ? "bg-emerald-500"
                        : s.status === "error"
                          ? "bg-red-500"
                          : "bg-muted-foreground/30"
                    }`}
                  />
                  <span className="text-sm font-medium truncate">
                    {s.source.replace("_", " ")}
                  </span>
                </div>
                <div className="flex items-center gap-3 text-xs text-muted-foreground shrink-0">
                  {s.lastRun && (
                    <span>{timeAgo(s.lastRun)}</span>
                  )}
                  {s.status && (
                    <span
                      className={
                        s.status === "ok"
                          ? "text-emerald-600 dark:text-emerald-400"
                          : s.status === "error"
                            ? "text-red-600 dark:text-red-400"
                            : ""
                      }
                    >
                      {s.status}
                    </span>
                  )}
                  <span
                    className={
                      s.count > 0
                        ? "tabular-nums text-foreground"
                        : "tabular-nums"
                    }
                  >
                    +{s.count}
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
