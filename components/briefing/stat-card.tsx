import { cn } from "@/lib/utils";

interface StatCardProps {
  label: string;
  value: string | number;
  icon: React.ElementType;
  trend?: { value: string; positive: boolean };
  theme?: "blue" | "teal" | "purple" | "amber" | "rose";
}

const THEME_STYLES = {
  blue: { container: "bg-blue-100 dark:bg-blue-900/50", icon: "text-blue-600 dark:text-blue-400" },
  teal: { container: "bg-teal-100 dark:bg-teal-900/50", icon: "text-teal-600 dark:text-teal-400" },
  purple: { container: "bg-purple-100 dark:bg-purple-900/50", icon: "text-purple-600 dark:text-purple-400" },
  amber: { container: "bg-amber-100 dark:bg-amber-900/50", icon: "text-amber-600 dark:text-amber-400" },
  rose: { container: "bg-rose-100 dark:bg-rose-900/50", icon: "text-rose-600 dark:text-rose-400" },
} as const;

export function StatCard({ label, value, icon: Icon, trend, theme }: StatCardProps) {
  const s = theme ? THEME_STYLES[theme] : null;
  return (
    <div className="group relative overflow-hidden rounded-xl border border-border bg-card transition-all duration-200 hover:-translate-y-0.5 hover:shadow-sm">
      <div className="p-4">
        <div className="flex items-start justify-between">
          <div className="min-w-0">
            <div className="text-2xl font-bold tabular-nums tracking-tight text-foreground font-display">
              {value}
            </div>
            <div className="text-xs text-muted-foreground mt-0.5">
              {label}
            </div>
            {trend && (
              <div className={cn(
                "flex items-center gap-1 mt-1 text-[11px] font-medium",
                trend.positive ? "text-emerald-600 dark:text-emerald-400" : "text-red-600 dark:text-red-400",
              )}>
                <span>{trend.positive ? "↑" : "↓"}</span>
                <span>{trend.value}</span>
              </div>
            )}
          </div>
          <div className={cn("flex h-8 w-8 shrink-0 items-center justify-center rounded-lg", s?.container ?? "bg-accent/60")}>
            <Icon className={cn("h-4 w-4", s?.icon ?? "text-muted-foreground")} />
          </div>
        </div>
      </div>
    </div>
  );
}
