import { cn } from "@/lib/utils";
import { INTERN_TASKS } from "@/src/config/intern-tasks";
import { BookOpen, ArrowRight, Sparkles } from "lucide-react";
import Link from "next/link";

interface FeedItem {
  id: number;
  title: string;
  url: string;
  summary: string | null;
  tags: string | null;
}

interface InternTaskData {
  title: string;
  description: string;
  difficulty: "beginner" | "intermediate" | "advanced";
  category: "synthetic-data" | "mock-apis" | "local-db" | "code-review" | "docs-research" | "git-workflow";
}

interface InternTasksProps {
  recommendedItem: FeedItem | null;
  todayTask: InternTaskData;
  tomorrowTask: InternTaskData;
}

const DIFFICULTY_COLORS = {
  beginner: "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400",
  intermediate: "bg-amber-500/10 text-amber-600 dark:text-amber-400",
  advanced: "bg-red-500/10 text-red-600 dark:text-red-400",
};

const CATEGORY_LABELS: Record<string, string> = {
  "synthetic-data": "Synthetic Data",
  "mock-apis": "Mock APIs",
  "local-db": "Local DB",
  "code-review": "Code Review",
  "docs-research": "Docs & Research",
  "git-workflow": "Git Workflow",
};

const CATEGORY_COLORS: Record<string, string> = {
  "synthetic-data": "bg-purple-500/10 text-purple-600 dark:text-purple-400",
  "mock-apis": "bg-blue-500/10 text-blue-600 dark:text-blue-400",
  "local-db": "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400",
  "code-review": "bg-orange-500/10 text-orange-600 dark:text-orange-400",
  "docs-research": "bg-pink-500/10 text-pink-600 dark:text-pink-400",
  "git-workflow": "bg-cyan-500/10 text-cyan-600 dark:text-cyan-400",
};

export function InternTasks({ recommendedItem, todayTask, tomorrowTask }: InternTasksProps) {
  return (
    <div className="space-y-3">
      {/* Recommended tool */}
      {recommendedItem && (
        <div className="rounded-xl border border-border bg-card p-4">
          <div className="flex items-center gap-2 mb-2.5">
            <div className="flex h-6 w-6 items-center justify-center rounded-md bg-accent/70">
              <Sparkles className="h-3.5 w-3.5 text-muted-foreground" />
            </div>
            <h3 className="text-sm font-semibold text-foreground font-display">Recommended</h3>
          </div>
          <Link
            href={recommendedItem.url}
            target="_blank"
            rel="noopener noreferrer"
            className="group block rounded-lg bg-accent/50 px-3 py-2 transition-colors hover:bg-accent"
          >
            <p className="text-sm font-medium text-foreground/90 group-hover:text-accent-vibrant transition-colors truncate">
              {recommendedItem.title}
            </p>
            <p className="text-xs text-muted-foreground mt-0.5 line-clamp-1">
              {recommendedItem.summary || recommendedItem.tags || "No description"}
            </p>
          </Link>
        </div>
      )}

      {/* Tasks */}
      <div className="rounded-xl border border-border bg-card p-4">
        <div className="flex items-center gap-2 mb-2.5">
          <div className="flex h-6 w-6 items-center justify-center rounded-md bg-accent/70">
            <BookOpen className="h-3.5 w-3.5 text-muted-foreground" />
          </div>
          <h3 className="text-sm font-semibold text-foreground font-display">Tasks</h3>
        </div>
        <TaskMiniCard task={todayTask} label="Today" isPrimary />
        <TaskMiniCard task={tomorrowTask} label="Tomorrow" isPrimary={false} />
        <Link
          href="/intern-tasks"
          className="group flex items-center justify-center gap-1 rounded-lg border border-border/60 bg-accent/30 px-3 py-1.5 mt-2 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
        >
          View all {INTERN_TASKS.length} tasks
          <ArrowRight className="h-3 w-3 transition-transform group-hover:translate-x-0.5" />
        </Link>
      </div>
    </div>
  );
}

function TaskMiniCard({ task, label, isPrimary }: { task: InternTaskData; label: string; isPrimary: boolean }) {
  return (
    <div className={cn(
      "rounded-lg px-3 py-2 mb-1.5 last:mb-0",
      isPrimary ? "bg-accent/50 border border-border/50" : "",
    )}>
      <div className="flex items-center gap-1.5 mb-0.5">
        <span className={cn(
          "text-[10px] font-mono uppercase tracking-wider",
          isPrimary ? "text-muted-foreground" : "text-muted-foreground/60",
        )}>
          {label}
        </span>
        <span className={cn(
          "inline-flex items-center rounded-full px-1 py-0.5 text-[8px] font-medium font-mono",
          CATEGORY_COLORS[task.category],
        )}>
          {CATEGORY_LABELS[task.category]}
        </span>
        <span className={cn(
          "inline-flex items-center rounded-full px-1 py-0.5 text-[8px] font-medium font-mono ml-auto",
          DIFFICULTY_COLORS[task.difficulty],
        )}>
          {task.difficulty}
        </span>
      </div>
      <p className={cn(
        "text-xs font-medium",
        isPrimary ? "text-foreground" : "text-foreground/80",
      )}>
        {task.title}
      </p>
    </div>
  );
}
