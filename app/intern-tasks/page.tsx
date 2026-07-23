"use client";

import { useState, useMemo } from "react";
import { INTERN_TASKS, type InternTask } from "@/src/config/intern-tasks";
import { InternTaskCard } from "@/components/intern-tasks/intern-task-card";
import { BookOpen, GraduationCap, ListOrdered } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const CATEGORIES = [
  { value: "all", label: "All" },
  { value: "synthetic-data", label: "Synthetic Data" },
  { value: "mock-apis", label: "Mock APIs" },
  { value: "local-db", label: "Local DB" },
  { value: "code-review", label: "Code Review" },
  { value: "docs-research", label: "Docs & Research" },
  { value: "git-workflow", label: "Git Workflow" },
] as const;

const DIFFICULTIES = ["all", "beginner", "intermediate", "advanced"] as const;

const CATEGORY_COLORS: Record<string, string> = {
  "synthetic-data":
    "bg-purple-500/10 text-purple-600 dark:text-purple-400 border-purple-500/20",
  "mock-apis":
    "bg-blue-500/10 text-blue-600 dark:text-blue-400 border-blue-500/20",
  "local-db":
    "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border-emerald-500/20",
  "code-review":
    "bg-orange-500/10 text-orange-600 dark:text-orange-400 border-orange-500/20",
  "docs-research":
    "bg-pink-500/10 text-pink-600 dark:text-pink-400 border-pink-500/20",
  "git-workflow":
    "bg-cyan-500/10 text-cyan-600 dark:text-cyan-400 border-cyan-500/20",
};

const DIFFICULTY_COLORS: Record<string, string> = {
  beginner:
    "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border-emerald-500/20",
  intermediate:
    "bg-amber-500/10 text-amber-600 dark:text-amber-400 border-amber-500/20",
  advanced: "bg-red-500/10 text-red-600 dark:text-red-400 border-red-500/20",
};

function CategoryPill({
  value,
  label,
  active,
  onClick,
}: {
  value: string;
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className={cn(
        "inline-flex items-center rounded-full px-3 py-1 text-xs font-medium transition-all duration-150 border",
        active
          ? "bg-accent text-accent-foreground border-border shadow-sm"
          : "bg-transparent text-muted-foreground border-border/40 hover:border-border hover:text-foreground",
      )}
    >
      {label}
    </button>
  );
}

export default function InternTasksPage() {
  const [category, setCategory] = useState("all");
  const [difficulty, setDifficulty] = useState("all");
  const [sort, setSort] = useState("default");

  const filtered = useMemo(() => {
    let result = [...INTERN_TASKS];

    if (category !== "all") {
      result = result.filter((t) => t.category === category);
    }
    if (difficulty !== "all") {
      result = result.filter((t) => t.difficulty === difficulty);
    }

    switch (sort) {
      case "difficulty-asc":
        result.sort((a, b) => {
          const order = ["beginner", "intermediate", "advanced"];
          return order.indexOf(a.difficulty) - order.indexOf(b.difficulty);
        });
        break;
      case "difficulty-desc":
        result.sort((a, b) => {
          const order = ["beginner", "intermediate", "advanced"];
          return order.indexOf(b.difficulty) - order.indexOf(a.difficulty);
        });
        break;
      case "title-asc":
        result.sort((a, b) => a.title.localeCompare(b.title));
        break;
      case "title-desc":
        result.sort((a, b) => b.title.localeCompare(a.title));
        break;
    }

    return result;
  }, [category, difficulty, sort]);

  return (
    <div className="min-h-screen">
      <div className="mx-auto max-w-7xl px-4 md:px-6 py-6 md:py-8">
        {/* Header */}
        <div className="flex items-start justify-between gap-4 flex-wrap mb-6">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
              <BookOpen className="h-5 w-5 text-accent-vibrant shrink-0" />
              <h1 className="text-3xl font-bold tracking-tight">
                Intern Safe Task Board
              </h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1">
              Safe, isolated tasks for learning and contributing
            </p>
          </div>
        </div>

        {/* Filters */}
        <div
          className="animate-slide-up mb-6 space-y-4"
          style={{ animationDelay: "100ms" }}
        >
          {/* Category pills */}
          <div className="flex flex-wrap gap-2">
            {CATEGORIES.map((cat) => (
              <CategoryPill
                key={cat.value}
                value={cat.value}
                label={cat.label}
                active={category === cat.value}
                onClick={() => setCategory(cat.value)}
              />
            ))}
          </div>

          {/* Difficulty + Sort controls */}
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex items-center gap-2">
              <GraduationCap className="h-4 w-4 text-muted-foreground" />
              <Select value={difficulty} onValueChange={setDifficulty}>
                <SelectTrigger className="w-40 h-9 text-sm">
                  <SelectValue placeholder="Difficulty" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Levels</SelectItem>
                  <SelectItem value="beginner">Beginner</SelectItem>
                  <SelectItem value="intermediate">Intermediate</SelectItem>
                  <SelectItem value="advanced">Advanced</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="flex items-center gap-2">
              <ListOrdered className="h-4 w-4 text-muted-foreground" />
              <Select value={sort} onValueChange={setSort}>
                <SelectTrigger className="w-44 h-9 text-sm">
                  <SelectValue placeholder="Sort" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="default">Default</SelectItem>
                  <SelectItem value="difficulty-asc">Difficulty ↑</SelectItem>
                  <SelectItem value="difficulty-desc">Difficulty ↓</SelectItem>
                  <SelectItem value="title-asc">Title A–Z</SelectItem>
                  <SelectItem value="title-desc">Title Z–A</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <span className="text-xs text-muted-foreground sm:ml-auto sm:text-right">
              Showing {filtered.length} of {INTERN_TASKS.length} tasks
            </span>
          </div>
        </div>

        {/* Grid */}
        {filtered.length > 0 ? (
          <div
            className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4"
            style={{ animationDelay: "200ms" }}
          >
            {filtered.map((task, i) => (
              <InternTaskCard
                key={task.title}
                task={task}
                index={i}
                categoryColor={CATEGORY_COLORS[task.category]}
                difficultyColor={DIFFICULTY_COLORS[task.difficulty]}
              />
            ))}
          </div>
        ) : (
          <div className="animate-fade-in flex flex-col items-center justify-center py-20 text-center">
            <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-muted mb-4">
              <BookOpen className="h-6 w-6 text-muted-foreground" />
            </div>
            <h3 className="text-lg font-semibold text-foreground mb-1">
              No tasks match your filters
            </h3>
            <p className="text-sm text-muted-foreground mb-4 max-w-sm">
              Try changing the category or difficulty to see more tasks.
            </p>
            <button
              onClick={() => {
                setCategory("all");
                setDifficulty("all");
                setSort("default");
              }}
              className="inline-flex items-center gap-1.5 rounded-lg bg-accent px-3 py-2 text-sm font-medium text-accent-foreground hover:bg-accent/80 transition-colors"
            >
              Reset Filters
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
