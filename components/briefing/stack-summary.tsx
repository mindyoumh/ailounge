"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { Layers, ShieldX, ShieldAlert, ShieldCheck, ArrowRight } from "lucide-react";

interface StackStats {
  total: number;
  high: number;
  medium: number;
  low: number;
}

export function StackSummary() {
  const [stats, setStats] = useState<StackStats | null>(null);

  useEffect(() => {
    fetch("/api/stats")
      .then((r) => r.json())
      .then((data) => {
        setStats({
          total: data.stackTotal ?? 0,
          high: data.stackHigh ?? 0,
          medium: data.stackMedium ?? 0,
          low: data.stackLow ?? 0,
        });
      })
      .catch(() => {});
  }, []);

  if (!stats || stats.total === 0) return null;

  return (
    <Link href="/watchlist" className="block group">
      <div className="group/card relative overflow-hidden rounded-xl border border-border bg-card p-4 transition-all duration-200 hover:shadow-sm hover:border-accent-vibrant/30">
        <div className="absolute top-0 left-0 right-0 h-0.5 bg-gradient-to-r from-accent-vibrant/60 to-accent-vibrant/20 opacity-0 group-hover/card:opacity-100 transition-opacity" />
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 mb-2.5">
              <div className="flex h-6 w-6 items-center justify-center rounded-md bg-accent/70">
                <Layers className="h-3.5 w-3.5 text-muted-foreground" />
              </div>
              <h3 className="text-sm font-semibold text-foreground font-display">Stack</h3>
            </div>
            <div className="space-y-1">
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground text-xs">Total packages</span>
                <span className="font-medium text-sm">{stats.total}</span>
              </div>
              {stats.high > 0 && (
                <div className="flex items-center justify-between text-xs">
                  <span className="flex items-center gap-1 text-muted-foreground">
                    <ShieldX className="h-3 w-3 text-rose-500" /> High risk
                  </span>
                  <span className="font-medium text-rose-500">{stats.high}</span>
                </div>
              )}
              {stats.medium > 0 && (
                <div className="flex items-center justify-between text-xs">
                  <span className="flex items-center gap-1 text-muted-foreground">
                    <ShieldAlert className="h-3 w-3 text-amber-500" /> Medium
                  </span>
                  <span className="font-medium text-amber-500">{stats.medium}</span>
                </div>
              )}
              {stats.low > 0 && (
                <div className="flex items-center justify-between text-xs">
                  <span className="flex items-center gap-1 text-muted-foreground">
                    <ShieldCheck className="h-3 w-3 text-emerald-500" /> Low
                  </span>
                  <span className="font-medium text-emerald-500">{stats.low}</span>
                </div>
              )}
            </div>
          </div>
          <ArrowRight className="h-4 w-4 text-accent-vibrant mt-1.5 shrink-0 opacity-0 -translate-x-1 group-hover/card:opacity-100 group-hover/card:translate-x-0 transition-all duration-200" />
        </div>
      </div>
    </Link>
  );
}
