"use client";

import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { BarChart3 } from "lucide-react";
import { ResponsiveBar } from "@nivo/bar";

export interface BreakdownItem {
  name: string;
  count: number;
}

interface FeedBreakdownProps {
  sources: BreakdownItem[];
  categories: BreakdownItem[];
  total: number;
  delay?: number;
}

const SOURCE_COLORS: Record<string, string> = {
  hn: "#ff6600",
  github_trending: "#6e40c9",
  rss: "#3b82f6",
};

const PALETTE = [
  "#f59e0b",
  "#ef4444",
  "#8b5cf6",
  "#00d4aa",
  "#84cc16",
  "#ec4899",
  "#f97316",
  "#14b8a6",
];

function getColor(name: string): string {
  if (SOURCE_COLORS[name]) return SOURCE_COLORS[name];
  let hash = 0;
  for (let i = 0; i < name.length; i++) {
    hash = name.charCodeAt(i) + ((hash << 5) - hash);
  }
  return PALETTE[Math.abs(hash) % PALETTE.length];
}

function Chart({ items, total }: { items: BreakdownItem[]; total: number }) {
  if (items.length === 0) {
    return (
      <p className="text-sm text-muted-foreground py-4 text-center">
        No data yet.
      </p>
    );
  }

  const data = items.map((item) => ({
    name: item.name,
    count: item.count,
    pct: total > 0 ? Math.round((item.count / total) * 100) : 0,
    color: getColor(item.name),
  }));

  return (
    <div className="h-64">
      <ResponsiveBar
        data={data}
        keys={["count"]}
        indexBy="name"
        layout="horizontal"
        margin={{ top: 0, right: 70, bottom: 0, left: 100 }}
        padding={0.3}
        valueScale={{ type: "linear" }}
        colors={{ datum: "data.color" }}
        borderRadius={4}
        borderColor={{ from: "color", modifiers: [["darker", 0.2]] }}
        axisLeft={{
          tickSize: 0,
          tickPadding: 8,
        }}
        axisBottom={null}
        enableGridX={false}
        enableGridY={false}
        label={(d) => `${d.value}`}
        labelSkipWidth={40}
        labelTextColor="white"
        tooltip={({ data: d }) => (
          <div className="rounded-lg border border-border bg-popover px-3 py-2 text-sm shadow-md">
            <span className="font-medium text-foreground">{d.name}</span>
            <span className="text-muted-foreground">: {d.count.toLocaleString()} ({d.pct}%)</span>
          </div>
        )}
        theme={{
          axis: {
            ticks: {
              text: { fill: "var(--color-muted-foreground)", fontSize: 12 },
            },
          },
        }}
        motionConfig="gentle"
      />
    </div>
  );
}

export function FeedBreakdown({
  sources,
  categories,
  total,
  delay = 0,
}: FeedBreakdownProps) {
  return (
    <div
      className="animate-slide-up rounded-2xl border border-border/50 bg-card/50 backdrop-blur-xl transition-all duration-300 hover:shadow-lg"
      style={{ animationDelay: `${delay}ms` }}
    >
      <div className="px-5 pt-4 pb-2">
        <div className="flex items-center gap-2">
          <div className="flex h-7 w-7 items-center justify-center rounded-lg bg-teal-500/20">
            <BarChart3 className="h-3.5 w-3.5 text-teal-600 dark:text-teal-400" />
          </div>
          <h3 className="text-sm font-semibold text-foreground font-display tracking-wide">
            Feed Breakdown
          </h3>
        </div>
      </div>

      {/* Mobile: tabs */}
      <div className="px-5 pb-5 lg:hidden">
        <Tabs defaultValue="sources">
          <TabsList className="w-full bg-muted">
            <TabsTrigger value="sources" className="flex-1 data-[state=active]:bg-background">
              By Source
            </TabsTrigger>
            <TabsTrigger value="categories" className="flex-1 data-[state=active]:bg-background">
              By Category
            </TabsTrigger>
          </TabsList>
          <TabsContent value="sources" className="mt-4">
            <Chart items={sources} total={total} />
          </TabsContent>
          <TabsContent value="categories" className="mt-4">
            <Chart items={categories} total={total} />
          </TabsContent>
        </Tabs>
      </div>

      {/* Desktop: side-by-side */}
      <div className="hidden lg:grid lg:grid-cols-2 lg:gap-6 px-5 pb-5">
        <div>
          <h4 className="text-xs font-medium text-muted-foreground mb-3 tracking-wide">
            By Source
          </h4>
          <Chart items={sources} total={total} />
        </div>
        <div>
          <h4 className="text-xs font-medium text-muted-foreground mb-3 tracking-wide">
            By Category
          </h4>
          <Chart items={categories} total={total} />
        </div>
      </div>
    </div>
  );
}
