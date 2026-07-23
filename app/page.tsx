import { getServerComponentClient } from "@/src/db/server-client";
import {
  getItemsBySource,
  getItemsByCategory,
  getItemsToday,
  getItemsThisWeek,
  getLastGlobalIngestion,
} from "@/src/lib/analytics";
import { INTERN_TASKS } from "@/src/config/intern-tasks";
import { BookOpen, Rss } from "lucide-react";
import Link from "next/link";
import { Greeting } from "@/components/briefing/greeting";
import { StatCard } from "@/components/briefing/stat-card";
import { FeedSection } from "@/components/briefing/feed-section";
import { FeedBreakdown } from "@/components/briefing/feed-breakdown";
import type { BreakdownItem } from "@/components/briefing/feed-breakdown";
import { FeaturedNews } from "@/components/briefing/featured-news";
import { InternTasks } from "@/components/briefing/intern-tasks";
import { IngestHealth } from "@/components/briefing/ingest-health";
import { StackSummary } from "@/components/briefing/stack-summary";
import { FeaturedPrompt } from "@/components/briefing/featured-prompt";

interface PromptItem {
  id: number;
  title: string;
  content: string;
  category: string;
  description: string | null;
  usage_count: number;
  is_featured: number;
}

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

function timeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  if (days < 30) return `${days}d ago`;
  return new Date(dateStr).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

export const dynamic = "force-dynamic";

export default async function HomePage() {
  const supabase = await getServerComponentClient();
  const { data: { user } } = await supabase.auth.getUser();

  const [
    { data: aiData },
    { data: frameworkData },
    { data: trendingData },
    { data: securityData },
    { data: recommendedData },
    { count: totalCount },
    { data: featuredPromptData },
  ] = await Promise.all([
    supabase.from("feed_items").select("*").eq("category", "ai").neq("source", "manual").order("score", { ascending: false }).order("published_at", { ascending: false }).limit(3),
    supabase.from("feed_items").select("*").in("category", ["nextjs", "django"]).neq("source", "manual").order("published_at", { ascending: false }).order("fetched_at", { ascending: false }).limit(3),
    supabase.from("feed_items").select("*").eq("source", "github_trending").order("fetched_at", { ascending: false }).limit(3),
    supabase.from("feed_items").select("*").or("category.eq.security,tags.ilike.%cve%").neq("source", "manual").order("published_at", { ascending: false }).order("fetched_at", { ascending: false }).limit(3),
    supabase.from("feed_items").select("id, title, url, summary, tags").neq("source", "manual").or("tags.ilike.%ai%,tags.ilike.%tool%").order("score", { ascending: false }).order("fetched_at", { ascending: false }).limit(1).maybeSingle(),
    supabase.from("feed_items").select("*", { count: "exact", head: true }).neq("source", "manual"),
    supabase.from("prompts").select("*").eq("is_featured", 1).eq("source", "curated").order("id").limit(1).maybeSingle(),
  ]);

  const aiItems = (aiData ?? []).slice(0, 3) as FeedItem[];
  const frameworkItems = (frameworkData ?? []).slice(0, 3) as FeedItem[];
  const trendingItems = (trendingData ?? []).slice(0, 3) as FeedItem[];
  const securityItems = (securityData ?? []).slice(0, 3) as FeedItem[];
  const recommendedItem = (recommendedData as FeedItem | null) ?? null;
  const totalItems = totalCount ?? 0;
  const featuredPrompt = (featuredPromptData as PromptItem | null) ?? null;

  let featuredItems: FeedItem[] = [];
  if (user) {
    const { data: pinnedStates } = await supabase
      .from("user_feed_states")
      .select("feed_item_id")
      .eq("user_id", user.id)
      .eq("is_pinned", 1);

    const pinnedIds = (pinnedStates ?? []).map((p) => p.feed_item_id);
    if (pinnedIds.length > 0) {
      const { data: pinnedData } = await supabase
        .from("feed_items")
        .select("*")
        .in("id", pinnedIds)
        .order("published_at", { ascending: false })
        .limit(4);

      featuredItems = (pinnedData ?? []) as FeedItem[];
    }
  }

  const taskIndex = Math.floor(Date.now() / 86400000);
  const todayTask = INTERN_TASKS[taskIndex % INTERN_TASKS.length];
  const tomorrowTask = INTERN_TASKS[(taskIndex + 1) % INTERN_TASKS.length];

  const [itemsToday, itemsThisWeek, lastIngestion] = await Promise.all([
    getItemsToday(),
    getItemsThisWeek(),
    getLastGlobalIngestion(),
  ]);

  const [sourcesData, categoriesData] = await Promise.all([
    getItemsBySource(),
    getItemsByCategory(),
  ]);
  const sources: BreakdownItem[] = sourcesData.map((s) => ({
    name: s.source,
    count: s.count,
  }));
  const categories: BreakdownItem[] = categoriesData.map((c) => ({
    name: c.category,
    count: c.count,
  }));

  return (
    <div className="min-h-screen">
      <div className="mx-auto max-w-7xl px-4 md:px-6 py-6 md:py-8">
        {/* Header row */}
        <div className="flex items-start justify-between gap-4 flex-wrap mb-8">
          <div className="flex-1 min-w-0">
            <Greeting totalItems={totalItems} />
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <Link
              href="/feed"
              className="inline-flex items-center gap-1.5 rounded-lg border border-border bg-card px-3 py-2 text-xs font-medium text-muted-foreground transition-all duration-200 hover:bg-accent hover:text-foreground"
            >
              <Rss className="h-3.5 w-3.5" />
              Full Feed
            </Link>
          </div>
        </div>

        {/* Row 2: Featured hero + stat cards + stack */}
        <div className="grid grid-cols-1 lg:grid-cols-6 gap-4 mb-8">
          {/* Featured News — spans 4 cols */}
          <div className="lg:col-span-4">
            <FeaturedNews items={featuredItems} />
          </div>

          {/* Stat cards + Stack — spans 2 cols */}
          <div className="lg:col-span-2 space-y-3">
            <div className="grid grid-cols-2 gap-3">
              <StatCard
                label="Total Items"
                value={totalItems}
                icon={BookOpen}
                theme="blue"
              />
              <StatCard
                label="Items Today"
                value={itemsToday}
                icon={Rss}
                theme="teal"
                trend={itemsThisWeek > 0 ? { value: `${itemsThisWeek} this week`, positive: itemsToday >= itemsThisWeek / 7 } : undefined}
              />
              <StatCard
                label="This Week"
                value={itemsThisWeek}
                icon={BookOpen}
                theme="purple"
              />
              <StatCard
                label="Last Ingest"
                value={lastIngestion ? timeAgo(lastIngestion) : "Never"}
                icon={BookOpen}
                theme="amber"
              />
            </div>
            <StackSummary />
          </div>
        </div>

        {/* Row 3: Feed sections — 3 columns */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-8">
          <FeedSection
            title="AI Changes"
            icon={BookOpen}
            items={aiItems}
            viewAllHref="/feed?category=ai"
            theme="teal"
          />
          <FeedSection
            title="Trending"
            icon={BookOpen}
            items={trendingItems}
            viewAllHref="/feed?source=github_trending"
            theme="purple"
          />
          <FeedSection
            title="Frameworks"
            icon={BookOpen}
            items={frameworkItems}
            viewAllHref="/feed?category=nextjs"
            theme="blue"
          />
        </div>

        {/* Row 4: Security + Featured Prompt */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-8">
          <FeedSection
            title="Security"
            icon={BookOpen}
            items={securityItems}
            viewAllHref="/feed?category=security"
            theme="rose"
          />
          <FeaturedPrompt item={featuredPrompt} />
        </div>

        {/* Row 5: Feed Breakdown */}
        <div className="mb-8">
          <FeedBreakdown sources={sources} categories={categories} total={totalItems} />
        </div>

        {/* Row 6: Tasks + Ingest Health */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <InternTasks
            recommendedItem={recommendedItem}
            todayTask={todayTask}
            tomorrowTask={tomorrowTask}
          />
          <IngestHealth />
        </div>
      </div>
    </div>
  );
}
