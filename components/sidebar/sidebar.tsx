"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import Image from "next/image";
import { usePathname, useRouter } from "next/navigation";
import logoImage from "@/src/image/MindYou_LogoBlue.png";
import { cn } from "@/lib/utils";
import {
  LayoutDashboard,
  Rss,
  Layers,
  ScrollText,
  Radio,
  MessageSquare,
  BookOpen,
  Sun,
  Moon,
  LogOut,
  User,
  X,
} from "lucide-react";
import { useTheme } from "@/components/theme-provider";
import { useUser } from "@/components/auth-provider";

export const NAV_ITEMS = [
  { href: "/", label: "Briefing", icon: LayoutDashboard },
  { href: "/feed", label: "Feed", icon: Rss },
  { href: "/watchlist", label: "Stack", icon: Layers },
  { href: "/logs", label: "Logs", icon: ScrollText },
  { href: "/intern-tasks", label: "Tasks", icon: BookOpen },
  { href: "/repo-radar", label: "Radar", icon: Radio },
  { href: "/prompts", label: "Prompts", icon: MessageSquare },
];

function SidebarStats() {
  const [stats, setStats] = useState({
    totalItems: 0,
    lastIngest: null as string | null,
    itemsToday: 0,
    itemsThisWeek: 0,
  });

  useEffect(() => {
    fetch("/api/stats")
      .then((r) => r.json())
      .then((data) => setStats(data))
      .catch(() => {});
  }, []);

  return (
    <div className="px-4 py-4 border-t border-border">
      <p className="text-[10px] font-mono text-muted-foreground uppercase tracking-widest mb-3">
        Quick Stats
      </p>
      <div className="space-y-2">
        <StatRow label="Total Items" value={String(stats.totalItems || "—")} />
        <StatRow
          label="Last Ingest"
          value={stats.lastIngest ? timeAgo(stats.lastIngest) : "—"}
        />
        <StatRow label="Today" value={`${stats.itemsToday || "—"} items`} />
        <StatRow
          label="This Week"
          value={`${stats.itemsThisWeek || "—"} items`}
        />
      </div>
    </div>
  );
}

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

function StatRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-[11px] text-muted-foreground">{label}</span>
      <span className="text-[11px] font-mono text-foreground/80 font-medium">
        {value}
      </span>
    </div>
  );
}

function NavItem({
  href,
  label,
  icon: Icon,
  active,
  onClick,
}: {
  href: string;
  label: string;
  icon: React.ElementType;
  active: boolean;
  onClick?: () => void;
}) {
  return (
    <Link
      href={href}
      onClick={onClick}
      className={cn(
        "flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-all duration-150",
        active
          ? "bg-accent text-accent-foreground border border-border shadow-sm"
          : "text-muted-foreground hover:text-foreground hover:bg-accent/50",
      )}
    >
      <Icon className="h-4 w-4 shrink-0" />
      <span>{label}</span>
    </Link>
  );
}

export function Sidebar({ isOpen, onClose }: { isOpen: boolean; onClose: () => void }) {
  const pathname = usePathname();
  const router = useRouter();
  const { theme, toggle } = useTheme();
  const { user, role, loading, signOut } = useUser();

  async function handleLogout() {
    await signOut();
    onClose();
    router.push("/login");
  }

  function handleNavClick() {
    onClose();
  }

  return (
    <aside
      className={cn(
        "fixed top-0 left-0 z-40 w-60 h-screen bg-background border-r border-border flex flex-col transition-transform duration-300 ease-out-expo",
        "md:translate-x-0 md:shadow-none",
        isOpen ? "translate-x-0 shadow-2xl" : "-translate-x-full",
      )}
    >
      {/* Close button (mobile) */}
      <button
        onClick={onClose}
        className="absolute top-5 right-4 md:hidden flex items-center justify-center h-8 w-8 rounded-lg bg-accent text-muted-foreground hover:text-foreground hover:bg-accent/80 transition-colors"
        aria-label="Close menu"
      >
        <X className="h-4 w-4" />
      </button>

      {/* Logo */}
      <div className="px-6 py-5 border-b border-border">
        <div className="flex items-center gap-2">
          <Image src={logoImage} alt="Mind You" className="w-7 h-7" />
          <span className="text-foreground font-semibold text-sm tracking-wide font-display">
            my-ailounge
          </span>
        </div>
        <p className="text-[10px] text-muted-foreground mt-1 font-mono">
          Developer Intelligence
        </p>
      </div>

      {/* User info */}
      {user && !loading && (
        <div className="px-3 pt-4 pb-2 border-b border-border">
          <div className="flex items-center gap-2.5 px-3 py-2 rounded-lg bg-accent/30">
            <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-gradient-to-br from-teal-400 to-purple-600">
              {user.user_metadata?.avatar_url ? (
                <img
                  src={user.user_metadata.avatar_url}
                  alt=""
                  className="h-8 w-8 rounded-full"
                />
              ) : (
                <User className="h-4 w-4 text-white" />
              )}
            </div>
            <div className="min-w-0 flex-1">
              <p className="text-xs font-medium text-foreground truncate">
                {user.user_metadata?.full_name ||
                 user.user_metadata?.user_name ||
                 user.email?.split("@")[0] ||
                 "User"}
              </p>
              <p className="text-[10px] text-muted-foreground truncate">
                {user.email || ""}
              </p>
            </div>
            {role && (
              <span className={cn(
                "text-[10px] font-medium px-1.5 py-0.5 rounded-full shrink-0",
                role === "lead"
                  ? "bg-amber-500/10 text-amber-600 dark:text-amber-400"
                  : role === "dev"
                    ? "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400"
                    : "bg-blue-500/10 text-blue-600 dark:text-blue-400",
              )}>
                {role}
              </span>
            )}
          </div>
        </div>
      )}

      {/* Nav */}
      <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
        {NAV_ITEMS.map((item) => (
          <NavItem
            key={item.href}
            href={item.href}
            label={item.label}
            icon={item.icon}
            active={pathname === item.href}
            onClick={handleNavClick}
          />
        ))}
      </nav>

      {/* Theme Toggle */}
      <div className="px-3 mb-2">
        <button
          onClick={toggle}
          className="flex items-center gap-3 w-full px-3 py-2.5 rounded-lg text-sm text-muted-foreground hover:text-foreground hover:bg-accent/50 transition-all duration-150"
        >
          {theme === "dark" ? (
            <Sun className="h-4 w-4 shrink-0" />
          ) : (
            <Moon className="h-4 w-4 shrink-0" />
          )}
          <span>{theme === "dark" ? "Light Mode" : "Dark Mode"}</span>
        </button>
      </div>

      {/* Logout */}
      {user && !loading && (
        <div className="px-3 mb-2">
          <button
            onClick={handleLogout}
            className="flex items-center gap-3 w-full px-3 py-2.5 rounded-lg text-sm text-muted-foreground hover:text-red-500 hover:bg-red-500/10 transition-all duration-150"
          >
            <LogOut className="h-4 w-4 shrink-0" />
            <span>Log out</span>
          </button>
        </div>
      )}

      {/* Quick Stats */}
      <SidebarStats />
    </aside>
  );
}
