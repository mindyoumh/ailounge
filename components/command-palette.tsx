"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import { useRouter } from "next/navigation";
import {
  Command,
  CommandInput,
  CommandList,
  CommandEmpty,
  CommandGroup,
  CommandItem,
  CommandSeparator,
} from "@/components/ui/command";
import { Dialog, DialogContent } from "@/components/ui/dialog";
import { NAV_ITEMS } from "@/components/sidebar/sidebar";

interface PaletteItem {
  id: string;
  label: string;
  subtitle?: string;
  url: string;
}

export function CommandPalette() {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [feedResults, setFeedResults] = useState<PaletteItem[]>([]);
  const [promptResults, setPromptResults] = useState<PaletteItem[]>([]);
  const [watchlistItems, setWatchlistItems] = useState<PaletteItem[]>([]);
  const [radarItems, setRadarItems] = useState<PaletteItem[]>([]);
  const router = useRouter();
  const debounceRef = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    const down = (e: KeyboardEvent) => {
      if (e.key === "k" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        setOpen((prev) => !prev);
      }
    };
    document.addEventListener("keydown", down);
    return () => document.removeEventListener("keydown", down);
  }, []);

  const fetchWatchlist = useCallback(async () => {
    try {
      const res = await fetch("/api/watchlist");
      const data = await res.json();
      setWatchlistItems(
        (data.items ?? []).map((i: any) => ({
          id: `wl-${i.id}`,
          label: i.name,
          subtitle: i.category,
          url: "/watchlist",
        })),
      );
    } catch {}
  }, []);

  const fetchRadar = useCallback(async () => {
    try {
      const res = await fetch("/api/repo-radar");
      const data = await res.json();
      setRadarItems(
        (data.items ?? []).map((i: any) => ({
          id: `radar-${i.id}`,
          label: i.full_name,
          subtitle: i.language ?? i.description?.slice(0, 60),
          url: i.url,
        })),
      );
    } catch {}
  }, []);

  useEffect(() => {
    if (open) {
      setQuery("");
      setFeedResults([]);
      setPromptResults([]);
      fetchWatchlist();
      fetchRadar();
    }
  }, [open, fetchWatchlist, fetchRadar]);

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);

    if (!query || query.length < 2) {
      setFeedResults([]);
      setPromptResults([]);
      return;
    }

    debounceRef.current = setTimeout(async () => {
      try {
        const [feedRes, promptRes] = await Promise.all([
          fetch(`/api/feed?q=${encodeURIComponent(query)}&limit=5`),
          fetch(`/api/prompts?search=${encodeURIComponent(query)}&limit=5`),
        ]);
        const feedData = await feedRes.json();
        const promptData = await promptRes.json();

        setFeedResults(
          (feedData.items ?? []).map((i: any) => ({
            id: `feed-${i.id}`,
            label: i.title,
            subtitle: i.source,
            url: i.url,
          })),
        );

        setPromptResults(
          (promptData.items ?? []).map((i: any) => ({
            id: `prompt-${i.id}`,
            label: i.title,
            subtitle: i.category,
            url: `/prompts`,
          })),
        );
      } catch {}
    }, 200);

    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [query]);

  const runCommand = useCallback(
    (url: string) => {
      setOpen(false);
      if (url.startsWith("http")) {
        window.open(url, "_blank", "noopener,noreferrer");
      } else {
        router.push(url);
      }
    },
    [router],
  );

  const hasLiveResults = feedResults.length > 0 || promptResults.length > 0;

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogContent className="overflow-hidden p-0 sm:max-w-[550px]">
        <Command>
          <CommandInput
            placeholder="Search pages, feed items, prompts..."
            value={query}
            onValueChange={setQuery}
          />
          <CommandList>
            <CommandEmpty>No results found.</CommandEmpty>

            <CommandGroup heading="Pages">
              {NAV_ITEMS.map((item) => {
                const Icon = item.icon;
                return (
                  <CommandItem
                    key={item.href}
                    value={item.label}
                    onSelect={() => runCommand(item.href)}
                  >
                    <Icon className="mr-2 h-4 w-4" />
                    {item.label}
                  </CommandItem>
                );
              })}
            </CommandGroup>

            {watchlistItems.length > 0 && (
              <CommandGroup heading="Stack Watchlist">
                {watchlistItems.map((item) => (
                  <CommandItem
                    key={item.id}
                    value={item.label}
                    onSelect={() => runCommand(item.url)}
                  >
                    <span className="text-xs text-muted-foreground mr-2">
                      stack
                    </span>
                    {item.label}
                    {item.subtitle && (
                      <span className="ml-auto text-xs text-muted-foreground">
                        {item.subtitle}
                      </span>
                    )}
                  </CommandItem>
                ))}
              </CommandGroup>
            )}

            {radarItems.length > 0 && (
              <CommandGroup heading="Repo Radar">
                {radarItems.map((item) => (
                  <CommandItem
                    key={item.id}
                    value={item.label}
                    onSelect={() => runCommand(item.url)}
                  >
                    <span className="text-xs text-muted-foreground mr-2">
                      repo
                    </span>
                    {item.label}
                    {item.subtitle && (
                      <span className="ml-auto text-xs text-muted-foreground truncate max-w-[180px]">
                        {item.subtitle}
                      </span>
                    )}
                  </CommandItem>
                ))}
              </CommandGroup>
            )}

            {hasLiveResults && <CommandSeparator />}

            {feedResults.length > 0 && (
              <CommandGroup heading="Feed">
                {feedResults.map((item) => (
                  <CommandItem
                    key={item.id}
                    value={item.label}
                    onSelect={() => runCommand(item.url)}
                  >
                    <span className="text-xs text-muted-foreground mr-2">
                      feed
                    </span>
                    {item.label}
                    {item.subtitle && (
                      <span className="ml-auto text-xs text-muted-foreground truncate max-w-[120px]">
                        {item.subtitle}
                      </span>
                    )}
                  </CommandItem>
                ))}
              </CommandGroup>
            )}

            {promptResults.length > 0 && (
              <CommandGroup heading="Prompts">
                {promptResults.map((item) => (
                  <CommandItem
                    key={item.id}
                    value={item.label}
                    onSelect={() => runCommand(item.url)}
                  >
                    <span className="text-xs text-muted-foreground mr-2">
                      prompt
                    </span>
                    {item.label}
                  </CommandItem>
                ))}
              </CommandGroup>
            )}

            {!query && (
              <div className="border-t border-border px-3 py-2 text-[10px] text-muted-foreground flex items-center gap-4">
                <span>
                  <kbd className="px-1 py-0.5 rounded bg-muted font-mono text-[10px]">
                    ↑↓
                  </kbd>{" "}
                  navigate
                </span>
                <span>
                  <kbd className="px-1 py-0.5 rounded bg-muted font-mono text-[10px]">
                    ↵
                  </kbd>{" "}
                  open
                </span>
                <span>
                  <kbd className="px-1 py-0.5 rounded bg-muted font-mono text-[10px]">
                    esc
                  </kbd>{" "}
                  close
                </span>
              </div>
            )}
          </CommandList>
        </Command>
      </DialogContent>
    </Dialog>
  );
}
