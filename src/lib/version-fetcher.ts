const REGISTRIES: Record<string, (name: string) => Promise<string | null>> = {
  npm: async (name) => {
    const res = await fetch(`https://registry.npmjs.org/${encodeURIComponent(name)}/latest`);
    if (!res.ok) return null;
    const data = await res.json();
    return (data.version as string) ?? null;
  },

  PyPI: async (name) => {
    const res = await fetch(`https://pypi.org/pypi/${encodeURIComponent(name)}/json`);
    if (!res.ok) return null;
    const data = await res.json();
    return (data.info?.version as string) ?? null;
  },

  Go: async (name) => {
    const res = await fetch(`https://proxy.golang.org/${encodeURIComponent(name)}/@latest`);
    if (!res.ok) return null;
    const data = await res.json();
    return (data.Version as string) ?? null;
  },

  NuGet: async (name) => {
    const lower = name.toLowerCase();
    const res = await fetch(`https://api.nuget.org/v3-flatcontainer/${encodeURIComponent(lower)}/index.json`);
    if (!res.ok) return null;
    const data = await res.json();
    const versions: string[] = data.versions ?? [];
    const stable = versions.filter((v) => !/-(alpha|beta|rc|preview|dev)/i.test(v));
    return stable[stable.length - 1] ?? versions[versions.length - 1] ?? null;
  },

  "crates.io": async (name) => {
    const res = await fetch(`https://crates.io/api/v1/crates/${encodeURIComponent(name)}`, {
      headers: { "User-Agent": "my-ailounge/1.0" },
    });
    if (!res.ok) return null;
    const data = await res.json();
    return (data.crate?.max_version as string) ?? null;
  },

  RubyGems: async (name) => {
    const res = await fetch(`https://rubygems.org/api/v1/gems/${encodeURIComponent(name)}.json`);
    if (!res.ok) return null;
    const data = await res.json();
    return (data.version as string) ?? null;
  },
};

import { toRegistryName } from "./package-name-map";

export async function fetchLatestVersion(name: string, ecosystem: string): Promise<string | null> {
  const fetcher = REGISTRIES[ecosystem];
  if (!fetcher) return null;
  try {
    return await fetcher(toRegistryName(name));
  } catch {
    return null;
  }
}
