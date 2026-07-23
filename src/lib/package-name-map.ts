const MAP: Record<string, string> = {
  "next.js": "next",
  "tailwind css": "tailwindcss",
  "node.js": "node",
  "react native": "react-native",
  "ruby on rails": "rails",
  "asp.net core": "aspnetcore",
  "entity framework": "entityframework",
  "beautiful soup": "beautifulsoup4",
  "scikit-learn": "scikit-learn",
  "shadcn/ui": "shadcn-ui",
  "radix ui": "@radix-ui/react-primitive",
  "tanstack query": "@tanstack/react-query",
  "tanstack router": "@tanstack/react-router",
  "vercel ai sdk": "ai",
  "spring boot": "spring-boot",
  "github actions": "@actions/core",
  "gitlab ci": "gitlab-ci",
  "apache kafka": "kafka",
  "newtonsoft.json": "newtonsoft.json",
  "postcss": "postcss",
  "prisma": "@prisma/client",
};

export function toRegistryName(displayName: string): string {
  const key = displayName.toLowerCase().trim();
  return MAP[key] ?? key;
}
