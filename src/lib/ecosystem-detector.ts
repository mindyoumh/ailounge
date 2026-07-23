const KNOWN: Record<string, string> = {
  django: "PyPI",
  flask: "PyPI",
  fastapi: "PyPI",
  pandas: "PyPI",
  numpy: "PyPI",
  pytorch: "PyPI",
  celery: "PyPI",
  gunicorn: "PyPI",
  uvicorn: "PyPI",
  sqlalchemy: "PyPI",
  requests: "PyPI",
  "python": "PyPI",
  cobra: "Go",
  viper: "Go",
  gorilla: "Go",
  serde: "crates.io",
  tokio: "crates.io",
  spring: "Maven",
  log4j: "Maven",
  hibernate: "Maven",
  "asp.net": "NuGet",
  entityframework: "NuGet",
  rails: "RubyGems",
  devise: "RubyGems",
  laravel: "Packagist",
  symfony: "Packagist",
};

export function detectEcosystem(name: string): string {
  const key = name.toLowerCase().trim();
  if (KNOWN[key]) return KNOWN[key];
  if (key.startsWith("@")) return "npm";
  return "npm";
}
