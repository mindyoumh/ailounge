import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  turbopack: {
    root: process.cwd(),
  },

  experimental: {
    // Allow large Acuity/Zoho CSV uploads for Log Analysis.
    proxyClientMaxBodySize: "150mb",
  },
};

export default nextConfig;