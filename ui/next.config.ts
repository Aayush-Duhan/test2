import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  "devIndicators": false,
  experimental: {
    turbopackUseSystemTlsCerts: true,
  },
};

export default nextConfig;
