import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

// The bench, on the runtime the library exists for.
export default defineConfig({
  plugins: [cloudflareTest({ miniflare: { compatibilityDate: "2026-01-01" } })],
  test: {
    include: ["bench/workers.bench.ts"],
    testTimeout: 120_000,
  },
});
