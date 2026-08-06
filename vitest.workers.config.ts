import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

// workerd chunks and applies backpressure differently from Node, so the
// cross-runtime contract checks are re-run here.
export default defineConfig({
  plugins: [cloudflareTest({ miniflare: { compatibilityDate: "2026-01-01" } })],
  test: {
    include: ["runtime/workers.test.ts"],
  },
});
