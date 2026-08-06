import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

// workerd chunks and applies backpressure differently from Node, so the
// cross-runtime contract checks are re-run here.
// The differential fuzzers run here too. process.env does not reach the
// isolate: FUZZ_SEED and FUZZ_ROUNDS have no effect.
export default defineConfig({
  plugins: [cloudflareTest({ miniflare: { compatibilityDate: "2026-01-01" } })],
  test: {
    include: ["runtime/workers.test.ts", "test/differential.test.ts", "test/needles.test.ts"],
  },
});
