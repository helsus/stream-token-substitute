import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

// workerd chunks and applies backpressure differently from Node, so the
// cross-runtime contract checks are re-run here.
// The differential fuzzers run here too. process.env does not reach the
// isolate: FUZZ_SEED and FUZZ_ROUNDS have no effect.
// workerd leaks an internal rejection whenever a native TransformStream transform
// fails, even with every public promise observed. These tests fail one on purpose.
const EXPECTED_TRANSFORM_FAILURES = new Set([
  "rejects anything else",
  "errors a native stream with the abort reason",
]);

export default defineConfig({
  plugins: [cloudflareTest({ miniflare: { compatibilityDate: "2026-01-01" } })],
  test: {
    onUnhandledError(error) {
      const test = (error as { VITEST_TEST_NAME?: string }).VITEST_TEST_NAME;
      if (test !== undefined && EXPECTED_TRANSFORM_FAILURES.has(test)) return false;
    },
    include: ["runtime/workers.test.ts", "test/differential.test.ts", "test/literals.test.ts"],
  },
});
