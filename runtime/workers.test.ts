// Run: vitest run -c vitest.workers.config.ts
import { expect, it } from "vitest";
import { CHECKS } from "../test/cross-runtime.ts";

it("really is running inside workerd", () => {
  expect(navigator.userAgent).toBe("Cloudflare-Workers");
});

for (const check of CHECKS) {
  it(check.name, check.run);
}
