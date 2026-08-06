import { test } from "bun:test";
import { CHECKS } from "../test/cross-runtime.ts";

for (const check of CHECKS) {
  test(check.name, check.run);
}
