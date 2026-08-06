import { CHECKS } from "../test/cross-runtime.ts";

for (const check of CHECKS) {
  Deno.test(check.name, check.run);
}
