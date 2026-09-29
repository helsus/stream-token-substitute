import { describe, expect, it } from "vitest";
import { compileLiterals, createLiteralTransformer } from "../src/literals.ts";
import { decoder } from "./helpers.ts";

describe("indexed path cost", () => {
  it("scans 1 MiB of adversarial overlap against 200 literals quickly", () => {
    // Every byte extends a live prefix of the 199 long literals, and "a" forces
    // replays, so the scanner switches to the rolling index early.
    const literals: Record<string, string> = { a: "X" };
    for (let k = 1; k < 200; k++) literals[`${"a".repeat(k)}b`] = "Y";
    const compiled = compileLiterals(literals);
    const n = 1024 * 1024;
    const input = new Uint8Array(n + 1).fill(0x61);
    input[n] = 0x62;

    const parts: Uint8Array[] = [];
    const ctrl = {
      enqueue: (part: Uint8Array) => parts.push(part),
    } as unknown as TransformStreamDefaultController<Uint8Array>;
    const started = performance.now();
    const body = createLiteralTransformer({ literals: compiled });
    for (let at = 0; at < input.length; at += 65536)
      body.transform(input.subarray(at, at + 65536), ctrl);
    body.flush(ctrl);
    const elapsed = performance.now() - started;

    let out = "";
    for (const part of parts) out += decoder.decode(part);
    expect(out.length).toBe(n - 198);
    expect(out.slice(0, -1)).toBe("X".repeat(n - 199));
    expect(elapsed).toBeLessThan(750);
  });
});
