// The token scanner's unit surface: placement, delimiters, options, and the
// KMP matcher underneath it.

import { describe, expect, it } from "vitest";
import {
  ADVANCED,
  buildFailureTable,
  COMPLETE,
  DelimiterMatcher,
  REJECTED,
} from "../src/matcher.ts";
import { createTokenTransformStream } from "../src/transformer.ts";
import type { TokenTransformOptions } from "../src/types.ts";
import { bytes, concat, decoder, hex, runStream, runStreamParts } from "./helpers.ts";
import { substituteBytes } from "./reference-impl.ts";

const open = bytes("{{");
const close = bytes("}}");
const upper = (payload: Uint8Array) => bytes(decoder.decode(payload).toUpperCase());

const opts = (override: Partial<TokenTransformOptions> = {}): TokenTransformOptions => ({
  open,
  close,
  resolve: upper,
  ...override,
});

const run = async (chunks: string[], override: Partial<TokenTransformOptions> = {}) =>
  decoder.decode(await runStream(chunks.map(bytes), opts(override)));

describe("token placement", () => {
  it("handles start, end, alone, empty input and empty chunks", async () => {
    expect(await run(["{{x}}tail"])).toBe("Xtail");
    expect(await run(["head{{x}}"])).toBe("headX");
    expect(await run(["{{x}}"])).toBe("X");
    expect(await run([])).toBe("");
    expect(await run([""])).toBe("");
    expect(await run(["", "{{x", "", "}}", ""])).toBe("X");
  });

  it("handles adjacent tokens split across every boundary", async () => {
    const input = "{{a}}{{b}}";
    for (let i = 0; i <= input.length; i++) {
      expect(await run([input.slice(0, i), input.slice(i)])).toBe("AB");
    }
  });

  it("finds a token in three delimiters in a row", async () => {
    // The first { is content; the last two form the real open.
    expect(await run(["{{{x}}"])).toBe("{X");
  });

  it("handles a rejected token adjacent to a valid one", async () => {
    const only = (payload: Uint8Array) => (decoder.decode(payload) === "y" ? bytes("Y") : null);
    expect(await run(["{{x}}{{y}}"], { resolve: only })).toBe("{{x}}Y");
    expect(await run(["{{y}}{{x}}"], { resolve: only })).toBe("Y{{x}}");
    for (let i = 0; i <= 10; i++) {
      const s = "{{x}}{{y}}";
      expect(await run([s.slice(0, i), s.slice(i)], { resolve: only })).toBe("{{x}}Y");
    }
  });
});

describe("resolver contract", () => {
  it("drops a token resolving to empty bytes", async () => {
    expect(await run(["a{{x}}b"], { resolve: () => new Uint8Array(0) })).toBe("ab");
  });

  it("emits a null-resolved token verbatim", async () => {
    expect(await run(["a{{x}}b"], { resolve: () => null })).toBe("a{{x}}b");
  });

  it("null is atomic: the rejected span is not re-scanned", async () => {
    const outerOnly = (payload: Uint8Array) =>
      decoder.decode(payload) === "b" ? bytes("B") : null;
    expect(await run(["{{a{{b}}"], { resolve: outerOnly })).toBe("{{a{{b}}");
  });

  it("does not alias the scratch across tokens", async () => {
    const parts = await runStreamParts([bytes("{{aaa}}{{bbb}}")], opts({ resolve: () => null }));
    // Payload re-emissions must be copies, valid after later tokens are scanned.
    expect(parts.map((payload) => decoder.decode(payload)).join("")).toBe("{{aaa}}{{bbb}}");
  });
});

describe("validator and cap", () => {
  it("does not let a validator rejecting close[0] block completion", async () => {
    expect(await run(["{{a}}"], { validate: (_p, n) => n !== 0x7d })).toBe("A");
  });

  it("validates a fallen-through close candidate late", async () => {
    expect(await run(["{{a}b}}"], { validate: (_p, n) => n !== 0x7d })).toBe("{{a}b}}");
  });

  it("abort re-scans the payload, unlike null", async () => {
    const alpha = (_p: Uint8Array, n: number) => n >= 0x61 && n <= 0x7a;
    expect(await run(["{{a{{b}}"], { validate: alpha })).toBe("{{aB");
  });

  it("aborts on maxPayloadBytes overflow", async () => {
    expect(await run(["{{abc}}"], { maxPayloadBytes: 2 })).toBe("{{abc}}");
    expect(await run(["{{ab}}"], { maxPayloadBytes: 2 })).toBe("AB");
    expect(await run(["{{}}"], { maxPayloadBytes: 0, resolve: () => bytes("!") })).toBe("!");
    expect(await run(["{{a}}"], { maxPayloadBytes: 0 })).toBe("{{a}}");
  });

  it("aborts on the same byte whether or not the fast path is used", async () => {
    const withValidator = await run(["{{abcd}}"], {
      maxPayloadBytes: 2,
      validate: () => true,
    });
    const without = await run(["{{abcd}}"], { maxPayloadBytes: 2 });
    expect(withValidator).toBe(without);
    expect(without).toBe("{{abcd}}");
  });

  it("sees every committed byte in the validator", async () => {
    const seen: string[] = [];
    await run(["{{a}b}}"], {
      validate: (payload, n) => {
        seen.push(`${decoder.decode(payload)}+${String.fromCharCode(n)}`);
        return true;
      },
    });
    expect(seen).toEqual(["+a", "a+}", "a}+b"]);
  });
});

describe("flush conditions", () => {
  it("emits a partial open match", async () => {
    expect(await run(["abc{"])).toBe("abc{");
  });

  it("emits an unterminated token", async () => {
    expect(await run(["abc{{xy"])).toBe("abc{{xy");
  });

  it("emits an unterminated token with a close candidacy in progress", async () => {
    expect(await run(["abc{{xy}"])).toBe("abc{{xy}");
  });

  it("emits partial state split across chunks", async () => {
    expect(await run(["abc{", ""])).toBe("abc{");
    expect(await run(["abc{{", "xy"])).toBe("abc{{xy");
    expect(await run(["abc{{xy", "}"])).toBe("abc{{xy}");
  });
});

describe("error contract", () => {
  it("throws synchronously on bad options", () => {
    expect(() => createTokenTransformStream({ open: "", resolve: upper })).toThrow(TypeError);
    expect(() => createTokenTransformStream({ open: "{{", close: "", resolve: upper })).toThrow(
      TypeError,
    );
    // biome-ignore lint/suspicious/noExplicitAny: testing runtime validation
    expect(() => createTokenTransformStream({ open: "{{" } as any)).toThrow(TypeError);
    expect(() =>
      createTokenTransformStream({ open: "{{", resolve: upper, maxPayloadBytes: -1 }),
    ).toThrow(RangeError);
    // biome-ignore lint/suspicious/noExplicitAny: testing runtime validation
    expect(() => createTokenTransformStream(null as any)).toThrow(TypeError);
    // biome-ignore lint/suspicious/noExplicitAny: testing runtime validation
    expect(() => createTokenTransformStream({ open: 42 as any, resolve: upper })).toThrow(
      TypeError,
    );
    expect(() =>
      // biome-ignore lint/suspicious/noExplicitAny: testing runtime validation
      createTokenTransformStream({ open: "{{", resolve: upper, validate: 1 as any }),
    ).toThrow(TypeError);
  });

  it("copies Uint8Array delimiters instead of aliasing them", async () => {
    // Buffer, because Buffer.prototype.slice returns a view: a plain Uint8Array
    // would pass even if the delimiter were only sliced. Compiled first, then
    // the caller's bytes are overwritten.
    const open = Buffer.from("{{");
    const close = Buffer.from("}}");
    const tx = createTokenTransformStream({ open, close, resolve: upper });
    open.fill(0);
    close.fill(0);
    const writer = tx.writable.getWriter();
    void writer.write(bytes("a{{x}}b")).then(() => writer.close());
    const parts: Uint8Array[] = [];
    const reader = tx.readable.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      parts.push(value);
    }
    expect(decoder.decode(concat(parts))).toBe("aXb");
  });

  it("errors the stream on a non-Uint8Array chunk", async () => {
    const tx = createTokenTransformStream(opts());
    const writer = tx.writable.getWriter();
    const reader = tx.readable.getReader();
    // The readable has no queue by default, so start the read before writing.
    const read = reader.read().catch((e: Error) => e);
    // biome-ignore lint/suspicious/noExplicitAny: testing runtime validation
    await expect(writer.write("nope" as any)).rejects.toThrow(TypeError);
    expect(await read).toBeInstanceOf(TypeError);
  });

  it("errors both sides when the resolver throws", async () => {
    const tx = createTokenTransformStream(
      opts({
        resolve: () => {
          throw new Error("boom");
        },
      }),
    );
    const writer = tx.writable.getWriter();
    const reader = tx.readable.getReader();
    const read = reader.read().catch((e: Error) => e.message);
    await expect(writer.write(bytes("{{x}}"))).rejects.toThrow("boom");
    expect(await read).toBe("boom");
  });

  it("errors both sides when the validator throws", async () => {
    const tx = createTokenTransformStream(
      opts({
        validate: () => {
          throw new Error("bang");
        },
      }),
    );
    const writer = tx.writable.getWriter();
    const reader = tx.readable.getReader();
    const read = reader.read().catch((e: Error) => e.message);
    await expect(writer.write(bytes("{{x}}"))).rejects.toThrow("bang");
    expect(await read).toBe("bang");
  });
});

describe("input views", () => {
  it("respects byteOffset and byteLength", async () => {
    const backing = bytes("ZZZZ{{x}}ZZZZ");
    const view = backing.subarray(4, 9);
    expect(decoder.decode(await runStream([view], opts()))).toBe("X");
  });

  it("accepts a view whose buffer continues past it", async () => {
    const backing = bytes("{{x}}{{y}}");
    expect(decoder.decode(await runStream([backing.subarray(0, 5)], opts()))).toBe("X");
  });
});

describe("zero-copy and maximal spans", () => {
  it("emits one span aliasing the input when a chunk completes no token", async () => {
    const chunk = bytes("a{b{c{d{{".replace("{{", "{ ")); // dense in open[0], no token
    const parts = await runStreamParts([chunk], opts());
    expect(parts.length).toBe(1);
    expect(parts[0]?.buffer).toBe(chunk.buffer);
    expect(decoder.decode(parts[0])).toBe(decoder.decode(chunk));
  });

  it("emits one span for a large token-free chunk", async () => {
    const chunk = bytes("x".repeat(4096));
    const parts = await runStreamParts([chunk], opts());
    expect(parts.length).toBe(1);
    expect(parts[0]?.buffer).toBe(chunk.buffer);
  });

  it("does not fragment on near-miss candidacies", async () => {
    // 50 candidacies that all die one byte in.
    const chunk = bytes("{x".repeat(50));
    const parts = await runStreamParts([chunk], opts());
    expect(parts.length).toBe(1);
    expect(parts[0]?.buffer).toBe(chunk.buffer);
  });

  it("enqueues the resolver value by reference", async () => {
    const value = bytes("VALUE");
    const parts = await runStreamParts([bytes("{{x}}")], opts({ resolve: () => value }));
    expect(parts.length).toBe(1);
    expect(parts[0]).toBe(value);
  });
});

describe("bounds", () => {
  it("handles a pathological validator within a time budget", async () => {
    // Every token aborts one byte before the cap and restarts inside its payload.
    const unit = `{{${"a".repeat(63)}`;
    const input = bytes(unit.repeat(Math.ceil((1 << 20) / unit.length)));
    const alpha = (_p: Uint8Array, n: number) => n >= 0x61 && n <= 0x7a;
    const start = performance.now();
    const out = await runStream([input], opts({ validate: alpha, maxPayloadBytes: 64 }));
    const ms = performance.now() - start;
    expect(out.length).toBe(input.length);
    expect(ms).toBeLessThan(10_000);
  });

  it("matches the reference over many chunks with a candidacy on every seam", async () => {
    const unit = "{{name}} filler {";
    const chunks: Uint8Array[] = [];
    for (let i = 0; i < 2000; i++) chunks.push(bytes(unit));
    const override = opts({ resolve: () => bytes("N") });
    const out = await runStream(chunks, override);
    expect(hex(out)).toBe(hex(substituteBytes(bytes(unit.repeat(2000)), override)));
  });
});

describe("delimiter shapes", () => {
  it("handles single-byte delimiters", async () => {
    const override: TokenTransformOptions = {
      open: new Uint8Array([0]),
      resolve: upper,
    };
    expect(decoder.decode(await runStream([bytes("a\0x\0b")], override))).toBe("aXb");
  });

  it("handles symmetric multi-byte delimiters", async () => {
    const override: TokenTransformOptions = { open: new Uint8Array([0xc2, 0xa7]), resolve: upper };
    expect(decoder.decode(await runStream([bytes("a§x§b")], override))).toBe("aXb");
  });

  it("handles self-overlapping delimiters across chunks", async () => {
    const override: TokenTransformOptions = {
      open: bytes("aab"),
      close: bytes("baa"),
      resolve: upper,
    };
    const input = "aaabxbaa";
    for (let i = 0; i <= input.length; i++) {
      expect(
        decoder.decode(
          await runStream([bytes(input.slice(0, i)), bytes(input.slice(i))], override),
        ),
      ).toBe("aX");
    }
  });

  it("handles a close that is a prefix of open", async () => {
    const override: TokenTransformOptions = {
      open: bytes("aa"),
      close: bytes("a"),
      resolve: upper,
    };
    expect(decoder.decode(await runStream([bytes("aaxa")], override))).toBe("X");
  });
});

describe("flushBytes", () => {
  const value = bytes("VALUE");
  const byRef = opts({ resolve: () => value });

  it("merges the pieces of a chunk into one part by default", async () => {
    const parts = await runStreamParts([bytes("head{{x}}tail")], opts());
    expect(parts.length).toBe(1);
    expect(decoder.decode(parts[0])).toBe("headXtail");
  });

  it("emits one part per piece when disabled, aliasing input and value", async () => {
    const chunk = bytes("head{{x}}tail");
    const parts = await runStreamParts([chunk], { ...byRef, flushBytes: 0 });
    expect(parts.map((p) => decoder.decode(p))).toEqual(["head", "VALUE", "tail"]);
    expect(parts[0]?.buffer).toBe(chunk.buffer);
    expect(parts[1]).toBe(value);
    expect(parts[2]?.buffer).toBe(chunk.buffer);
  });

  it("still forwards a lone piece by reference when buffering is on", async () => {
    const parts = await runStreamParts([bytes("{{x}}")], byRef);
    expect(parts.length).toBe(1);
    expect(parts[0]).toBe(value);
  });

  it("splits output once the accumulator reaches the high-water mark", async () => {
    const chunks = [bytes("{{x}}".repeat(400))];
    const wide = await runStreamParts(chunks, { ...byRef, flushBytes: 256 });
    const narrow = await runStreamParts(chunks, { ...byRef, flushBytes: 64 });
    expect(narrow.length).toBeGreaterThan(wide.length);
    for (const part of wide.slice(0, -1)) expect(part.length).toBeGreaterThanOrEqual(256);
  });

  it("produces identical bytes at every setting", async () => {
    const input = bytes("a{{x}}b{{yy}}c{{}}d{{zzz}}e");
    const expected = substituteBytes(input, opts());
    for (const flushBytes of [0, 1, 7, 64, 16384, Number.MAX_SAFE_INTEGER]) {
      const out = await runStream([input], opts({ flushBytes }));
      expect(hex(out), `flushBytes ${flushBytes}`).toBe(hex(expected));
    }
  });

  it("does not carry buffered spans across a chunk boundary", async () => {
    // A span buffered from chunk 1 must be copied out before chunk 1 is dropped.
    const parts = await runStreamParts([bytes("head{{x}}"), bytes("tail")], byRef);
    expect(decoder.decode(concat(parts))).toBe("headVALUEtail");
  });

  it("rejects invalid values", () => {
    for (const bad of [-1, 1.5, Number.NaN, "16" as unknown as number]) {
      expect(() => createTokenTransformStream(opts({ flushBytes: bad }))).toThrow(RangeError);
    }
  });
});

/** Feed a string, return the indices where the delimiter completed. */
function completions(pattern: string, text: string): number[] {
  const matcher = new DelimiterMatcher(bytes(pattern));
  const input = bytes(text);
  const found: number[] = [];
  for (let i = 0; i < input.length; i++) {
    if (matcher.feed(input[i]) === COMPLETE) found.push(i);
  }
  return found;
}

/** Brute-force oracle: leftmost, non-overlapping matches (the matcher resets on
 *  completion, so a completed delimiter is spent). */
function expected(pattern: string, input: string): number[] {
  const found: number[] = [];
  let i = 0;
  while (i + pattern.length <= input.length) {
    if (input.slice(i, i + pattern.length) === pattern) {
      found.push(i + pattern.length - 1);
      i += pattern.length;
    } else {
      i++;
    }
  }
  return found;
}

describe("buildFailureTable", () => {
  it("handles self-overlapping patterns", () => {
    expect([...buildFailureTable(bytes("aab"))]).toEqual([0, 1, 0]);
    expect([...buildFailureTable(bytes("aaa"))]).toEqual([0, 1, 2]);
    expect([...buildFailureTable(bytes("abcab"))]).toEqual([0, 0, 0, 1, 2]);
    expect([...buildFailureTable(bytes("{{"))]).toEqual([0, 1]);
  });
});

describe("DelimiterMatcher", () => {
  const cases: Array<[string, string]> = [
    ["aab", "aaab"],
    ["aab", "aabaab"],
    ["aaa", "aaaaa"],
    ["{{", "{{{"],
    ["{{", "a{b{{c{{"],
    ["}}", "a}b}}"],
    ["abcab", "abcabcabcab"],
    ["x", "xxaxx"],
  ];

  for (const [pattern, input] of cases) {
    it(`finds every ${pattern} in ${input}`, () => {
      expect(completions(pattern, input)).toEqual(expected(pattern, input));
    });
  }

  it("does not miss a match after a self-overlap fallback", () => {
    // Naive reset-to-zero misses the aab at index 1.
    expect(completions("aab", "aaab")).toEqual([3]);
  });

  it("reports released bytes on fallback and rejection", () => {
    const matcher = new DelimiterMatcher(bytes("aab"));
    expect(matcher.feed(0x61)).toBe(ADVANCED); // a, k=1
    expect(matcher.released).toBe(0);
    expect(matcher.feed(0x61)).toBe(ADVANCED); // a, k=2
    expect(matcher.released).toBe(0);
    expect(matcher.feed(0x61)).toBe(ADVANCED); // a, fallback k=2
    expect(matcher.released).toBe(1);
    expect(matcher.feed(0x63)).toBe(REJECTED); // c
    expect(matcher.released).toBe(2);
    expect(matcher.k).toBe(0);
  });

  it("single-byte delimiters never hold state", () => {
    const matcher = new DelimiterMatcher(bytes("\0"));
    expect(matcher.feed(0)).toBe(COMPLETE);
    expect(matcher.k).toBe(0);
    expect(matcher.feed(1)).toBe(REJECTED);
    expect(matcher.released).toBe(0);
  });

  it("rejects an empty delimiter", () => {
    expect(() => new DelimiterMatcher(new Uint8Array(0))).toThrow(TypeError);
  });
});
