import { describe, expect, it } from "vitest";
import { AhoCorasick } from "../src/aho-corasick.ts";
import {
  compileNeedles,
  createNeedleTransformer,
  createNeedleTransformStream,
  type NeedleStats,
  type NeedleTransformOptions,
} from "../src/needles.ts";
import { bytes, concat, decoder, hex, prng, splitAt } from "./helpers.ts";
import { substituteNeedles } from "./needle-reference.ts";

async function runNeedleParts(
  chunks: Uint8Array[],
  options: NeedleTransformOptions,
): Promise<Uint8Array[]> {
  const tx = createNeedleTransformStream(options);
  const writer = tx.writable.getWriter();
  const reader = tx.readable.getReader();
  const out: Uint8Array[] = [];
  const pump = (async () => {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      out.push(value);
    }
  })();
  try {
    for (const c of chunks) await writer.write(c);
    await writer.close();
  } catch (error) {
    pump.catch(() => {});
    throw error;
  }
  await pump;
  return out;
}

const run = async (chunks: Uint8Array[], options: NeedleTransformOptions): Promise<string> =>
  decoder.decode(concat(await runNeedleParts(chunks, options)));

const one = (input: string, options: NeedleTransformOptions): Promise<string> =>
  run([bytes(input)], options);

describe("needles", () => {
  it("substitutes a single literal", async () => {
    expect(await one("a __ID__ b", { needles: { __ID__: "42" } })).toBe("a 42 b");
  });

  it("substitutes several literals in one pass", async () => {
    const options = { needles: { __A__: "1", __B__: "2", __C__: "" } };
    expect(await one("[__A__][__B__][__C__]", options)).toBe("[1][2][]");
  });

  it("is leftmost-longest", async () => {
    const options = { needles: { ab: "SHORT", abc: "LONG" } };
    expect(await one("abc", options)).toBe("LONG");
    expect(await one("abd", options)).toBe("SHORTd");
  });

  it("prefers the earlier start over the longer match", async () => {
    // "ab" starts at 0, "bcd" at 1. Leftmost wins even though it is shorter.
    expect(await one("abcd", { needles: { ab: "X", bcd: "Y" } })).toBe("Xcd");
  });

  it("keeps a live longer prefix alive across an inner match", async () => {
    // "ab" completes at offset 1 while "aabc" is still live from 0. The
    // leftmost match must win, whole and across every chunk split.
    const options = { needles: { ab: "X", aabc: "Y" } };
    expect(await one("aabx", options)).toBe("aXx");
    for (let i = 0; i <= 4; i++) {
      expect(await run(splitAt(bytes("aabc"), [i]), options)).toBe("Y");
    }
    const inner = { needles: { bc: "X", abcd: "Y" } };
    for (let i = 0; i <= 5; i++) {
      expect(await run(splitAt(bytes("zabcd"), [i]), inner)).toBe("zY");
    }
  });

  it("passes unmatched input through byte for byte", async () => {
    const input = "__I__ __ID_ _ID__ ___ID___";
    expect(await one(input, { needles: { __ID__: "!" } })).toBe("__I__ __ID_ _ID__ _!_");
  });

  it("does not re-scan a replacement", async () => {
    // The value reintroduces the needle. One pass, no cascade.
    expect(await one("<X>", { needles: { X: "<X>" } })).toBe("<<X>>");
  });

  it("does not re-scan a rejected match", async () => {
    const options: NeedleTransformOptions = { needles: ["aa"], resolve: () => null };
    expect(await one("aaa", options)).toBe("aaa");
  });

  it("re-scans bytes held behind a decided match", async () => {
    // "ab" decides only at 'x'; the held "a" after it must start a new match.
    expect(await one("abax", { needles: { ab: "-", ax: "+" } })).toBe("-+");
  });

  it("holds an unterminated prefix to end of stream", async () => {
    expect(await one("tail __I", { needles: { __ID__: "!" } })).toBe("tail __I");
  });

  it("bridges a needle spread over chunks smaller than itself", async () => {
    const needle = "N".repeat(64);
    const input = bytes(`a${needle}b`);
    const chunks: Uint8Array[] = [];
    for (let i = 0; i < input.length; i += 3) chunks.push(input.subarray(i, i + 3));
    expect(await run(chunks, { needles: { [needle]: "!" } })).toBe("a!b");
  });

  it("keeps the first entry for a duplicate needle", async () => {
    const options: NeedleTransformOptions = {
      needles: ["dup", "dup"],
      resolve: (_needle, index) => bytes(`${index}`),
    };
    expect(await one("dup", options)).toBe("0");
  });

  it("accepts an array with a resolver", async () => {
    const options: NeedleTransformOptions = {
      needles: ["one", "two"],
      resolve: (needle, index) => bytes(`${index}:${needle.length}`),
    };
    expect(await one("one two", options)).toBe("0:3 1:3");
  });

  it("accepts byte needles and a Map", async () => {
    const options: NeedleTransformOptions = {
      needles: new Map<string | Uint8Array, string | Uint8Array>([
        [new Uint8Array([0xc2, 0xa7]), bytes("S")],
      ]),
    };
    expect(await one("a§b", options)).toBe("aSb");
  });

  it("reports counts once", async () => {
    let stats: NeedleStats | undefined;
    const options: NeedleTransformOptions = {
      needles: ["a", "b"],
      resolve: (needle) => (needle[0] === 0x61 ? bytes("AA") : null),
      onDone: (s) => {
        stats = s;
      },
    };
    await one("ab", options);
    expect(stats).toEqual({ substituted: 1, rejected: 1, bytesIn: 2, bytesOut: 3 });
  });

  it("rejects bad options", () => {
    expect(() => createNeedleTransformStream({ needles: [] })).toThrow(TypeError);
    expect(() => createNeedleTransformStream({ needles: [""] })).toThrow(TypeError);
    expect(() => createNeedleTransformStream({ needles: ["a"] })).toThrow(/resolve is required/);
    expect(() => createNeedleTransformStream({ needles: { a: "b" }, flushBytes: -1 })).toThrow(
      RangeError,
    );
  });

  it("refuses a needle set whose transition table would be too large", () => {
    // 300 needles of 150 distinct-ish bytes: enough states times byte classes
    // to blow past the cap, and it must throw rather than allocate.
    const wide: string[] = [];
    for (let n = 0; n < 300; n++) {
      let needle = "";
      for (let i = 0; i < 150; i++) needle += String.fromCharCode((n + i) % 256);
      wide.push(needle);
    }
    expect(() => compileNeedles(wide)).toThrow(/needle set too large/);
    // The ceiling is a policy, not a hard rule: raise it and the same set builds.
    expect(() => compileNeedles(wide, { maxTableBytes: 256 * 1024 * 1024 })).not.toThrow();
  });

  it("validates maxTableBytes", () => {
    expect(() => compileNeedles(["a"], { maxTableBytes: 0 })).toThrow(RangeError);
    expect(() => compileNeedles(["a"], { maxTableBytes: 1.5 })).toThrow(RangeError);
    // Too small for even a tiny set, so it reports rather than allocating.
    expect(() => compileNeedles(["abc"], { maxTableBytes: 1 })).toThrow(/needle set too large/);
  });

  it("enqueues one part per piece at flushBytes 0", async () => {
    const parts = await runNeedleParts([bytes("x__ID__y")], {
      needles: { __ID__: "!" },
      flushBytes: 0,
    });
    expect(parts.map((p) => decoder.decode(p))).toEqual(["x", "!", "y"]);
  });
});

/** The same statement the token transformer defends: for every input and every
 *  chunking of it, the output equals the non-streaming reference. */
describe("needles differential", () => {
  const SETS: Array<{ name: string; options: NeedleTransformOptions }> = [
    { name: "distinct", options: { needles: { __A__: "1", __B__: "22" } } },
    { name: "nested", options: { needles: { ab: "X", abc: "Y", abcd: "Z" } } },
    { name: "overlapping", options: { needles: { aba: "1", bab: "2" } } },
    { name: "shared-prefix", options: { needles: { aa: "-", ab: "+", a: "." } } },
    { name: "single-byte", options: { needles: { a: "LONGER" } } },
    { name: "empty-value", options: { needles: { ab: "", ba: "x" } } },
    {
      name: "rejecting",
      options: { needles: ["ab", "b"], resolve: (_n, i) => (i === 0 ? null : bytes("!")) },
    },
    { name: "multibyte", options: { needles: { "§§": "S", "§x": "T" } } },
    // A needle that is a prefix of a longer one through a NON-terminal node:
    // "aa" is not itself a needle, so a decided "a" can leave a fresh candidate
    // pending with nothing after it. Regression cover for the flush-time drop.
    { name: "prefix-via-non-terminal", options: { needles: { a: "1", aab: "2" } } },
    { name: "prefix-via-non-terminal-long", options: { needles: { ab: "X", abcde: "Y" } } },
    { name: "chained-prefix", options: { needles: { a: "1", aa: "2", aaab: "3" } } },
  ];

  const ALPHABET = bytes("aabbcx§".normalize());

  function makeInput(rnd: () => number): Uint8Array {
    const len = Math.floor(rnd() * 40);
    const out = new Uint8Array(len);
    for (let i = 0; i < len; i++) out[i] = ALPHABET[Math.floor(rnd() * ALPHABET.length)];
    return out;
  }

  const SEED = Number(process.env.FUZZ_SEED ?? 20260806);
  const ROUNDS = Number(process.env.FUZZ_ROUNDS ?? 400);

  for (const set of SETS) {
    it(`matches the reference for every chunking: ${set.name}`, async () => {
      const rnd = prng(SEED);
      for (let round = 0; round < ROUNDS; round++) {
        const input = makeInput(rnd);
        const expected = substituteNeedles(input, set.options);

        // Exhaustive single cuts for short inputs, random cuts otherwise.
        const chunkings: number[][] = [[]];
        if (input.length <= 12) {
          for (let c = 1; c < input.length; c++) chunkings.push([c]);
          for (let c = 1; c < input.length; c++) {
            for (let d = c + 1; d < input.length; d++) chunkings.push([c, d]);
          }
        } else {
          for (let t = 0; t < 6; t++) {
            const cuts = [...new Set([1, 2, 3].map(() => 1 + Math.floor(rnd() * input.length)))];
            chunkings.push(cuts.sort((a, b) => a - b));
          }
        }
        // Byte-at-a-time is the worst case for carried state.
        chunkings.push(Array.from({ length: input.length }, (_, k) => k).slice(1));

        for (const cuts of chunkings) {
          for (const flushBytes of [16384, 0]) {
            const actual = concat(
              await runNeedleParts(splitAt(input, cuts), { ...set.options, flushBytes }),
            );
            if (hex(actual) !== hex(expected)) {
              throw new Error(
                `set=${set.name} seed=${SEED} round=${round} cuts=[${cuts}] flushBytes=${flushBytes}\n` +
                  `input:    ${hex(input)}\nexpected: ${hex(expected)}\nactual:   ${hex(actual)}`,
              );
            }
          }
        }
      }
    });
  }
});

describe("compileNeedles", () => {
  it("substitutes identically to an inline set", async () => {
    const compiled = compileNeedles({ __A__: "1", __B__: "2" });
    expect(await one("[__A__][__B__]", { needles: compiled })).toBe("[1][2]");
  });

  it("is reusable across streams", async () => {
    const compiled = compileNeedles({ __A__: "1" });
    for (let i = 0; i < 3; i++) {
      expect(await one("x__A__y", { needles: compiled })).toBe("x1y");
    }
  });

  it("carries its table but lets a resolver override it", async () => {
    const compiled = compileNeedles({ __A__: "1" });
    expect(await one("__A__", { needles: compiled, resolve: () => bytes("Z") })).toBe("Z");
  });

  it("still requires a resolver when compiled from an array", async () => {
    const compiled = compileNeedles(["__A__"]);
    expect(() => createNeedleTransformStream({ needles: compiled })).toThrow(/resolve is required/);
    expect(await one("__A__", { needles: compiled, resolve: () => bytes("!") })).toBe("!");
  });

  it("validates at compile time, not per stream", () => {
    expect(() => compileNeedles([])).toThrow(TypeError);
    expect(() => compileNeedles([""])).toThrow(TypeError);
  });
});

describe("needle boundaries and table limits", () => {
  it("matches randomized sets after switching to the rolling index", () => {
    const rnd = prng(20260907);
    const word = (length: number) =>
      Uint8Array.from({ length }, () => [97, 98, 120, 0, 255][Math.floor(rnd() * 5)]);
    for (let round = 0; round < 40; round++) {
      const patterns = [
        bytes("a"),
        bytes(`${"a".repeat(300 + Math.floor(rnd() * 500))}b`),
        ...Array.from({ length: 8 }, () => word(1 + Math.floor(rnd() * 12))),
      ];
      const options = {
        needles: compileNeedles(patterns),
        resolve: (_: Uint8Array, index: number) =>
          index % 3 === 0 ? null : new Uint8Array(index % 3).fill(index),
      };
      const input = concat([
        bytes("a".repeat(2000)),
        word(2000),
        ...patterns,
        bytes("a".repeat(1000)),
      ]);
      const expected = substituteNeedles(input, options);
      for (const size of [1, 17, input.length]) {
        const parts: Uint8Array[] = [];
        const ctrl = {
          enqueue: (part: Uint8Array) => parts.push(part),
        } as unknown as TransformStreamDefaultController<Uint8Array>;
        const body = createNeedleTransformer(options);
        for (let at = 0; at < input.length; at += size)
          body.transform(input.subarray(at, at + size), ctrl);
        body.flush(ctrl);
        expect(hex(concat(parts))).toBe(hex(expected));
      }
    }
  });

  it("indexes long overlaps across chunks and ring wraps", () => {
    const long = `${"a".repeat(4096)}b`;
    const input = bytes(`${"a".repeat(150000)}b${"a".repeat(10000)}`);
    const expected = `${"X".repeat(150000 - 4096)}Y${"X".repeat(10000)}`;
    const needles = compileNeedles({ a: "X", [long]: "Y" });
    for (const size of [1, 7, 1024, 65536, input.length]) {
      const parts: Uint8Array[] = [];
      const ctrl = {
        enqueue: (part: Uint8Array) => parts.push(part),
      } as unknown as TransformStreamDefaultController<Uint8Array>;
      const body = createNeedleTransformer({ needles });
      for (let at = 0; at < input.length; at += size)
        body.transform(input.subarray(at, at + size), ctrl);
      body.flush(ctrl);
      expect(decoder.decode(concat(parts))).toBe(expected);
    }
  });

  it("keeps leftmost matches and duplicate precedence in the indexed path", () => {
    const needles = ["a", "ab", "b", `${"a".repeat(2048)}b`, "ab", `${"ba".repeat(1024)}c`];
    const options = {
      needles: compileNeedles(needles),
      resolve: (_: Uint8Array, index: number) => (index === 2 ? null : bytes(String(index))),
    };
    const input = bytes(`${"a".repeat(5000)}b${"ba".repeat(3000)}cx${"a".repeat(3000)}`);
    const expected = substituteNeedles(input, options);
    for (const size of [511, input.length]) {
      const parts: Uint8Array[] = [];
      const ctrl = {
        enqueue: (part: Uint8Array) => parts.push(part),
      } as unknown as TransformStreamDefaultController<Uint8Array>;
      const body = createNeedleTransformer(options);
      for (let at = 0; at < input.length; at += size)
        body.transform(input.subarray(at, at + size), ctrl);
      body.flush(ctrl);
      expect(hex(concat(parts))).toBe(hex(expected));
    }
  });

  it("emits a decided short needle without waiting for another chunk", () => {
    for (const prefix of ["", "x"]) {
      const parts: Uint8Array[] = [];
      const body = createNeedleTransformer({ needles: { ab: "X", abcdef: "Y", q: "Z" } });
      const ctrl = {
        enqueue: (part: Uint8Array) => parts.push(part),
      } as unknown as TransformStreamDefaultController<Uint8Array>;
      body.transform(bytes(`${prefix}q`), ctrl);
      expect(decoder.decode(concat(parts))).toBe(`${prefix}Z`);
      body.flush(ctrl);
      expect(decoder.decode(concat(parts))).toBe(`${prefix}Z`);
    }
  });

  it("handles wide alphabets and the 16-bit state boundary", async () => {
    const needle = new Uint8Array(65536);
    for (let i = 0; i < needle.length; i++) needle[i] = i % 256;
    const compiled = compileNeedles([needle], { maxTableBytes: 80 * 1024 * 1024 });
    expect(
      await run([needle.subarray(0, 65535), needle.subarray(65535)], {
        needles: compiled,
        resolve: () => bytes("X"),
      }),
    ).toBe("X");
  });

  it("matches random needle sets across chunk boundaries", async () => {
    const rnd = prng(20260906);
    const word = (max: number) => {
      let result = "";
      for (let i = 1 + Math.floor(rnd() * max); i > 0; i--) {
        result += "abc"[Math.floor(rnd() * 3)];
      }
      return result;
    };
    for (let round = 0; round < 300; round++) {
      const needles = Array.from({ length: 8 }, () => word(9));
      const options = {
        needles: compileNeedles(needles),
        resolve: (_: Uint8Array, index: number) => bytes(String(index)),
      };
      const input = bytes(needles.join("") + word(30));
      const expected = substituteNeedles(input, options);
      for (const size of [1, 3, 16, input.length]) {
        const chunks = [];
        for (let at = 0; at < input.length; at += size) chunks.push(input.subarray(at, at + size));
        expect(hex(concat(await runNeedleParts(chunks, options)))).toBe(hex(expected));
      }
    }
  });

  it("emits a longest-length needle without waiting for another chunk", () => {
    const body = createNeedleTransformer({ needles: { abc: "X", def: "Y" } });
    const parts: Uint8Array[] = [];
    const ctrl = {
      enqueue: (part: Uint8Array) => parts.push(part),
    } as unknown as TransformStreamDefaultController<Uint8Array>;
    body.transform(bytes("abc"), ctrl);
    expect(decoder.decode(concat(parts))).toBe("X");
    body.transform(bytes("def"), ctrl);
    body.flush(ctrl);
    expect(decoder.decode(concat(parts))).toBe("XY");
  });

  it("rejects an oversized trie before reading the rest of the set", () => {
    const needles = [bytes("abcdef"), bytes("later")];
    Object.defineProperty(needles, 1, {
      get() {
        throw new Error("read past budget");
      },
    });
    expect(() => new AhoCorasick(needles, 8)).toThrow(/needle set too large/);
  });

  it("applies the table budget exactly, including new byte classes", () => {
    // Root + a + b, three columns, two bytes per cell.
    expect(() => compileNeedles(["a", "b"], { maxTableBytes: 18 })).not.toThrow();
    expect(() => compileNeedles(["a", "b"], { maxTableBytes: 17 })).toThrow(RangeError);
  });
});
