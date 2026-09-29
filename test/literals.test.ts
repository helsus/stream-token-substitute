import { describe, expect, it } from "vitest";
import { AhoCorasick } from "../src/aho-corasick.ts";
import {
  compileLiterals,
  createLiteralStream,
  createLiteralTransformer,
  DEFAULT_MAX_MEMORY_BYTES,
  type LiteralStats,
  LiteralSubstituter,
  type LiteralTransformOptions,
} from "../src/literals.ts";
import { bytes, concat, decoder, hex, nativeLiteralStream, prng, splitAt } from "./helpers.ts";
import { substituteLiterals } from "./literal-reference.ts";

async function runLiteralParts(
  chunks: Uint8Array[],
  options: LiteralTransformOptions,
): Promise<Uint8Array[]> {
  const tx = nativeLiteralStream(options);
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
  // workerd reports rejected closed promises as unhandled.
  pump.catch(() => {});
  writer.closed.catch(() => {});
  reader.closed.catch(() => {});
  writer.ready.catch(() => {});
  for (const c of chunks) await writer.write(c);
  await writer.close();
  await pump;
  return out;
}

const run = async (chunks: Uint8Array[], options: LiteralTransformOptions): Promise<string> =>
  decoder.decode(concat(await runLiteralParts(chunks, options)));

const one = (input: string, options: LiteralTransformOptions): Promise<string> =>
  run([bytes(input)], options);

describe("literals", () => {
  it("substitutes a single literal", async () => {
    expect(await one("a __ID__ b", { literals: { __ID__: "42" } })).toBe("a 42 b");
  });

  it("substitutes several literals in one pass", async () => {
    const options = { literals: { __A__: "1", __B__: "2", __C__: "" } };
    expect(await one("[__A__][__B__][__C__]", options)).toBe("[1][2][]");
  });

  it("is leftmost-longest", async () => {
    const options = { literals: { ab: "SHORT", abc: "LONG" } };
    expect(await one("abc", options)).toBe("LONG");
    expect(await one("abd", options)).toBe("SHORTd");
  });

  it("prefers the earlier start over the longer match", async () => {
    // "ab" starts at 0, "bcd" at 1. Leftmost wins even though it is shorter.
    expect(await one("abcd", { literals: { ab: "X", bcd: "Y" } })).toBe("Xcd");
  });

  it("keeps a live longer prefix alive across an inner match", async () => {
    // "ab" completes at offset 1 while "aabc" is still live from 0. The
    // leftmost match must win, whole and across every chunk split.
    const options = { literals: { ab: "X", aabc: "Y" } };
    expect(await one("aabx", options)).toBe("aXx");
    for (let i = 0; i <= 4; i++) {
      expect(await run(splitAt(bytes("aabc"), [i]), options)).toBe("Y");
    }
    const inner = { literals: { bc: "X", abcd: "Y" } };
    for (let i = 0; i <= 5; i++) {
      expect(await run(splitAt(bytes("zabcd"), [i]), inner)).toBe("zY");
    }
  });

  it("passes unmatched input through byte for byte", async () => {
    const input = "__I__ __ID_ _ID__ ___ID___";
    expect(await one(input, { literals: { __ID__: "!" } })).toBe("__I__ __ID_ _ID__ _!_");
  });

  it("does not re-scan a replacement", async () => {
    // The value reintroduces the literal. One pass, no cascade.
    expect(await one("<X>", { literals: { X: "<X>" } })).toBe("<<X>>");
  });

  it("does not re-scan a rejected match", async () => {
    const options: LiteralTransformOptions = { literals: ["aa"], resolve: () => null };
    expect(await one("aaa", options)).toBe("aaa");
  });

  it("re-scans bytes held behind a decided match", async () => {
    // "ab" decides only at 'x'; the held "a" after it must start a new match.
    expect(await one("abax", { literals: { ab: "-", ax: "+" } })).toBe("-+");
  });

  it("holds an unterminated prefix to end of stream", async () => {
    expect(await one("tail __I", { literals: { __ID__: "!" } })).toBe("tail __I");
  });

  it("bridges a literal spread over chunks smaller than itself", async () => {
    const literal = "N".repeat(64);
    const input = bytes(`a${literal}b`);
    const chunks: Uint8Array[] = [];
    for (let i = 0; i < input.length; i += 3) chunks.push(input.subarray(i, i + 3));
    expect(await run(chunks, { literals: { [literal]: "!" } })).toBe("a!b");
  });

  it("keeps the first entry for a duplicate literal", async () => {
    const options: LiteralTransformOptions = {
      literals: ["dup", "dup"],
      resolve: (_literal, index) => bytes(`${index}`),
    };
    expect(await one("dup", options)).toBe("0");
  });

  it("accepts an array with a resolver", async () => {
    const options: LiteralTransformOptions = {
      literals: ["one", "two"],
      resolve: (literal, index) => bytes(`${index}:${literal.length}`),
    };
    expect(await one("one two", options)).toBe("0:3 1:3");
  });

  it("accepts byte literals and a Map", async () => {
    const options: LiteralTransformOptions = {
      literals: new Map<string | Uint8Array, string | Uint8Array>([
        [new Uint8Array([0xc2, 0xa7]), bytes("S")],
      ]),
    };
    expect(await one("a\u00a7b", options)).toBe("aSb");
  });

  it("reports counts once", async () => {
    let stats: LiteralStats | undefined;
    const options: LiteralTransformOptions = {
      literals: ["a", "b"],
      resolve: (literal) => (literal[0] === 0x61 ? bytes("AA") : null),
      onDone: (s) => {
        stats = s;
      },
    };
    await one("ab", options);
    expect(stats).toEqual({ replaced: 1, rejected: 1, bytesIn: 2, bytesOut: 3 });
  });

  it("rejects bad options", () => {
    expect(() => nativeLiteralStream({ literals: [] })).toThrow(TypeError);
    expect(() => nativeLiteralStream({ literals: [""] })).toThrow(TypeError);
    expect(() => nativeLiteralStream({ literals: ["a"] })).toThrow(/resolve is required/);
    expect(() => nativeLiteralStream({ literals: { a: "b" }, mergeBytes: -1 })).toThrow(RangeError);
  });

  it("refuses a literal set that would need too much memory to compile", () => {
    // 300 literals of 150 distinct-ish bytes: enough states times byte classes
    // to blow past the cap, and it must throw rather than allocate.
    const wide: string[] = [];
    for (let n = 0; n < 300; n++) {
      let literal = "";
      for (let i = 0; i < 150; i++) literal += String.fromCharCode((n + i) % 256);
      wide.push(literal);
    }
    expect(() => compileLiterals(wide)).toThrow(/literal set too large/);
    // The ceiling is a policy, not a hard rule: raise it and the same set builds.
    expect(() => compileLiterals(wide, { maxMemoryBytes: 256 * 1024 * 1024 })).not.toThrow();
  });

  it("validates maxMemoryBytes", () => {
    for (const bad of [0, -1, 1.5, Number.NaN, 2 ** 53, "8" as unknown as number]) {
      expect(() => compileLiterals(["a"], { maxMemoryBytes: bad })).toThrow(RangeError);
      expect(() => nativeLiteralStream({ literals: { a: "b" }, maxMemoryBytes: bad })).toThrow(
        /maxMemoryBytes/,
      );
    }
    // Too small for even a tiny set, so it reports rather than allocating.
    expect(() => compileLiterals(["abc"], { maxMemoryBytes: 1 })).toThrow(/literal set too large/);
  });

  it("counts pattern and value bytes before building the trie", () => {
    const big = "x".repeat(1000);
    expect(() => compileLiterals([big], { maxMemoryBytes: 999 })).toThrow(
      /literals and values take 1000 bytes, over the 999 byte maxMemoryBytes limit/,
    );
    expect(() => compileLiterals({ a: big }, { maxMemoryBytes: 1000 })).toThrow(
      /literals and values take 1001 bytes/,
    );
  });

  it("enqueues one part per piece at mergeBytes 0", async () => {
    const parts = await runLiteralParts([bytes("x__ID__y")], {
      literals: { __ID__: "!" },
      mergeBytes: 0,
    });
    expect(parts.map((p) => decoder.decode(p))).toEqual(["x", "!", "y"]);
  });
});

/** The same statement the token transformer defends: for every input and every
 *  chunking of it, the output equals the non-streaming reference. */
describe("literals differential", () => {
  const SETS: Array<{ name: string; options: LiteralTransformOptions }> = [
    { name: "distinct", options: { literals: { __A__: "1", __B__: "22" } } },
    { name: "nested", options: { literals: { ab: "X", abc: "Y", abcd: "Z" } } },
    { name: "overlapping", options: { literals: { aba: "1", bab: "2" } } },
    { name: "shared-prefix", options: { literals: { aa: "-", ab: "+", a: "." } } },
    { name: "single-byte", options: { literals: { a: "LONGER" } } },
    { name: "empty-value", options: { literals: { ab: "", ba: "x" } } },
    {
      name: "rejecting",
      options: { literals: ["ab", "b"], resolve: (_n, i) => (i === 0 ? null : bytes("!")) },
    },
    { name: "multibyte", options: { literals: { "\u00a7\u00a7": "S", "\u00a7x": "T" } } },
    // A literal that is a prefix of a longer one through a NON-terminal node:
    // "aa" is not itself a literal, so a decided "a" can leave a fresh candidate
    // pending with nothing after it. Regression cover for the flush-time drop.
    { name: "prefix-via-non-terminal", options: { literals: { a: "1", aab: "2" } } },
    { name: "prefix-via-non-terminal-long", options: { literals: { ab: "X", abcde: "Y" } } },
    { name: "chained-prefix", options: { literals: { a: "1", aa: "2", aaab: "3" } } },
  ];

  const ALPHABET = bytes("aabbcx\u00a7".normalize());

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
        const expected = substituteLiterals(input, set.options);

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
          for (const mergeBytes of [16384, 0]) {
            const actual = concat(
              await runLiteralParts(splitAt(input, cuts), { ...set.options, mergeBytes }),
            );
            if (hex(actual) !== hex(expected)) {
              throw new Error(
                `set=${set.name} seed=${SEED} round=${round} cuts=[${cuts}] mergeBytes=${mergeBytes}\n` +
                  `input:    ${hex(input)}\nexpected: ${hex(expected)}\nactual:   ${hex(actual)}`,
              );
            }
          }
        }
      }
    });
  }
});

describe("compileLiterals", () => {
  it("substitutes identically to an inline set", async () => {
    const compiled = compileLiterals({ __A__: "1", __B__: "2" });
    expect(await one("[__A__][__B__]", { literals: compiled })).toBe("[1][2]");
  });

  it("is reusable across streams", async () => {
    const compiled = compileLiterals({ __A__: "1" });
    for (let i = 0; i < 3; i++) {
      expect(await one("x__A__y", { literals: compiled })).toBe("x1y");
    }
  });

  it("carries its table but lets a resolver override it", async () => {
    const compiled = compileLiterals({ __A__: "1" });
    expect(await one("__A__", { literals: compiled, resolve: () => bytes("Z") })).toBe("Z");
  });

  it("still requires a resolver when compiled from an array", async () => {
    const compiled = compileLiterals(["__A__"]);
    expect(() => nativeLiteralStream({ literals: compiled })).toThrow(/resolve is required/);
    expect(await one("__A__", { literals: compiled, resolve: () => bytes("!") })).toBe("!");
  });

  it("validates at compile time, not per stream", () => {
    expect(() => compileLiterals([])).toThrow(TypeError);
    expect(() => compileLiterals([""])).toThrow(TypeError);
  });
});

describe("literal boundaries and table limits", () => {
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
        literals: compileLiterals(patterns),
        resolve: (_: Uint8Array, index: number) =>
          index % 3 === 0 ? null : new Uint8Array(index % 3).fill(index),
      };
      const input = concat([
        bytes("a".repeat(2000)),
        word(2000),
        ...patterns,
        bytes("a".repeat(1000)),
      ]);
      const expected = substituteLiterals(input, options);
      for (const size of [1, 17, input.length]) {
        const parts: Uint8Array[] = [];
        const ctrl = {
          enqueue: (part: Uint8Array) => parts.push(part),
        } as unknown as TransformStreamDefaultController<Uint8Array>;
        const body = createLiteralTransformer(options);
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
    const literals = compileLiterals({ a: "X", [long]: "Y" });
    for (const size of [1, 7, 1024, 65536, input.length]) {
      const parts: Uint8Array[] = [];
      const ctrl = {
        enqueue: (part: Uint8Array) => parts.push(part),
      } as unknown as TransformStreamDefaultController<Uint8Array>;
      const body = createLiteralTransformer({ literals });
      for (let at = 0; at < input.length; at += size)
        body.transform(input.subarray(at, at + size), ctrl);
      body.flush(ctrl);
      expect(decoder.decode(concat(parts))).toBe(expected);
    }
  });

  it("keeps leftmost matches and duplicate precedence in the indexed path", () => {
    const literals = ["a", "ab", "b", `${"a".repeat(2048)}b`, "ab", `${"ba".repeat(1024)}c`];
    const options = {
      literals: compileLiterals(literals),
      resolve: (_: Uint8Array, index: number) => (index === 2 ? null : bytes(String(index))),
    };
    const input = bytes(`${"a".repeat(5000)}b${"ba".repeat(3000)}cx${"a".repeat(3000)}`);
    const expected = substituteLiterals(input, options);
    for (const size of [511, input.length]) {
      const parts: Uint8Array[] = [];
      const ctrl = {
        enqueue: (part: Uint8Array) => parts.push(part),
      } as unknown as TransformStreamDefaultController<Uint8Array>;
      const body = createLiteralTransformer(options);
      for (let at = 0; at < input.length; at += size)
        body.transform(input.subarray(at, at + size), ctrl);
      body.flush(ctrl);
      expect(hex(concat(parts))).toBe(hex(expected));
    }
  });

  it("emits a decided short literal without waiting for another chunk", () => {
    for (const prefix of ["", "x"]) {
      const parts: Uint8Array[] = [];
      const body = createLiteralTransformer({ literals: { ab: "X", abcdef: "Y", q: "Z" } });
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
    const literal = new Uint8Array(65536);
    for (let i = 0; i < literal.length; i++) literal[i] = i % 256;
    const compiled = compileLiterals([literal], { maxMemoryBytes: 80 * 1024 * 1024 });
    expect(
      await run([literal.subarray(0, 65535), literal.subarray(65535)], {
        literals: compiled,
        resolve: () => bytes("X"),
      }),
    ).toBe("X");
  });

  it("matches random literal sets across chunk boundaries", async () => {
    const rnd = prng(20260906);
    const word = (max: number) => {
      let result = "";
      for (let i = 1 + Math.floor(rnd() * max); i > 0; i--) {
        result += "abc"[Math.floor(rnd() * 3)];
      }
      return result;
    };
    for (let round = 0; round < 300; round++) {
      const literals = Array.from({ length: 8 }, () => word(9));
      const options = {
        literals: compileLiterals(literals),
        resolve: (_: Uint8Array, index: number) => bytes(String(index)),
      };
      const input = bytes(literals.join("") + word(30));
      const expected = substituteLiterals(input, options);
      for (const size of [1, 3, 16, input.length]) {
        const chunks = [];
        for (let at = 0; at < input.length; at += size) chunks.push(input.subarray(at, at + size));
        expect(hex(concat(await runLiteralParts(chunks, options)))).toBe(hex(expected));
      }
    }
  });

  it("emits a longest-length literal without waiting for another chunk", () => {
    const body = createLiteralTransformer({ literals: { abc: "X", def: "Y" } });
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
    const literals = [bytes("abcdef"), bytes("later")];
    Object.defineProperty(literals, 1, {
      get() {
        throw new Error("read past budget");
      },
    });
    expect(() => new AhoCorasick(literals, 8)).toThrow(/literal set too large/);
  });

  it("applies the memory budget exactly, including new byte classes", () => {
    // 2 pattern bytes, 80-cell trie (320), 3 states x 24 bytes of arrays (72),
    // and a 3 x 3 table of 2-byte cells (18).
    expect(() => compileLiterals(["a", "b"], { maxMemoryBytes: 412 })).not.toThrow();
    expect(() => compileLiterals(["a", "b"], { maxMemoryBytes: 411 })).toThrow(RangeError);
  });

  it("builds dictionary links listing every literal ending at a node", () => {
    const literals = ["a", "aa", "aaa", "ba", "xaa"].map((n) => bytes(n));
    const ac = new AhoCorasick(literals, DEFAULT_MAX_MEMORY_BYTES);
    const ending = (text: string) => {
      let node = 0;
      for (const b of bytes(text)) node = ac.delta[node * ac.width + ac.classOf[b]];
      const found: number[] = [];
      for (let s = ac.own[node] >= 0 ? node : ac.dict[node]; s !== 0; s = ac.dict[s]) {
        found.push(ac.own[s]);
      }
      return found;
    };
    expect(ending("aaa")).toEqual([2, 1, 0]);
    expect(ending("xaa")).toEqual([4, 1, 0]);
    expect(ending("ba")).toEqual([3, 0]);
    expect(ending("x")).toEqual([]);
  });
});

function runSync(
  options: LiteralTransformOptions,
  input: Uint8Array,
  cuts: number[],
): { output: Uint8Array; indexed: boolean } {
  const parts: Uint8Array[] = [];
  const ctrl = {
    enqueue: (part: Uint8Array) => parts.push(part),
  } as unknown as TransformStreamDefaultController<Uint8Array>;
  const s = new LiteralSubstituter(options, () => {});
  let indexed = false;
  for (const chunk of splitAt(input, cuts)) {
    s.transform(chunk, ctrl);
    indexed ||= (s as unknown as { index?: unknown }).index !== undefined;
  }
  s.flush(ctrl);
  return { output: concat(parts), indexed };
}

describe("indexed path with many overlapping literals", () => {
  const LONG = `${"a".repeat(300)}z`;
  const SETS: Array<{ name: string; literals: string[] }> = [
    { name: "a-runs", literals: Array.from({ length: 40 }, (_, k) => "a".repeat(k + 1)) },
    {
      name: "a-runs-then-b",
      literals: ["a", ...Array.from({ length: 40 }, (_, k) => `${"a".repeat(k)}b`)],
    },
    {
      name: "a-runs-then-ab",
      literals: Array.from(
        { length: 40 },
        (_, k) => `${"a".repeat(k + 1)}${"ab".slice(0, 1 + (k % 2))}`,
      ),
    },
    { name: "mixed", literals: ["ab", "aab", "b", "ba", "bab", "abab", "a", "aba", "baa"] },
  ];

  for (const set of SETS) {
    it(`matches the reference across chunkings: ${set.name}`, () => {
      const rnd = prng(20260929);
      for (let round = 0; round < 12; round++) {
        const options: LiteralTransformOptions = {
          literals: compileLiterals([...set.literals, LONG]),
          resolve: (_n, i) => (i % 5 === 4 ? null : String.fromCharCode(65 + (i % 26))),
        };
        const noise = Uint8Array.from({ length: 2000 }, () => bytes("aab")[Math.floor(rnd() * 3)]);
        const input = concat([bytes("a".repeat(2000)), noise, bytes(LONG), bytes("a".repeat(500))]);
        const expected = hex(substituteLiterals(input, options));
        const chunkings = [[], Array.from({ length: 30 }, () => Math.floor(rnd() * input.length))];
        const step = 1 + Math.floor(rnd() * 64);
        chunkings.push(Array.from({ length: Math.floor(input.length / step) }, (_, k) => k * step));
        for (const cuts of chunkings) {
          const sorted = [...new Set(cuts)].filter((c) => c > 0).sort((a, b) => a - b);
          const { output, indexed } = runSync(options, input, sorted);
          expect(indexed).toBe(true);
          if (hex(output) !== expected) {
            throw new Error(`set=${set.name} round=${round} cuts=[${sorted}]`);
          }
        }
      }
    });
  }
});

describe("resolver returns", () => {
  it("encodes a string return", async () => {
    const options: LiteralTransformOptions = {
      literals: ["a", "b"],
      resolve: (_n, i) => (i ? "\u00a7" : ""),
    };
    expect(await one("xaby", options)).toBe("x\u00a7y");
  });

  it("rejects anything else", async () => {
    for (const [value, kind] of [
      [undefined, "undefined"],
      [42, "number"],
      [{}, "object"],
    ] as const) {
      const options = {
        literals: ["a"],
        resolve: () => value,
      } as unknown as LiteralTransformOptions;
      await expect(one("a", options)).rejects.toThrow(
        `resolve must return Uint8Array, string, null, a stream, or a promise of one; got ${kind}`,
      );
    }
  });

  it("copies a returned literal view held across a chunk boundary", async () => {
    for (const mergeBytes of [16384, 0]) {
      const options: LiteralTransformOptions = {
        literals: ["abcd", "cdx"],
        resolve: (literal) => literal.subarray(1),
        mergeBytes,
      };
      const input = bytes("..abcd..abcd.cdx.abcdabcd");
      const expected = decoder.decode(substituteLiterals(input, options));
      expect(expected).toBe("..bcd..bcd.dx.bcdbcd");
      for (let c = 1; c < input.length; c++) {
        for (let d = c + 1; d < input.length; d++) {
          expect(await run(splitAt(input, [c, d]), options)).toBe(expected);
        }
      }
      expect(
        await run(
          splitAt(input, Array.from({ length: input.length }, (_, k) => k).slice(1)),
          options,
        ),
      ).toBe(expected);
    }
  });
});

describe("literal signal", () => {
  it("errors the stream pair with the abort reason", async () => {
    const controller = new AbortController();
    const pair = createLiteralStream({ literals: { a: "b" }, signal: controller.signal });
    const reason = new Error("stop");
    controller.abort(reason);
    await expect(pair.readable.getReader().read()).rejects.toBe(reason);
  });

  it("errors a native stream with the abort reason", async () => {
    const controller = new AbortController();
    const gone = new Error("gone");
    controller.abort(gone);
    const stream = nativeLiteralStream({ literals: { a: "b" }, signal: controller.signal });
    const reader = stream.readable.getReader();
    const writer = stream.writable.getWriter();
    // workerd reports rejected closed promises as unhandled.
    reader.closed.catch(() => {});
    writer.closed.catch(() => {});
    const read = reader.read();
    const write = writer.write(bytes("a"));
    await expect(write).rejects.toBe(gone);
    await expect(read).rejects.toBe(gone);
  });

  it("rejects a non-signal", () => {
    expect(() => createLiteralStream({ literals: { a: "b" }, signal: {} as AbortSignal })).toThrow(
      /AbortSignal/,
    );
  });
});
