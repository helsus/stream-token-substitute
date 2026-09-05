// The suite's centrepiece: the streaming scanners against the naive
// non-streaming references, plus the properties and the reference's own tests.

import { describe, expect, it } from "vitest";
import type { PayloadValidator, TokenResolver, TokenTransformOptions } from "../src/types.ts";
import {
  bytes,
  concat,
  decoder,
  deferResolver,
  hex,
  prng,
  runAsyncStream,
  runStream,
  splitAt,
} from "./helpers.ts";
import { substituteBytes } from "./reference-impl.ts";

/** Delimiter matrix. */
const DELIMS: Array<{ name: string; open: Uint8Array; close: Uint8Array }> = [
  { name: "nul", open: new Uint8Array([0]), close: new Uint8Array([0]) },
  { name: "sect", open: new Uint8Array([0xc2, 0xa7]), close: new Uint8Array([0xc2, 0xa7]) },
  { name: "mustache", open: bytes("{{"), close: bytes("}}") },
  { name: "overlapping", open: bytes("aab"), close: bytes("baa") },
  { name: "shared-bytes", open: bytes("ab"), close: bytes("ba") },
  { name: "close-is-open-prefix", open: bytes("aa"), close: bytes("a") },
];

const VALIDATORS: Array<{
  name: string;
  make: (open: Uint8Array, close: Uint8Array) => PayloadValidator | undefined;
}> = [
  { name: "none", make: () => undefined },
  { name: "accept-all", make: () => () => true },
  { name: "reject-open0", make: (open) => (_p, len) => len !== open[0] },
  { name: "reject-close0", make: (_o, close) => (_p, len) => len !== close[0] },
  { name: "reject-both", make: (open, close) => (_p, len) => len !== open[0] && len !== close[0] },
  { name: "max-4", make: () => (part) => part.length < 4 },
  {
    name: "no-repeat",
    make: () => (part, len) => part.length === 0 || part[part.length - 1] !== len,
  },
];

const RESOLVERS: Array<{
  name: string;
  make: (open: Uint8Array, close: Uint8Array) => TokenResolver;
}> = [
  { name: "null", make: () => () => null },
  { name: "empty", make: () => () => new Uint8Array(0) },
  { name: "identity", make: (open, close) => (part) => concat([open, part, close]) },
  {
    name: "keyed-short",
    make: () => (part) => (part.length === 0 ? null : new Uint8Array([part[0]])),
  },
  {
    name: "keyed-long",
    make: () => (part) => {
      const out = new Uint8Array(part.length * 2 + 3);
      out.fill(0x5a);
      for (let i = 0; i < part.length; i++) out[i] = part[i];
      return out;
    },
  },
];

/** Fragment pool biased toward the shapes that break chunk-boundary code. */
function makeInput(rnd: () => number, open: Uint8Array, close: Uint8Array): Uint8Array {
  const frags: Uint8Array[] = [];
  const len = 1 + Math.floor(rnd() * 14);
  const payloadAlphabet = bytes("abxyz0123");

  const randPayload = (max: number): Uint8Array => {
    const len = Math.floor(rnd() * max);
    const out = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      const resolverCase = rnd();
      if (resolverCase < 0.55) out[i] = payloadAlphabet[Math.floor(rnd() * payloadAlphabet.length)];
      else if (resolverCase < 0.75) out[i] = open[Math.floor(rnd() * open.length)];
      else if (resolverCase < 0.9) out[i] = close[Math.floor(rnd() * close.length)];
      else out[i] = Math.floor(rnd() * 256);
    }
    return out;
  };

  for (let f = 0; f < len; f++) {
    const kind = rnd();
    if (kind < 0.28) {
      frags.push(open, randPayload(8), close); // token
    } else if (kind < 0.36) {
      frags.push(open, close); // empty payload
    } else if (kind < 0.46) {
      frags.push(open, randPayload(90)); // unterminated / cap overflow
    } else if (kind < 0.56) {
      frags.push(open.subarray(0, 1 + Math.floor(rnd() * open.length))); // partial open
    } else if (kind < 0.64) {
      frags.push(close.subarray(0, 1 + Math.floor(rnd() * close.length))); // stray close prefix
    } else if (kind < 0.72) {
      // self-overlapping runs
      const rep = 1 + Math.floor(rnd() * 3);
      for (let i = 0; i < rep; i++) frags.push(open.subarray(0, 1));
      frags.push(open);
    } else if (kind < 0.8) {
      frags.push(bytes("héllo中文")); // multi-byte utf-8
    } else if (kind < 0.86) {
      frags.push(new Uint8Array([0xff, 0xfe, 0xc0, 0x80, 0xed, 0xa0])); // invalid utf-8
    } else {
      frags.push(randPayload(12));
    }
  }
  return concat(frags);
}

function randomCuts(rnd: () => number, len: number): number[] {
  const count = Math.floor(rnd() * 6);
  const cuts: number[] = [];
  for (let i = 0; i < count; i++) cuts.push(Math.floor(rnd() * (len + 1)));
  cuts.sort((x, y) => x - y);
  return cuts;
}

function chunkings(
  rnd: () => number,
  input: Uint8Array,
): Array<{ name: string; parts: Uint8Array[] }> {
  const byteByByte: Uint8Array[] = [];
  for (let i = 0; i < input.length; i++) byteByByte.push(input.subarray(i, i + 1));

  const withEmpties: Uint8Array[] = [];
  for (const part of splitAt(input, randomCuts(rnd, input.length))) {
    withEmpties.push(new Uint8Array(0), part);
  }
  withEmpties.push(new Uint8Array(0));

  return [
    { name: "single", parts: [input] },
    { name: "byte-by-byte", parts: byteByByte },
    { name: "random", parts: splitAt(input, randomCuts(rnd, input.length)) },
    { name: "random-2", parts: splitAt(input, randomCuts(rnd, input.length)) },
    { name: "empties", parts: withEmpties },
  ];
}

async function assertMatches(
  input: Uint8Array,
  parts: Uint8Array[],
  options: TokenTransformOptions,
  label: string,
): Promise<void> {
  const expected = substituteBytes(input, options);
  const disagree = (actual: Uint8Array, via: string): void => {
    if (hex(actual) === hex(expected)) return;
    throw new Error(
      `${label} via=${via}\n  input:    ${hex(input)}\n  chunks:   ${parts.map((part) => part.length).join(",")}\n  expected: ${hex(expected)}\n  actual:   ${hex(actual)}`,
    );
  };

  disagree(await runStream(parts, options), "sync");

  // The async transformer suspends the scanner at every token and resumes it
  // from carried state. Held to the same oracle: an awaited resolver must not
  // change one output byte.
  disagree(
    await runAsyncStream(parts, { ...options, resolve: deferResolver(options.resolve) }),
    "async",
  );
}

const SEED = Number(process.env.FUZZ_SEED ?? 0x5eed) >>> 0;
const ROUNDS = Number(process.env.FUZZ_ROUNDS ?? 120);

describe("differential fuzz", () => {
  for (const delim of DELIMS) {
    it(`matches the reference for ${delim.name} delimiters`, async () => {
      const rnd = prng(SEED);
      for (let round = 0; round < ROUNDS; round++) {
        const input = makeInput(rnd, delim.open, delim.close);
        const validatorCase = VALIDATORS[Math.floor(rnd() * VALIDATORS.length)];
        const resolverCase = RESOLVERS[Math.floor(rnd() * RESOLVERS.length)];
        const maxPayloadBytes = [0, 1, 4, 64, 1024][Math.floor(rnd() * 5)];
        const options: TokenTransformOptions = {
          open: delim.open,
          close: delim.close,
          resolve: resolverCase.make(delim.open, delim.close),
          maxPayloadBytes,
        };
        const validate = validatorCase.make(delim.open, delim.close);
        if (validate) options.validate = validate;

        for (const chunking of chunkings(rnd, input)) {
          await assertMatches(
            input,
            chunking.parts,
            options,
            `seed=${SEED} round=${round} delim=${delim.name} validate=${validatorCase.name} resolve=${resolverCase.name} max=${maxPayloadBytes} chunking=${chunking.name}`,
          );
        }
      }
    });
  }
});

describe("exhaustive splits for short inputs", () => {
  for (const delim of DELIMS) {
    it(`covers every 2- and 3-part split for ${delim.name}`, async () => {
      const rnd = prng(SEED ^ 0x9e3779b9);
      for (let round = 0; round < 6; round++) {
        let input = makeInput(rnd, delim.open, delim.close);
        if (input.length > 16) input = input.subarray(0, 16);
        const validatorCase = VALIDATORS[round % VALIDATORS.length];
        const resolverCase = RESOLVERS[round % RESOLVERS.length];
        const options: TokenTransformOptions = {
          open: delim.open,
          close: delim.close,
          resolve: resolverCase.make(delim.open, delim.close),
          maxPayloadBytes: 8,
        };
        const validate = validatorCase.make(delim.open, delim.close);
        if (validate) options.validate = validate;

        const len = input.length;
        const label = `seed=${SEED} round=${round} delim=${delim.name} validate=${validatorCase.name} resolve=${resolverCase.name}`;
        for (let i = 0; i <= len; i++) {
          await assertMatches(input, splitAt(input, [i]), options, `${label} cut=${i}`);
          for (let j = i; j <= len; j++) {
            await assertMatches(input, splitAt(input, [i, j]), options, `${label} cuts=${i},${j}`);
          }
        }
      }
    });
  }
});

describe("splits inside structural positions", () => {
  it("cuts inside open, payload and close candidacies", async () => {
    const open = bytes("{{");
    const close = bytes("}}");
    const input = bytes("head{{name}}mid{{a}b}}tail{{unterminated");
    const options: TokenTransformOptions = {
      open,
      close,
      resolve: (part) => (part.length === 4 ? bytes("VALUE") : null),
      maxPayloadBytes: 64,
    };
    for (let i = 0; i <= input.length; i++) {
      await assertMatches(input, splitAt(input, [i]), options, `cut=${i}`);
    }
  });
});

describe("resolver receives only committed bytes", () => {
  it("never sees close in a payload", async () => {
    const rnd = prng(SEED ^ 0x1234);
    const open = bytes("{{");
    const close = bytes("}}");
    for (let round = 0; round < 40; round++) {
      const input = makeInput(rnd, open, close);
      const seen: string[] = [];
      const options: TokenTransformOptions = {
        open,
        close,
        resolve: (part) => {
          seen.push(hex(part));
          return null;
        },
      };
      await runStream(splitAt(input, randomCuts(rnd, input.length)), options);
      for (const s of seen) expect(s).not.toContain("7d 7d");
    }
  });
});

const open = bytes("{{");
const close = bytes("}}");

const CORPUS = [
  "",
  "plain content",
  "{{a}}",
  "{{}}",
  "{{a}}{{b}}",
  "x{{a}}y",
  "{{{a}}}",
  "{{a{{b}}",
  "{{a}b}}",
  "{{unterminated",
  "{",
  "}}",
  "{{a}",
  "a{{b{{c}}d}}e",
  "héllo {{n}} 中文",
].map(bytes);

function allSplits(input: Uint8Array): Uint8Array[][] {
  const out: Uint8Array[][] = [[input]];
  for (let i = 0; i <= input.length; i++) out.push(splitAt(input, [i]));
  const single: Uint8Array[] = [];
  for (let i = 0; i < input.length; i++) single.push(input.subarray(i, i + 1));
  out.push(single);
  return out;
}

describe("properties", () => {
  it("passes input through losslessly when the resolver returns null", async () => {
    const options: TokenTransformOptions = { open, close, resolve: () => null };
    for (const input of CORPUS) {
      for (const parts of allSplits(input)) {
        expect(hex(await runStream(parts, options))).toBe(hex(input));
      }
    }
  });

  it("is the identity for left0 resolver echoing open+payload+close", async () => {
    const options: TokenTransformOptions = {
      open,
      close,
      resolve: (p) => concat([open, p, close]),
    };
    for (const input of CORPUS) {
      for (const parts of allSplits(input)) {
        expect(hex(await runStream(parts, options))).toBe(hex(input));
      }
    }
  });

  it("concatenates when no token straddles the seam", async () => {
    const options: TokenTransformOptions = { open, close, resolve: () => bytes("V") };
    const rnd = prng(7);
    // Seam-safe corpus: nothing that leaves an open candidacy or token dangling.
    const dangling = new Set(["{{unterminated", "{", "{{a}"]);
    const closed = CORPUS.filter((right0) => !dangling.has(decoder.decode(right0)));
    for (let i = 0; i < 30; i++) {
      const left0 = closed[Math.floor(rnd() * closed.length)];
      const right0 = closed[Math.floor(rnd() * closed.length)];
      const left = concat([left0, bytes("\n---\n")]);
      const right = concat([bytes("\n---\n"), right0]);
      const whole = concat([left, right]);
      const joined = concat([substituteBytes(left, options), substituteBytes(right, options)]);
      expect(hex(await runStream([whole], options))).toBe(hex(joined));
    }
  });

  it("output length is independent of chunking", async () => {
    const options: TokenTransformOptions = {
      open,
      close,
      resolve: (p) => (p.length ? bytes("X".repeat(p.length + 2)) : null),
    };
    for (const input of CORPUS) {
      const lens = new Set<number>();
      for (const parts of allSplits(input)) lens.add((await runStream(parts, options)).length);
      expect(lens.size).toBe(1);
    }
  });
});

function sub(input: string, options: Omit<TokenTransformOptions, "open"> & { open?: string }) {
  return decoder.decode(
    substituteBytes(bytes(input), { open: "{{", close: "}}", ...options } as TokenTransformOptions),
  );
}

const upper: TokenTransformOptions["resolve"] = (p) => bytes(decoder.decode(p).toUpperCase());
const nullAll = () => null;

describe("substituteBytes", () => {
  it("substitutes a token", () => {
    expect(sub("a{{x}}b", { resolve: upper })).toBe("aXb");
  });

  it("handles token at start, end and alone", () => {
    expect(sub("{{x}}b", { resolve: upper })).toBe("Xb");
    expect(sub("a{{x}}", { resolve: upper })).toBe("aX");
    expect(sub("{{x}}", { resolve: upper })).toBe("X");
  });

  it("handles empty input and empty payload", () => {
    expect(sub("", { resolve: upper })).toBe("");
    expect(sub("{{}}", { resolve: (p) => bytes(`[${p.length}]`) })).toBe("[0]");
  });

  it("drops a token resolving to empty bytes", () => {
    expect(sub("a{{x}}b", { resolve: () => new Uint8Array(0) })).toBe("ab");
  });

  it("passes everything through when the resolver returns null", () => {
    for (const s of ["", "plain", "{{x}}", "{{a{{b}}", "{{unterminated", "{{", "}}", "{{{x}}}"]) {
      expect(sub(s, { resolve: nullAll })).toBe(s);
    }
  });

  it("is the identity for an echoing resolver", () => {
    const echo = (p: Uint8Array) => bytes(`{{${decoder.decode(p)}}}`);
    expect(sub("a{{x}}b{{y}}", { resolve: echo })).toBe("a{{x}}b{{y}}");
  });

  it("is leftmost-greedy: payload never contains close", () => {
    expect(sub("{{a}}b}}", { resolve: upper })).toBe("Ab}}");
  });

  it("keeps a fallen-through close prefix in the payload", () => {
    expect(sub("{{a}b}}", { resolve: (p) => bytes(`<${decoder.decode(p)}>`) })).toBe("<a}b>");
  });

  it("validator rejecting close[0] does not block completion", () => {
    expect(sub("{{a}}", { resolve: upper, validate: (_p, n) => n !== 0x7d })).toBe("A");
  });

  it("validates a fallen-through close candidate late", () => {
    // The } falls through and is validated at that point, aborting the token.
    expect(sub("{{a}b}}", { resolve: upper, validate: (_p, n) => n !== 0x7d })).toBe("{{a}b}}");
  });

  it("re-scans aborted payload bytes for a new token", () => {
    const alpha = (_p: Uint8Array, n: number) => n >= 0x61 && n <= 0x7a;
    expect(sub("{{a{{b}}", { resolve: upper, validate: alpha })).toBe("{{aB");
  });

  it("null is atomic: no re-scan inside a rejected span", () => {
    const outerOnly = (p: Uint8Array) => (decoder.decode(p) === "b" ? bytes("B") : null);
    expect(sub("{{a{{b}}", { resolve: outerOnly })).toBe("{{a{{b}}");
  });

  it("aborts on maxPayloadBytes overflow", () => {
    expect(sub("{{abc}}", { resolve: upper, maxPayloadBytes: 2 })).toBe("{{abc}}");
    expect(sub("{{ab}}", { resolve: upper, maxPayloadBytes: 2 })).toBe("AB");
    expect(sub("{{}}", { resolve: () => bytes("!"), maxPayloadBytes: 0 })).toBe("!");
    expect(sub("{{a}}", { resolve: upper, maxPayloadBytes: 0 })).toBe("{{a}}");
  });

  it("emits unterminated tokens verbatim", () => {
    expect(sub("a{{x", { resolve: upper })).toBe("a{{x");
    expect(sub("a{{x}", { resolve: upper })).toBe("a{{x}");
    expect(sub("a{", { resolve: upper })).toBe("a{");
  });

  it("handles a rejected token adjacent to a valid one", () => {
    const only = (p: Uint8Array) => (decoder.decode(p) === "y" ? bytes("Y") : null);
    expect(sub("{{x}}{{y}}", { resolve: only })).toBe("{{x}}Y");
    expect(sub("{{y}}{{x}}", { resolve: only })).toBe("Y{{x}}");
  });

  it("handles symmetric delimiters", () => {
    const sect = new Uint8Array([0xc2, 0xa7]); // U+00A7
    const input = bytes("a\u00a7x\u00a7b");
    expect(decoder.decode(substituteBytes(input, { open: sect, resolve: upper }))).toBe("aXb");
  });

  it("handles self-overlapping delimiters", () => {
    const opts = { open: "aab", close: "baa", resolve: upper };
    expect(decoder.decode(substituteBytes(bytes("aaabxbaa"), opts))).toBe("aX");
  });

  it("respects byteOffset on input views", () => {
    const backing = bytes("XX{{x}}XX");
    const view = backing.subarray(2, 7);
    expect(decoder.decode(substituteBytes(view, { open: "{{", close: "}}", resolve: upper }))).toBe(
      "X",
    );
  });

  it("throws on bad options", () => {
    const ok = { open: "{{", resolve: upper };
    expect(() => substituteBytes(bytes(""), { ...ok, open: "" })).toThrow(TypeError);
    expect(() => substituteBytes(bytes(""), { ...ok, close: "" })).toThrow(TypeError);
    // biome-ignore lint/suspicious/noExplicitAny: testing runtime validation
    expect(() => substituteBytes(bytes(""), { open: "{{" } as any)).toThrow(TypeError);
    expect(() => substituteBytes(bytes(""), { ...ok, maxPayloadBytes: -1 })).toThrow(RangeError);
    expect(() => substituteBytes(bytes(""), { ...ok, maxPayloadBytes: 1.5 })).toThrow(RangeError);
  });

  it("propagates resolver exceptions", () => {
    expect(() =>
      substituteBytes(bytes("{{x}}"), {
        open: "{{",
        close: "}}",
        resolve: () => {
          throw new Error("boom");
        },
      }),
    ).toThrow("boom");
  });
});

describe("delimiter boundaries", () => {
  it("finds delimiters at every position after a run of first-byte near misses", async () => {
    for (const open of [bytes("{{"), bytes("ab"), bytes("abac"), new Uint8Array([255, 0])]) {
      for (let padding = 0; padding < 132; padding++) {
        const prefix = new Uint8Array(padding + 2);
        prefix.fill((open[1] + 1) & 255);
        prefix[0] = open[0];
        const input = concat([prefix, open, bytes("x!")]);
        const options = { open, close: "!", resolve: () => bytes("X") };
        const expected = substituteBytes(input, options);
        for (const cut of [0, input.length - 3, input.length - 2]) {
          const chunks = splitAt(input, [cut]);
          expect(await runStream(chunks, options)).toEqual(expected);
        }
      }
    }
  });

  it("preserves CSS bytes and substitutes tokens across small chunk boundaries", async () => {
    const input = bytes(
      `${".a{color:red}.b{margin:0}".repeat(12)}{{a}}${"x{y:z}".repeat(12)}{{b}}{`,
    );
    const options = { open: "{{", close: "}}", resolve: () => bytes("X") };
    const expected = substituteBytes(input, options);
    for (const size of [1, 2, 3, 7, 31, 63, 64, 65, input.length]) {
      const chunks: Uint8Array[] = [];
      for (let i = 0; i < input.length; i += size) chunks.push(input.subarray(i, i + size));
      expect(await runStream(chunks, options)).toEqual(expected);
      expect(await runAsyncStream(chunks, { ...options, resolve: async () => bytes("X") })).toEqual(
        expected,
      );
    }
  });

  it("preserves overlapping opening delimiters across every split", async () => {
    for (const open of ["aaaab", "ababac", "abcabd", "aaaa", "ababa"]) {
      const input = bytes(`!${open.repeat(3)}aaa${open}x!${open.slice(0, -1)}`);
      const options = { open, close: "!", maxPayloadBytes: 8, resolve: () => bytes("X") };
      const expected = substituteBytes(input, options);
      for (let cut = 0; cut <= input.length; cut++) {
        const chunks = splitAt(input, [cut]);
        expect(await runStream(chunks, options)).toEqual(expected);
        expect(
          await runAsyncStream(chunks, { ...options, resolve: async () => bytes("X") }),
        ).toEqual(expected);
      }
    }
  });

  it("keeps the same payload-cap boundary on the bulk search path", async () => {
    for (const length of [1023, 1024, 1025]) {
      const input = bytes(`{{${"x".repeat(length)}}}{{ok}}`);
      const options = { open: "{{", close: "}}", maxPayloadBytes: 1024, resolve: () => bytes("X") };
      const expected = substituteBytes(input, options);
      for (const cut of [0, 513, 1025, input.length]) {
        expect(await runStream(splitAt(input, [cut]), options)).toEqual(expected);
      }
    }
  });
});

describe("async resolver thenables", () => {
  it("recovers when reading a resolver's then property throws", async () => {
    const failure = new Error("then getter failed");
    // biome-ignore lint/suspicious/noThenProperty: exercises throwing thenable getters
    const thenable = Object.defineProperty({}, "then", {
      get() {
        throw failure;
      },
    }) as PromiseLike<Uint8Array>;
    const seen: string[] = [];
    const output = await runAsyncStream([bytes("{{a}}{{b}}")], {
      open: "{{",
      close: "}}",
      resolve: () => thenable,
      onResolveError: (error, payload) => {
        expect(error).toBe(failure);
        seen.push(decoder.decode(payload));
        return bytes("recovered");
      },
    });
    expect(decoder.decode(output)).toBe("recoveredrecovered");
    expect(seen).toEqual(["a", "b"]);
  });

  it("reads a custom then getter once and preserves its receiver", async () => {
    let reads = 0;
    // biome-ignore lint/suspicious/noThenProperty: exercises stateful thenable getters
    const thenable = Object.defineProperty({}, "then", {
      get() {
        if (++reads > 1) throw new Error("read twice");
        return function (this: unknown, resolve: (value: Uint8Array) => void) {
          expect(this).toBe(thenable);
          resolve(bytes("OK"));
        };
      },
    }) as PromiseLike<Uint8Array>;
    expect(
      decoder.decode(
        await runAsyncStream([bytes("{{a}}")], {
          open: "{{",
          close: "}}",
          resolve: () => thenable,
        }),
      ),
    ).toBe("OK");
    expect(reads).toBe(1);
  });
});
