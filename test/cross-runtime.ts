// Runtime-agnostic contract checks. Web Streams chunking and backpressure differ
// per runtime, so this body is re-run under each one via a thin adapter.
// No test framework, no assertion library: checks throw on mismatch.
import { createNeedleTransformStream, type NeedleTransformOptions } from "../src/needles.ts";
import type { TokenTransformOptions } from "../src/types.ts";
import { bytes, concat, decoder, hex, runStream } from "./helpers.ts";
import { substituteNeedles } from "./needle-reference.ts";
import { substituteBytes } from "./reference-impl.ts";

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
  "{{a}",
  "a{{b{{c}}d}}e",
  "héllo {{n}} 中文",
].map(bytes);

const options: TokenTransformOptions = {
  open: "{{",
  close: "}}",
  resolve: (payload) => (payload.length === 1 ? bytes(`<${decoder.decode(payload)}>`) : null),
  maxPayloadBytes: 8,
};

const NEEDLE_CORPUS = [
  "",
  "plain content",
  "__A__",
  "__A____B__",
  "x__A__y",
  "__A_",
  "_A__",
  "ab",
  "abc",
  "abcd",
  "héllo __A__ 中文",
].map(bytes);

// Nested and overlapping needles, so leftmost-longest is exercised too.
const needleOptions: NeedleTransformOptions = {
  needles: { __A__: "1", __B__: "22", ab: "X", abc: "YY" },
};

/** The needle transformer, driven the way runStream drives the token one. */
async function runNeedleStream(
  chunks: Uint8Array[],
  options: NeedleTransformOptions,
): Promise<Uint8Array> {
  const tx = createNeedleTransformStream(options);
  const writer = tx.writable.getWriter();
  const reader = tx.readable.getReader();
  const parts: Uint8Array[] = [];
  const pump = (async () => {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      parts.push(value);
    }
  })();
  for (const chunk of chunks) await writer.write(chunk);
  await writer.close();
  await pump;
  return concat(parts);
}

function assertSame(actual: Uint8Array, expected: Uint8Array, label: string): void {
  if (hex(actual) !== hex(expected)) {
    throw new Error(`${label}\n  expected: ${hex(expected)}\n  actual:   ${hex(actual)}`);
  }
}

export const CHECKS: Array<{ name: string; run: () => Promise<void> }> = [
  {
    name: "streaming matches the reference for every 2-part split",
    run: async () => {
      for (const input of CORPUS) {
        const expected = substituteBytes(input, options);
        for (let cut = 0; cut <= input.length; cut++) {
          const actual = await runStream([input.subarray(0, cut), input.subarray(cut)], options);
          assertSame(actual, expected, `input=${decoder.decode(input)} cut=${cut}`);
        }
      }
    },
  },
  {
    name: "streaming matches the reference byte by byte",
    run: async () => {
      for (const input of CORPUS) {
        const parts: Uint8Array[] = [];
        for (let i = 0; i < input.length; i++) parts.push(input.subarray(i, i + 1));
        assertSame(
          await runStream(parts, options),
          substituteBytes(input, options),
          `input=${decoder.decode(input)} byte-by-byte`,
        );
      }
    },
  },
  {
    name: "needles match the reference for every 2-part split",
    run: async () => {
      for (const input of NEEDLE_CORPUS) {
        const expected = substituteNeedles(input, needleOptions);
        for (let cut = 0; cut <= input.length; cut++) {
          assertSame(
            await runNeedleStream([input.subarray(0, cut), input.subarray(cut)], needleOptions),
            expected,
            `needles input=${decoder.decode(input)} cut=${cut}`,
          );
        }
      }
    },
  },
  {
    name: "needles match the reference byte at a time",
    run: async () => {
      for (const input of NEEDLE_CORPUS) {
        const parts: Uint8Array[] = [];
        for (let i = 0; i < input.length; i++) parts.push(input.subarray(i, i + 1));
        assertSame(
          await runNeedleStream(parts, needleOptions),
          substituteNeedles(input, needleOptions),
          `needles input=${decoder.decode(input)} per byte`,
        );
      }
    },
  },
  {
    name: "empty chunks are inert",
    run: async () => {
      for (const input of CORPUS) {
        const parts: Uint8Array[] = [];
        for (let i = 0; i < input.length; i++) {
          parts.push(new Uint8Array(0), input.subarray(i, i + 1));
        }
        parts.push(new Uint8Array(0));
        assertSame(
          await runStream(parts, options),
          substituteBytes(input, options),
          `input=${decoder.decode(input)} empties`,
        );
      }
    },
  },
];
