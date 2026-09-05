// Runtime-agnostic contract checks. Web Streams chunking and backpressure differ
// per runtime, so this body is re-run under each one via a thin adapter.
// No test framework, no assertion library: checks throw on mismatch.
import {
  createAsyncTokenStreamPair,
  createAsyncTokenTransformer,
  createAsyncTokenTransformStream,
} from "../src/async-transformer.ts";
import {
  createNeedleStreamPair,
  createNeedleTransformStream,
  type NeedleTransformOptions,
} from "../src/needles.ts";
import { createTokenStreamPair, createTokenTransformStream } from "../src/transformer.ts";
import type { TokenTransformOptions } from "../src/types.ts";
import { bytes, concat, decoder, deferred, hex, runAsyncStream, runStream } from "./helpers.ts";
import { substituteNeedles } from "./needle-reference.ts";
import { substituteBytes } from "./reference-impl.ts";

const CORPUS = [
  "",
  "plain content",
  `${".a{color:red}.b{margin:0}".repeat(4)}{{a}}{`,
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
    name: "preserves native factories alongside opt-in stream pairs",
    async run() {
      const options = { open: "{{", close: "}}", resolve: () => bytes("value") };
      const native = [
        createTokenTransformStream(options),
        createAsyncTokenTransformStream(options),
        createNeedleTransformStream({ needles: { "{{x}}": "value" } }),
      ];
      const pairs = [
        createTokenStreamPair(options),
        createAsyncTokenStreamPair(options),
        createNeedleStreamPair({ needles: { "{{x}}": "value" } }),
      ];
      for (const stream of native) {
        if (!(stream instanceof TransformStream)) throw new Error("lost native identity");
      }
      for (const stream of pairs) {
        if (stream instanceof TransformStream) throw new Error("expected a stream pair");
      }
      for (const stream of [...native, ...pairs]) {
        const input = new Response("head{{x}}tail").body;
        if (!input) throw new Error("missing response body");
        const actual = await new Response(input.pipeThrough(stream)).text();
        if (actual !== "headvaluetail") throw new Error("incorrect substitution");
      }
    },
  },
  {
    name: "abort signal releases a backpressured async stream",
    async run() {
      const abort = new AbortController();
      const stream = createAsyncTokenStreamPair({
        open: "{{",
        close: "}}",
        signal: abort.signal,
        resolve: async () => new Uint8Array(65536),
      });
      const reader = stream.readable.getReader();
      const writer = stream.writable.getWriter();
      const first = reader.read();
      const written = writer.write(bytes("{{x}}".repeat(100))).catch((error) => error);
      await first;
      abort.abort("stop");
      if ((await written) !== "stop") throw new Error("lost abort reason");
      await reader.read().catch(() => {});
      reader.releaseLock();
      writer.releaseLock();
    },
  },
  {
    name: "pauses expansion within an input chunk and final flush",
    async run() {
      for (const mode of ["sync", "async", "needles", "flush"]) {
        let calls = 0;
        const resolve = () => {
          calls++;
          return new Uint8Array(65536);
        };
        const options = { open: "{{", close: "}}", resolve };
        const stream =
          mode === "sync"
            ? createTokenStreamPair(options)
            : mode === "async"
              ? createAsyncTokenStreamPair({ ...options, resolve: async () => resolve() })
              : createNeedleStreamPair({
                  needles: mode === "flush" ? ["a", `${"a".repeat(1024)}b`] : ["{{x}}"],
                  resolve,
                });
        const reader = stream.readable.getReader();
        const writer = stream.writable.getWriter();
        const first = reader.read();
        const written = writer
          .write(bytes(mode === "flush" ? "a".repeat(512) : "{{x}}".repeat(16)))
          .then(() => writer.close())
          .catch(() => {});
        await first;
        await new Promise((done) => setTimeout(done, 0));
        if (calls > 2) throw new Error(`${mode} ran ahead: ${calls} resolutions`);
        await reader.cancel("stop");
        await written;
        reader.releaseLock();
        writer.releaseLock();
      }
    },
  },
  {
    name: "flushes the first prefix before an async lookup",
    run: async () => {
      const gate = deferred<Uint8Array | null>();
      const parts: Uint8Array[] = [];
      const body = createAsyncTokenTransformer({
        open: "{{",
        close: "}}",
        resolve: () => gate.promise,
      });
      const ctrl = {
        enqueue: (part: Uint8Array) => parts.push(part),
      } as unknown as TransformStreamDefaultController<Uint8Array>;
      const pending = body.transform(bytes("head{{x}}tail"), ctrl);
      assertSame(concat(parts), bytes("head"), "prefix before resolution");
      gate.resolve(bytes("X"));
      await pending;
      body.flush(ctrl);
      assertSame(concat(parts), bytes("headXtail"), "completed output");
    },
  },
  {
    name: "abort signal detaches a pending resolver",
    run: async () => {
      const gate = deferred<Uint8Array | null>();
      const started = deferred<void>();
      const abort = new AbortController();
      let calls = 0;
      const body = createAsyncTokenTransformer({
        open: "{{",
        close: "}}",
        signal: abort.signal,
        resolve: () => {
          calls++;
          started.resolve();
          return gate.promise;
        },
      });
      const ctrl = { enqueue() {} } as TransformStreamDefaultController<Uint8Array>;
      const written = body.transform(bytes("{{a}}{{b}}"), ctrl).then(
        () => {
          throw new Error("write must reject");
        },
        (reason) => {
          if (reason !== "stop") throw reason;
        },
      );
      await started.promise;
      abort.abort("stop");
      await written;
      gate.resolve(null);
      await Promise.resolve();
      if (calls !== 1) throw new Error("resolver restarted after abort");
    },
  },
  {
    name: "recovers from a throwing thenable getter",
    run: async () => {
      const failure = new Error("then getter");
      // biome-ignore lint/suspicious/noThenProperty: intentional thenable getter regression
      const thenable = Object.defineProperty({}, "then", {
        get() {
          throw failure;
        },
      }) as PromiseLike<Uint8Array>;
      const output = await runAsyncStream([bytes("{{a}}{{b}}")], {
        open: "{{",
        close: "}}",
        resolve: () => thenable,
        onResolveError: (error, payload) => {
          if (error !== failure) throw new Error("wrong resolver error");
          return payload;
        },
      });
      assertSame(output, bytes("ab"), "thenable error recovery");
    },
  },
  {
    name: "handles long overlapping opening delimiters",
    run: async () => {
      const open = `${"a".repeat(1023)}b`;
      const input = bytes(`${"a".repeat(4096)}bx!`);
      const expected = bytes(`${"a".repeat(4096 - 1023)}X`);
      const options = { open, close: "!", resolve: () => bytes("X") };
      for (const cut of [0, 512, 1024, 4096, input.length]) {
        const chunks = [input.subarray(0, cut), input.subarray(cut)];
        assertSame(await runStream(chunks, options), expected, `long open cut=${cut}`);
        assertSame(
          await runAsyncStream(chunks, {
            ...options,
            resolve: async () => bytes("X"),
          }),
          expected,
          `async long open cut=${cut}`,
        );
      }
    },
  },
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
