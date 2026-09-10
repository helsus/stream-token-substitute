// Stream behaviour rather than substitution semantics: awaitable resolvers,
// counts, backpressure, zero-copy output and carried-state bounds.

import { describe, expect, it } from "vitest";
import {
  createAsyncTokenStreamPair,
  createAsyncTokenTransformer,
  createAsyncTokenTransformStream,
} from "../src/async-transformer.ts";
import { substituteResponse } from "../src/helpers.ts";
import { createTokenStreamPair as exportedTokenStreamPair } from "../src/index.ts";
import { createNeedleStreamPair, createNeedleTransformStream } from "../src/needles.ts";
import { createTokenStreamPair, createTokenTransformStream } from "../src/transformer.ts";
import type {
  AsyncTokenTransformOptions,
  TokenStats,
  TokenTransformOptions,
} from "../src/types.ts";
import {
  bytes,
  concat,
  decoder,
  deferResolver,
  deferred,
  runAsyncStream,
  runAsyncStreamParts,
  runStream,
  runStreamParts,
  splitAt,
} from "./helpers.ts";
import { substituteBytes } from "./reference-impl.ts";

const open = bytes("{{");
const close = bytes("}}");

const values = new Map([
  ["name", bytes("Ada")],
  ["city", bytes("London")],
]);

const asyncOptions = (
  extra: Partial<AsyncTokenTransformOptions> = {},
): AsyncTokenTransformOptions => ({
  open,
  close,
  resolve: async (payload) => values.get(decoder.decode(payload)) ?? null,
  ...extra,
});

describe("async transformer", () => {
  it("substitutes across an await", async () => {
    const out = await runAsyncStream([bytes("hi {{name}} from {{city}}!")], asyncOptions());
    expect(decoder.decode(out)).toBe("hi Ada from London!");
  });

  it("resumes correctly when a token straddles every chunk boundary", async () => {
    const input = bytes("a{{name}}b{{nope}}c{{city}}d");
    const expected = substituteBytes(input, {
      open,
      close,
      resolve: (payload) => values.get(decoder.decode(payload)) ?? null,
    });
    for (let cut = 0; cut <= input.length; cut++) {
      const out = await runAsyncStream(splitAt(input, [cut]), asyncOptions());
      expect(decoder.decode(out), `cut=${cut}`).toBe(decoder.decode(expected));
    }
  });

  it("takes the non-suspending path when the resolver answers synchronously", async () => {
    const out = await runAsyncStream(
      [bytes("{{name}}")],
      asyncOptions({ resolve: deferResolver(() => bytes("SYNC"), "sync") }),
    );
    expect(decoder.decode(out)).toBe("SYNC");
  });

  it("awaits callable thenables", async () => {
    const thenable = () => {};
    // biome-ignore lint/suspicious/noThenProperty: this intentionally exercises thenable assimilation
    Object.defineProperty(thenable, "then", {
      value: (resolve: (value: Uint8Array) => void) => resolve(bytes("VALUE")),
    });
    const out = await runAsyncStream(
      [bytes("{{name}}")],
      asyncOptions({ resolve: () => thenable as unknown as Promise<Uint8Array> }),
    );
    expect(decoder.decode(out)).toBe("VALUE");
  });

  it("survives a resolver that only settles on a later macrotask", async () => {
    const out = await runAsyncStream(
      [bytes("<{{name}}|{{city}}>")],
      asyncOptions({
        resolve: deferResolver((p) => values.get(decoder.decode(p)) ?? null, "macro"),
      }),
    );
    expect(decoder.decode(out)).toBe("<Ada|London>");
  });

  it("hands the resolver a payload that survives the await", async () => {
    const seen: string[] = [];
    await runAsyncStream([bytes("{{alpha}}{{beta}}{{gamma}}")], {
      open,
      close,
      // Reads the payload only after suspending. A shared scratch view would
      // show a later token's bytes here.
      resolve: (payload) =>
        Promise.resolve().then(() => {
          seen.push(decoder.decode(payload));
          return null;
        }),
    });
    expect(seen).toEqual(["alpha", "beta", "gamma"]);
  });

  it("resolves tokens in stream order", async () => {
    const order: string[] = [];
    await runAsyncStream([bytes("{{one}}{{two}}{{three}}")], {
      open,
      close,
      resolve: async (payload) => {
        const name = decoder.decode(payload);
        // Deliberately inverted delays: ordering must come from the scanner
        // suspending, not from how fast each resolver happens to settle.
        await new Promise((done) => setTimeout(done, name === "one" ? 6 : 0));
        order.push(name);
        return null;
      },
    });
    expect(order).toEqual(["one", "two", "three"]);
  });

  it("errors the stream when an awaited resolver rejects", async () => {
    await expect(
      runAsyncStream([bytes("{{name}}")], {
        open,
        close,
        resolve: async () => {
          throw new Error("kv down");
        },
      }),
    ).rejects.toThrow("kv down");
  });

  it("still emits unterminated tokens verbatim at flush", async () => {
    const out = await runAsyncStream([bytes("tail{{unterminated")], asyncOptions());
    expect(decoder.decode(out)).toBe("tail{{unterminated");
  });

  it("re-scans an aborted token whose inner token resolves asynchronously", async () => {
    const options: AsyncTokenTransformOptions = {
      open,
      close,
      validate: (_p, next) => next >= 0x61 && next <= 0x7a,
      resolve: async (payload) => (decoder.decode(payload) === "b" ? bytes("B") : null),
    };
    const out = await runAsyncStream([bytes("{{a{{b}}")], options);
    expect(decoder.decode(out)).toBe("{{aB");
  });

  it("rejects a non-Uint8Array chunk", async () => {
    const tx = createAsyncTokenTransformStream(asyncOptions());
    const writer = tx.writable.getWriter();
    const read = tx.readable
      .getReader()
      .read()
      .catch((e: Error) => e);
    await expect(writer.write("nope" as unknown as Uint8Array)).rejects.toThrow(TypeError);
    expect(await read).toBeInstanceOf(TypeError);
  });

  it("honours flushBytes framing", async () => {
    const parts = await runAsyncStreamParts(
      [bytes("a{{name}}b{{city}}c")],
      asyncOptions({ flushBytes: 0 }),
    );
    expect(parts.length).toBeGreaterThan(1);
    expect(decoder.decode(await runAsyncStream([bytes("a{{name}}b")], asyncOptions()))).toBe(
      "aAdab",
    );
  });
});

async function statsFor(
  input: string,
  extra: Partial<TokenTransformOptions> = {},
): Promise<TokenStats> {
  let stats: TokenStats | undefined;
  await runStream([bytes(input)], {
    open,
    close,
    resolve: (payload) => (decoder.decode(payload) === "ok" ? bytes("V") : null),
    onDone: (s) => {
      stats = s;
    },
    ...extra,
  });
  if (stats === undefined) throw new Error("onDone was not called");
  return stats;
}

describe("onDone stats", () => {
  it("counts resolved, rejected and aborted tokens", async () => {
    const stats = await statsFor("{{ok}} {{no}} {{toolong}}", {
      maxPayloadBytes: 4,
    });
    expect(stats.resolved).toBe(1);
    expect(stats.rejected).toBe(1);
    expect(stats.aborted).toBe(1);
  });

  it("counts an empty substitution as resolved, not rejected", async () => {
    const stats = await statsFor("{{gone}}", { resolve: () => new Uint8Array(0) });
    expect(stats).toMatchObject({ resolved: 1, rejected: 0, aborted: 0 });
  });

  it("reports byte totals that match the actual stream", async () => {
    const input = "prefix {{ok}} suffix";
    const stats = await statsFor(input);
    const out = await runStream([bytes(input)], {
      open,
      close,
      resolve: (payload) => (decoder.decode(payload) === "ok" ? bytes("V") : null),
    });
    expect(stats.bytesIn).toBe(bytes(input).length);
    expect(stats.bytesOut).toBe(out.length);
  });

  it("sums bytesIn across chunks and fires exactly once", async () => {
    let calls = 0;
    let stats: TokenStats | undefined;
    await runStream([bytes("aaa"), bytes("bb"), bytes("")], {
      open,
      close,
      resolve: () => null,
      onDone: (s) => {
        calls++;
        stats = s;
      },
    });
    expect(calls).toBe(1);
    expect(stats?.bytesIn).toBe(5);
  });

  it("counts through the async transformer too", async () => {
    let stats: TokenStats | undefined;
    await runAsyncStream([bytes("{{ok}}{{no}}")], {
      open,
      close,
      resolve: async (payload) => (decoder.decode(payload) === "ok" ? bytes("V") : null),
      onDone: (s) => {
        stats = s;
      },
    });
    expect(stats).toMatchObject({ resolved: 1, rejected: 1, aborted: 0 });
  });
});

describe("onResolveError", () => {
  const boom: TokenTransformOptions = {
    open,
    close,
    resolve: () => {
      throw new Error("kv down");
    },
  };

  it("propagates and errors the stream when absent", async () => {
    await expect(runStream([bytes("a{{x}}b")], boom)).rejects.toThrow("kv down");
  });

  it("substitutes the bytes the handler returns", async () => {
    const out = await runStream([bytes("a{{x}}b")], {
      ...boom,
      onResolveError: () => bytes("FALLBACK"),
    });
    expect(decoder.decode(out)).toBe("aFALLBACKb");
  });

  it("emits the token verbatim when the handler returns null", async () => {
    const out = await runStream([bytes("a{{x}}b")], { ...boom, onResolveError: () => null });
    expect(decoder.decode(out)).toBe("a{{x}}b");
  });

  it("receives the error and a retainable payload copy", async () => {
    const seen: Array<{ message: string; payload: Uint8Array }> = [];
    await runStream([bytes("{{one}}{{two}}")], {
      ...boom,
      onResolveError: (error, payload) => {
        seen.push({ message: (error as Error).message, payload });
        return null;
      },
    });
    expect(seen.map((s) => decoder.decode(s.payload))).toEqual(["one", "two"]);
    expect(seen[0].message).toBe("kv down");
  });

  it("still errors the stream if the handler itself throws", async () => {
    await expect(
      runStream([bytes("{{x}}")], {
        ...boom,
        onResolveError: () => {
          throw new Error("handler too");
        },
      }),
    ).rejects.toThrow("handler too");
  });

  it("catches a rejected promise from an async resolver", async () => {
    const out = await runAsyncStream([bytes("a{{x}}b")], {
      open,
      close,
      resolve: async () => {
        throw new Error("kv down");
      },
      onResolveError: () => bytes("FALLBACK"),
    });
    expect(decoder.decode(out)).toBe("aFALLBACKb");
  });

  it("does not intercept a throwing validator", async () => {
    await expect(
      runStream([bytes("{{x}}")], {
        open,
        close,
        resolve: () => null,
        validate: () => {
          throw new Error("bad validator");
        },
        onResolveError: () => bytes("FALLBACK"),
      }),
    ).rejects.toThrow("bad validator");
  });
});

const tick = (): Promise<void> => new Promise((done) => setTimeout(done, 0));

/** One chunk of content plus a token, so every write produces output. */
const CHUNK = bytes(`${"x".repeat(4096)}{{a}}`);

/** A stalled reader must stop the writer. The queue between them is bounded by
 *  one chunk's output, whatever the source does. */
async function assertBounded(tx: TransformStream<Uint8Array, Uint8Array>): Promise<void> {
  const writer = tx.writable.getWriter();
  const reader = tx.readable.getReader();
  let settled = 0;
  for (let i = 0; i < 8; i++) void writer.write(CHUNK).then(() => settled++);

  await tick();
  expect(settled).toBe(0);

  await reader.read();
  await tick();
  // Exactly one write clears per read: the transform side never runs ahead.
  expect(settled).toBe(1);

  reader.releaseLock();
  writer.releaseLock();
}

describe("Web factory compatibility", () => {
  it("exports the sync pair factory from the main entrypoint", () => {
    expect(exportedTokenStreamPair).toBe(createTokenStreamPair);
  });

  for (const mode of ["sync", "async", "needles"] as const) {
    it(`retains native ${mode} identity and transferability`, async () => {
      const options = { open: "{{", close: "}}", resolve: () => bytes("v") };
      const stream =
        mode === "sync"
          ? createTokenTransformStream(options)
          : mode === "async"
            ? createAsyncTokenTransformStream(options)
            : createNeedleTransformStream({ needles: { "{{x}}": "v" } });
      expect(stream).toBeInstanceOf(TransformStream);
      const getter = Object.getOwnPropertyDescriptor(TransformStream.prototype, "readable")?.get;
      if (!getter) throw new Error("missing native getter");
      expect(getter.call(stream)).toBe(stream.readable);
      const transferred = structuredClone(stream, { transfer: [stream] });
      expect(transferred).toBeInstanceOf(TransformStream);
      const result = substituteResponse(new Response("{{x}}"), transferred);
      expect(await result.text()).toBe("v");
    });

    it(`accepts an opt-in ${mode} pair in substituteResponse`, async () => {
      const options = { open: "{{", close: "}}", resolve: () => bytes("v") };
      const pair =
        mode === "sync"
          ? createTokenStreamPair(options)
          : mode === "async"
            ? createAsyncTokenStreamPair(options)
            : createNeedleStreamPair({ needles: { "{{x}}": "v" } });
      expect(pair).not.toBeInstanceOf(TransformStream);
      expect(await substituteResponse(new Response("{{x}}"), pair).text()).toBe("v");
    });
  }
});

describe("backpressure", () => {
  for (const mode of ["sync", "async", "needles"] as const) {
    it(`bounds ${mode} buffering with a large flushBytes setting`, async () => {
      let calls = 0;
      const resolve = () => {
        calls++;
        return new Uint8Array(99);
      };
      const options = { open: "{{", close: "}}", resolve, flushBytes: Number.MAX_SAFE_INTEGER };
      const tx =
        mode === "sync"
          ? createTokenStreamPair(options)
          : mode === "async"
            ? createAsyncTokenStreamPair({ ...options, resolve: async () => resolve() })
            : createNeedleStreamPair({
                needles: ["{{x}}"],
                resolve,
                flushBytes: options.flushBytes,
              });
      const reader = tx.readable.getReader();
      const writer = tx.writable.getWriter();
      const first = reader.read();
      const written = writer.write(bytes("{{x}}".repeat(10000))).then(() => writer.close());
      let count = (await first).value?.length ?? 0;
      await tick();
      expect(calls).toBeLessThan(400);
      for (;;) {
        const part = await reader.read();
        if (part.done) break;
        count += part.value.length;
      }
      await written;
      expect(count).toBe(990000);
      writer.releaseLock();
      reader.releaseLock();
    });
  }

  for (const mode of ["sync", "async", "needles", "needle flush"] as const) {
    it(`pauses ${mode} expansion within one input chunk`, async () => {
      let calls = 0;
      let completed = 0;
      const resolve = () => {
        calls++;
        return new Uint8Array(65536).fill(88);
      };
      const options = { open: "{{", close: "}}", resolve, onDone: () => completed++ };
      const tx =
        mode === "sync"
          ? createTokenStreamPair(options)
          : mode === "async"
            ? createAsyncTokenStreamPair({ ...options, resolve: async () => resolve() })
            : createNeedleStreamPair({
                needles: mode === "needle flush" ? ["a", `${"a".repeat(2048)}b`] : ["{{x}}"],
                resolve,
                onDone: () => completed++,
              });
      const reader = tx.readable.getReader();
      const writer = tx.writable.getWriter();
      const first = reader.read();
      let settled = false;
      const count = mode === "needle flush" ? 2000 : 100;
      const written = writer
        .write(bytes((mode === "needle flush" ? "a" : "{{x}}").repeat(count)))
        .then(() => writer.close())
        .then(() => {
          settled = true;
        });
      let total = (await first).value?.length ?? 0;
      await tick();
      expect(calls).toBeLessThanOrEqual(2);
      expect(settled).toBe(false);
      for (;;) {
        const part = await reader.read();
        if (part.done) break;
        total += part.value.length;
      }
      await written;
      expect(total).toBe(count * 65536);
      expect(calls).toBe(count);
      expect(completed).toBe(1);
      reader.releaseLock();
      writer.releaseLock();
    });
  }

  it("aborts a paused write without waiting for a reader", async () => {
    const tx = createTokenStreamPair({
      open: "{{",
      close: "}}",
      resolve: () => new Uint8Array(65536),
    });
    const writer = tx.writable.getWriter();
    const reader = tx.readable.getReader();
    const first = reader.read();
    const written = writer.write(bytes("{{x}}".repeat(100))).catch((error) => error);
    await first;
    const error = new Error("stop");
    await writer.abort(error);
    expect(await written).toBe(error);
    await expect(reader.read()).rejects.toBe(error);
    writer.releaseLock();
    reader.releaseLock();
  });

  it("holds the writer while the reader is stalled", async () => {
    await assertBounded(
      createTokenTransformStream({ open: "{{", close: "}}", resolve: () => bytes("v") }),
    );
  });

  it("holds the writer through an awaitable resolver", async () => {
    await assertBounded(
      createAsyncTokenTransformStream({
        open: "{{",
        close: "}}",
        resolve: async () => bytes("v"),
      }),
    );
  });

  it("holds the writer in needle mode", async () => {
    await assertBounded(createNeedleTransformStream({ needles: { "{{a}}": "v" } }));
  });
});

/** Enqueued parts that are views into the chunk they arrived in cost no copy.
 *  This is the property the accumulator's 1 kB pass-through exists to preserve,
 *  and it is what makes a sparse body cheap. */
async function copyRatio(
  input: Uint8Array,
  make: () => TransformStream<Uint8Array, Uint8Array>,
  chunkSize = 16384,
): Promise<{ copied: number; viewed: number; parts: number }> {
  const chunks: Uint8Array[] = [];
  for (let i = 0; i < input.length; i += chunkSize) {
    // A distinct buffer per chunk, so a view is unambiguously a view.
    chunks.push(new Uint8Array(input.subarray(i, i + chunkSize)));
  }
  const buffers = new Set(chunks.map((chunk) => chunk.buffer));

  const tx = make();
  const writer = tx.writable.getWriter();
  const reader = tx.readable.getReader();
  let copied = 0;
  let viewed = 0;
  let parts = 0;
  const pump = (async () => {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      parts++;
      if (buffers.has(value.buffer)) viewed += value.length;
      else copied += value.length;
    }
  })();
  for (const chunk of chunks) await writer.write(chunk);
  await writer.close();
  await pump;
  return { copied, viewed, parts };
}

const shell = (holes: number, hole: (index: number) => string): Uint8Array => {
  const filler = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(600);
  const parts: string[] = [];
  for (let i = 0; i < holes; i++) parts.push(filler + hole(i));
  return bytes(parts.join(""));
};

describe("zero-copy output", () => {
  it("copies almost nothing on a sparse body", async () => {
    const input = shell(6, (i) => `{{h${i}}}`);
    const { copied, viewed } = await copyRatio(input, () =>
      createTokenTransformStream({
        open: "{{",
        close: "}}",
        resolve: () => bytes("value"),
      }),
    );
    // Only the substituted values and the odd held prefix are copies.
    expect(copied).toBeLessThan(1024);
    expect(viewed).toBeGreaterThan(input.length - 1024);
  });

  it("copies almost nothing on a sparse body in needle mode", async () => {
    const input = shell(6, () => "__ID__");
    const { copied, viewed } = await copyRatio(input, () =>
      createNeedleTransformStream({ needles: { __ID__: "value" } }),
    );
    expect(copied).toBeLessThan(1024);
    expect(viewed).toBeGreaterThan(input.length - 1024);
  });

  it("still merges a dense body into few parts", async () => {
    // A hole every ~60 bytes: every span is small, so merging is worth it.
    const input = bytes(`${"x".repeat(56)}{{a}}`.repeat(2000));
    const { parts } = await copyRatio(input, () =>
      createTokenTransformStream({ open: "{{", close: "}}", resolve: () => bytes("v") }),
    );
    // Without the accumulator this would be one part per span and per value.
    expect(parts).toBeLessThan(50);
  });
});

const value = bytes("VALUE");

const gc = (globalThis as { gc?: () => void }).gc;

/** Write chunks one at a time, reading each enqueue before the next write, and
 *  never retaining an output view. */
async function pipeStats(chunks: () => Generator<Uint8Array>) {
  const tx = createTokenTransformStream({ open, close, resolve: () => value });
  const writer = tx.writable.getWriter();
  const reader = tx.readable.getReader();
  let total = 0;
  let maxPart = 0;
  let firstOutputAtWrite = -1;
  let writes = 0;
  const pump = (async () => {
    for (;;) {
      const { done, value: out } = await reader.read();
      if (done) break;
      if (firstOutputAtWrite < 0) firstOutputAtWrite = writes;
      if (out.length > maxPart) maxPart = out.length;
      total += out.length;
    }
  })();
  for (const chunk of chunks()) {
    await writer.write(chunk);
    writes++;
  }
  await writer.close();
  await pump;
  return { total, maxPart, firstOutputAtWrite, writes };
}

describe("memory", () => {
  it("streams 50 MB without buffering the body", async () => {
    const unit = "<p>{{name}}</p> filler filler filler ";
    const chunk = bytes(unit.repeat(1800)); // ~64 KB
    const rounds = Math.ceil((50 * 1024 * 1024) / chunk.length);
    const perChunk = chunk.length + 1800 * (value.length - "{{name}}".length);

    const stats = await pipeStats(function* () {
      for (let i = 0; i < rounds; i++) yield chunk;
    });

    expect(stats.total).toBe(perChunk * rounds);
    // Structural, not RSS: output starts on the first chunk and no single
    // enqueue ever exceeds one chunk, so nothing accumulates across the body.
    // bench/bench.ts samples process memory.
    expect(stats.firstOutputAtWrite).toBe(0);
    expect(stats.maxPart).toBeLessThanOrEqual(chunk.length);
    expect(stats.writes).toBe(rounds);
  }, 60_000);

  it.skipIf(!gc)("does not retain input chunks across transform calls", async () => {
    const tx = createTokenTransformStream({ open, close, resolve: () => value });
    const writer = tx.writable.getWriter();
    const reader = tx.readable.getReader();

    const ref = await (async () => {
      const first = bytes("no tokens here, just content");
      const weak = new WeakRef(first.buffer);
      const read = reader.read();
      await writer.write(first);
      const got = await read;
      expect(got.value?.length).toBe(first.length);
      return weak;
    })();

    // A second chunk, so the transformer has moved on from the first.
    const read2 = reader.read();
    await writer.write(bytes("more content"));
    await read2;

    gc?.();
    await new Promise((r) => setTimeout(r, 0));
    gc?.();

    expect(ref.deref()).toBeUndefined();
    await writer.close();
  });

  it("keeps the payload scratch bounded by maxPayloadBytes", async () => {
    // A single unterminated token far longer than the cap must not grow state.
    const chunk = bytes(`{{${"a".repeat(100_000)}`);
    let maxPayload = 0;
    const parts = await runStreamParts([chunk], {
      open,
      close,
      resolve: () => value,
      maxPayloadBytes: 8,
      validate: (payload) => {
        if (payload.length > maxPayload) maxPayload = payload.length;
        return true;
      },
    });
    const total = parts.reduce((n, p) => n + p.length, 0);
    expect(total).toBe(chunk.length);
    expect(maxPayload).toBeLessThanOrEqual(8);
  });
});

describe("async cancellation", () => {
  const ctrl = { enqueue() {} } as unknown as TransformStreamDefaultController<Uint8Array>;

  for (const preAborted of [false, true]) {
    it(`rejects close when aborted ${preAborted ? "before creation" : "between writes"}`, async () => {
      const abort = new AbortController();
      const failure = new Error("request cancelled");
      if (preAborted) abort.abort(failure);
      const stream = createAsyncTokenTransformStream({
        open: "{{",
        close: "}}",
        signal: abort.signal,
        resolve: async () => null,
      });
      const reader = stream.readable.getReader();
      const writer = stream.writable.getWriter();
      if (!preAborted) {
        const first = reader.read();
        await writer.write(bytes("head{{unfinished"));
        expect(decoder.decode((await first).value)).toBe("head");
        abort.abort(failure);
      }
      const read = expect(reader.read()).rejects.toBe(failure);
      await expect(writer.close()).rejects.toBe(failure);
      await read;
      reader.releaseLock();
      writer.releaseLock();
    });
  }

  it("detaches pending work on readable cancellation", async () => {
    const gate = deferred<Uint8Array | null>();
    const started = deferred<void>();
    const stream = createAsyncTokenTransformStream({
      open: "{{",
      close: "}}",
      resolve: () => {
        started.resolve();
        return gate.promise;
      },
    });
    const reader = stream.readable.getReader();
    const writer = stream.writable.getWriter();
    const read = reader.read();
    const written = writer.write(bytes("{{a}}")).catch((reason) => reason);
    await started.promise;
    await reader.cancel("stop");
    expect(await written).toBe("stop");
    expect((await read).done).toBe(true);
    gate.resolve(null);
    reader.releaseLock();
    writer.releaseLock();
  });
  for (const lateFailure of [false, true]) {
    it(`detaches pending work before late ${lateFailure ? "rejection" : "fulfillment"}`, async () => {
      const gate = deferred<Uint8Array | null>();
      const abort = new AbortController();
      let calls = 0;
      let recovered = 0;
      const body = createAsyncTokenTransformer({
        open: "{{",
        close: "}}",
        signal: abort.signal,
        resolve: () => {
          calls++;
          return gate.promise;
        },
        onResolveError: () => {
          recovered++;
          return null;
        },
      });
      const pending = body.transform(bytes("{{a}}{{b}}"), ctrl);
      const failure = new Error("cancelled");
      abort.abort(failure);
      await expect(pending).rejects.toBe(failure);
      if (lateFailure) gate.reject(new Error("late"));
      else gate.resolve(null);
      await Promise.resolve();
      expect(calls).toBe(1);
      expect(recovered).toBe(0);
    });
  }

  for (const promised of [false, true]) {
    it(`handles abort inside a ${promised ? "promised" : "direct"} resolver`, async () => {
      const abort = new AbortController();
      const failure = new Error("abort in resolver");
      let calls = 0;
      const body = createAsyncTokenTransformer({
        open: "{{",
        close: "}}",
        signal: abort.signal,
        resolve: () => {
          calls++;
          abort.abort(failure);
          return promised ? Promise.reject(new Error("late")) : null;
        },
      });
      await expect(body.transform(bytes("{{a}}{{b}}"), ctrl)).rejects.toBe(failure);
      expect(calls).toBe(1);
    });
  }

  it("preserves undefined rejection reasons", async () => {
    const body = createAsyncTokenTransformer({
      open: "{{",
      close: "}}",
      resolve: () => Promise.reject(undefined),
    });
    await expect(body.transform(bytes("{{a}}"), ctrl)).rejects.toBeUndefined();
    await expect(body.transform(bytes("{{b}}"), ctrl)).rejects.toBeUndefined();
  });

  it("rejects a pre-aborted signal without calling the resolver", async () => {
    const abort = new AbortController();
    abort.abort("stop");
    const body = createAsyncTokenTransformer({
      open: "{{",
      close: "}}",
      signal: abort.signal,
      resolve: () => {
        throw new Error("unexpected resolver");
      },
    });
    await expect(body.transform(bytes("{{a}}"), ctrl)).rejects.toBe("stop");
  });
});

describe("first output", () => {
  it("does not call an overridden catch while discarding a resolver promise", async () => {
    const gate = deferred<Uint8Array | null>();
    Object.defineProperty(gate.promise, "catch", {
      value() {
        throw new Error("unexpected catch");
      },
    });
    const failure = new Error("enqueue failed");
    const body = createAsyncTokenTransformer({
      open: "{{",
      close: "}}",
      resolve: () => gate.promise,
    });
    const ctrl = {
      enqueue() {
        throw failure;
      },
    } as unknown as TransformStreamDefaultController<Uint8Array>;
    await expect(body.transform(bytes("head{{x}}"), ctrl)).rejects.toBe(failure);
    gate.reject(new Error("late rejection"));
    await Promise.resolve();
  });

  for (const prefix of ["<head>", "{{fast}}", "{{promised}}"]) {
    it(`delivers ${prefix} before a later lookup settles`, async () => {
      const gate = deferred<Uint8Array | null>();
      const stream = createAsyncTokenTransformStream({
        open: "{{",
        close: "}}",
        resolve: (payload) => {
          const name = decoder.decode(payload);
          return name === "fast"
            ? bytes("F")
            : name === "promised"
              ? Promise.resolve(bytes("F"))
              : gate.promise;
        },
      });
      const reader = stream.readable.getReader();
      const writer = stream.writable.getWriter();
      const first = reader.read();
      const written = writer.write(bytes(`${prefix}{{slow}}tail`));
      expect(decoder.decode((await first).value)).toBe(prefix === "<head>" ? prefix : "F");
      gate.resolve(bytes("S"));
      const rest: Uint8Array[] = [];
      const drained = (async () => {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          rest.push(value);
        }
      })();
      await written;
      await writer.close();
      await drained;
      expect(decoder.decode(concat(rest))).toBe("Stail");
      reader.releaseLock();
      writer.releaseLock();
    });
  }

  it("does not flush every later promise", async () => {
    const parts: Uint8Array[] = [];
    const ctrl = {
      enqueue: (part: Uint8Array) => parts.push(part),
    } as unknown as TransformStreamDefaultController<Uint8Array>;
    const body = createAsyncTokenTransformer({
      open: "{{",
      close: "}}",
      resolve: async () => bytes("X"),
    });
    await body.transform(bytes(`head${"{{x}}".repeat(100)}`), ctrl);
    body.flush(ctrl);
    expect(parts.length).toBe(2);
    expect(decoder.decode(concat(parts))).toBe(`head${"X".repeat(100)}`);
  });

  it("handles an enqueue failure at the first await", async () => {
    const gate = deferred<Uint8Array | null>();
    const failure = new Error("enqueue failed");
    const body = createAsyncTokenTransformer({
      open: "{{",
      close: "}}",
      resolve: () => gate.promise,
    });
    const ctrl = {
      enqueue: () => {
        throw failure;
      },
    } as unknown as TransformStreamDefaultController<Uint8Array>;
    await expect(body.transform(bytes("head{{x}}"), ctrl)).rejects.toBe(failure);
    gate.reject(new Error("late rejection"));
    await Promise.resolve();
  });
});
