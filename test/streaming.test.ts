// Stream behaviour rather than substitution semantics: awaitable resolvers,
// counts, backpressure, zero-copy output and carried-state bounds.

import { describe, expect, it } from "vitest";
import { createAsyncTokenTransformStream } from "../src/async-transformer.ts";
import { createNeedleTransformStream } from "../src/needles.ts";
import { createTokenTransformStream } from "../src/transformer.ts";
import type {
  AsyncTokenTransformOptions,
  TokenStats,
  TokenTransformOptions,
} from "../src/types.ts";
import {
  bytes,
  decoder,
  deferResolver,
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

describe("backpressure", () => {
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
    // bench/memory.ts measures real RSS.
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
