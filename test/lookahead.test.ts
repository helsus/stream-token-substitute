// The unified resolver contract and ordered lookahead.

import { Transform } from "node:stream";
import { describe, expect, it } from "vitest";
import { resolveName } from "../src/helpers.ts";
import {
  createLiteralStream,
  createLiteralTransformer,
  type LiteralStats,
  type LiteralTransformOptions,
} from "../src/literals.ts";
import { createLiteralTransform, createTokenTransform } from "../src/node.ts";
import { createTokenStream, createTokenTransformer } from "../src/transformer.ts";
import type { TokenStats, TokenTransformOptions } from "../src/types.ts";
import { bytes, concat, decoder, deferred, runStream } from "./helpers.ts";

const base = { open: "{{", close: "}}" };
const run = async (input: string | string[], options: Omit<TokenTransformOptions, "open">) =>
  decoder.decode(
    await runStream((Array.isArray(input) ? input : [input]).map(bytes), { ...base, ...options }),
  );
const E = String.fromCodePoint(0xe9);
const sleep = (ms: number) => new Promise((done) => setTimeout(done, ms));

function streamOf(parts: string[], onCancel: (reason: unknown) => void = () => {}) {
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      const part = parts.shift();
      if (part === undefined) controller.close();
      else controller.enqueue(bytes(part));
    },
    cancel: onCancel,
  });
}

describe("resolver contract", () => {
  it("awaits a promise from the default API", async () => {
    expect(await run("A{{a}}B", { resolve: async () => bytes("X") })).toBe("AXB");
  });

  it("hands out a fresh copy that may be returned or retained", async () => {
    expect(await run("{{a}}{{b}}", { resolve: (p) => p })).toBe("ab");
    const seen: Uint8Array[] = [];
    await run(["{{aaa}}{{b", "bb}}"], {
      resolve: (p) => {
        seen.push(p);
        return null;
      },
    });
    expect(seen.map((p) => decoder.decode(p))).toEqual(["aaa", "bbb"]);
    expect(seen[0].buffer).not.toBe(seen[1].buffer);
  });

  it("encodes string results as UTF-8", async () => {
    const e = String.fromCodePoint(0xe9);
    expect(await run("a{{x}}b{{y}}c", { resolve: (p) => (p[0] === 0x78 ? e : "") })).toBe(
      `a${e}bc`,
    );
    expect(await run("{{x}}", { resolve: async () => "ok" })).toBe("ok");
  });

  for (const [value, kind] of [
    [undefined, "undefined"],
    [42, "number"],
    [{}, "object"],
    [true, "boolean"],
  ] as const) {
    it(`rejects a ${kind} result with a TypeError`, async () => {
      const message = `resolve must return Uint8Array, string, null, a stream, or a promise of one; got ${kind}`;
      const resolve = () => value as unknown as null;
      await expect(run("{{x}}", { resolve })).rejects.toThrow(new TypeError(message));
      await expect(run("{{x}}", { resolve: async () => resolve() })).rejects.toThrow(message);
    });
  }

  it("rejects a non-positive concurrency", () => {
    for (const concurrency of [0, -1, 1.5, Number.NaN]) {
      expect(() => createTokenTransformer({ ...base, concurrency, resolve: () => null })).toThrow(
        RangeError,
      );
    }
  });

  it("recovers a rejection through a promised onResolveError", async () => {
    let stats: TokenStats | undefined;
    const out = await run("a{{x}}b{{y}}c", {
      resolve: (p) => (p[0] === 0x78 ? Promise.reject(new Error("down")) : "Y"),
      onResolveError: async (_error, payload) => `<${decoder.decode(payload)}>`,
      onDone: (s) => {
        stats = s;
      },
    });
    expect(out).toBe("a<x>bYc");
    expect(stats).toMatchObject({ replaced: 2, rejected: 0 });
  });
});

describe("lookahead", () => {
  it("keeps output order when later lookups settle first", async () => {
    const started: string[] = [];
    const out = await run("<{{a}}|{{b}}|{{c}}>", {
      resolve: async (p) => {
        const name = decoder.decode(p);
        started.push(name);
        await sleep(name === "a" ? 20 : name === "b" ? 10 : 0);
        return name.toUpperCase();
      },
    });
    expect(out).toBe("<A|B|C>");
    expect(started).toEqual(["a", "b", "c"]);
  });

  it("starts lookups ahead of a slow one", async () => {
    const gate = deferred<string>();
    const calls: string[] = [];
    const body = createTokenTransformer({
      ...base,
      resolve: (p) => {
        calls.push(decoder.decode(p));
        return calls.length === 1 ? gate.promise : Promise.resolve("F");
      },
    });
    const parts: Uint8Array[] = [];
    const ctrl = {
      enqueue: (part: Uint8Array) => parts.push(part),
    } as unknown as TransformStreamDefaultController<Uint8Array>;
    await body.transform(bytes("h{{a}}{{b}}{{c}}t"), ctrl);
    expect(calls).toEqual(["a", "b", "c"]);
    expect(decoder.decode(concat(parts))).toBe("h");
    gate.resolve("S");
    await body.flush(ctrl);
    expect(decoder.decode(concat(parts))).toBe("hSFFt");
  });

  for (const concurrency of [1, 2, 5]) {
    it(`keeps at most ${concurrency} lookups in flight`, async () => {
      let inFlight = 0;
      let peak = 0;
      const out = await run(["x{{1}}{{2}}{{3", "}}{{4}}{{5}}{{6}}{{7}}y"], {
        concurrency,
        resolve: async (p) => {
          peak = Math.max(peak, ++inFlight);
          await sleep(1);
          inFlight--;
          return `<${decoder.decode(p)}>`;
        },
      });
      expect(out).toBe("x<1><2><3><4><5><6><7>y");
      expect(peak).toBe(concurrency);
    });
  }

  it("streams ReadableStream and async generator replacements in order", async () => {
    async function* gen() {
      yield bytes("g1");
      await sleep(1);
      yield bytes("g2");
    }
    const out = await run("[{{rs}}|{{gen}}|{{later}}|{{x}}]", {
      resolve: (p) => {
        const name = decoder.decode(p);
        if (name === "rs") return streamOf(["r1", "r2"]);
        if (name === "gen") return gen();
        if (name === "later") return sleep(1).then(() => streamOf(["l1"]));
        return "X";
      },
    });
    expect(out).toBe("[r1r2|g1g2|l1|X]");
  });

  it("rejects a stream piece that is not bytes or a string", async () => {
    async function* bad() {
      yield 42 as unknown as Uint8Array;
    }
    await expect(run("{{x}}", { resolve: () => bad() })).rejects.toThrow(
      "replacement stream must yield Uint8Array or string chunks",
    );
  });

  it("does not pull a streamed replacement while output is blocked", async () => {
    let pulls = 0;
    const big = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (++pulls > 8) controller.close();
        else controller.enqueue(new Uint8Array(65536));
      },
    });
    const pair = createTokenStream({ ...base, resolve: () => big });
    const reader = pair.readable.getReader();
    const writer = pair.writable.getWriter();
    const first = reader.read();
    const written = writer.write(bytes("{{x}}")).then(() => writer.close());
    await first;
    await sleep(10);
    expect(pulls).toBeLessThanOrEqual(3);
    let total = 65536;
    for (;;) {
      const part = await reader.read();
      if (part.done) break;
      total += part.value.length;
    }
    await written;
    expect(total).toBe(8 * 65536);
  });

  it("cancels pending streams and lookups on abort", async () => {
    const abort = new AbortController();
    const cancelled: unknown[] = [];
    const gate = deferred<string>();
    const late = deferred<ReadableStream<Uint8Array>>();
    const body = createTokenTransformer({
      ...base,
      signal: abort.signal,
      resolve: (p) => {
        const name = decoder.decode(p);
        if (name === "slow") return gate.promise;
        if (name === "late") return late.promise;
        return streamOf(["s"], (reason) => cancelled.push(reason));
      },
    });
    const ctrl = { enqueue() {} } as unknown as TransformStreamDefaultController<Uint8Array>;
    await body.transform(bytes("{{slow}}{{s}}{{late}}"), ctrl);
    const flushed = body.flush(ctrl);
    abort.abort("stop");
    await expect(flushed).rejects.toBe("stop");
    expect(cancelled).toEqual(["stop"]);
    late.resolve(streamOf(["l"], (reason) => cancelled.push(reason)));
    gate.resolve("S");
    await sleep(0);
    expect(cancelled).toEqual(["stop", "stop"]);
  });
});

describe("background failure and drain", () => {
  async function* late(value: unknown) {
    await sleep(5);
    yield value as Uint8Array;
  }
  async function* throwing() {
    await sleep(5);
    yield* [];
    throw new Error("late");
  }
  const failures = [
    ["rejected lookup", () => sleep(5).then(() => Promise.reject(new Error("late")))],
    ["throwing stream", () => throwing()],
    ["non-byte piece", () => late(42)],
  ] as const;

  for (const [name, resolve] of failures) {
    for (const mode of ["pair", "native", "node"] as const) {
      it(`surfaces a ${name} after the write settled (${mode})`, async () => {
        const options = { ...base, resolve: () => resolve() as unknown as Uint8Array };
        if (mode === "node") {
          const stream = createTokenTransform(options);
          const errored = new Promise<unknown>((done) => stream.once("error", done));
          stream.write(bytes("{{x}}"));
          stream.resume();
          await expect(
            Promise.race([errored, sleep(500).then(() => "hung")]),
          ).resolves.toBeInstanceOf(Error);
          return;
        }
        const tx =
          mode === "pair"
            ? createTokenStream(options)
            : new TransformStream<Uint8Array, Uint8Array>(createTokenTransformer(options));
        const writer = tx.writable.getWriter();
        const reader = tx.readable.getReader();
        const read = reader.read().then(
          () => "read",
          (error: unknown) => error,
        );
        await writer.write(bytes("{{x}}"));
        await expect(Promise.race([read, sleep(500).then(() => "hung")])).resolves.toBeInstanceOf(
          Error,
        );
        writer.releaseLock();
      });
    }
  }

  for (const mode of ["pair", "node"] as const) {
    it(`resumes a background drain once the reader catches up (${mode})`, async () => {
      async function* big() {
        await sleep(1);
        for (let k = 0; k < 8; k++) yield new Uint8Array(65536);
      }
      const options = { ...base, resolve: () => big() };
      let total = 0;
      const target = 8 * 65536;
      let reading: Promise<unknown>;
      if (mode === "node") {
        const stream = createTokenTransform(options, { highWaterMark: 1024 });
        stream.write(bytes("{{x}}"));
        await sleep(20);
        reading = new Promise<void>((done) => {
          stream.on("data", (part: Uint8Array) => {
            total += part.length;
            if (total >= target) done();
          });
        });
      } else {
        const tx = createTokenStream(options);
        const writer = tx.writable.getWriter();
        const reader = tx.readable.getReader();
        const first = reader.read();
        await writer.write(bytes("{{x}}"));
        await sleep(20);
        reading = (async () => {
          total += (await first).value?.length ?? 0;
          while (total < target) {
            const part = await reader.read();
            if (part.done) break;
            total += part.value.length;
          }
        })();
      }
      await Promise.race([reading, sleep(1000)]);
      expect(total).toBe(target);
    });
  }
});

async function pipeText(
  tx: ReadableWritablePair<Uint8Array, Uint8Array>,
  input: string[],
): Promise<string> {
  const reader = tx.readable.getReader();
  const writer = tx.writable.getWriter();
  const parts: Uint8Array[] = [];
  const pump = (async () => {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      parts.push(value);
    }
  })();
  pump.catch(() => {});
  for (const chunk of input) await writer.write(bytes(chunk));
  await writer.close();
  await pump;
  return decoder.decode(concat(parts));
}

describe("resolve context", () => {
  it("does not create an AbortController unless a resolver reads the signal", async () => {
    const Native = globalThis.AbortController;
    let created = 0;
    globalThis.AbortController = class extends Native {
      constructor() {
        super();
        created++;
      }
    };
    try {
      expect(await run("{{a}}", { resolve: () => "x" })).toBe("x");
      expect(created).toBe(0);
      expect(
        await run("{{a}}{{b}}", { resolve: (_p, context) => (context.signal ? "y" : "") }),
      ).toBe("yy");
      expect(created).toBe(1);
    } finally {
      globalThis.AbortController = Native;
    }
  });

  it("aborts the signal with the cancel reason, and not on completion", async () => {
    const signals: AbortSignal[] = [];
    const gate = deferred<string>();
    const body = createTokenTransformer({
      resolve: (_p, context) => {
        signals.push(context.signal);
        return gate.promise;
      },
    });
    const ctrl = { enqueue() {} } as unknown as TransformStreamDefaultController<Uint8Array>;
    await body.transform(bytes("{{a}}{{b}}"), ctrl);
    expect(signals).toHaveLength(2);
    expect(signals[0]).toBe(signals[1]);
    body.cancel?.("stop");
    expect(signals[0].aborted).toBe(true);
    expect(signals[0].reason).toBe("stop");

    let late: AbortSignal | undefined;
    const lateBody = createTokenTransformer({
      resolve: async (_p, context) => {
        await gate.promise;
        late = context.signal;
        return "x";
      },
    });
    await lateBody.transform(bytes("{{a}}"), ctrl);
    lateBody.cancel?.("late");
    gate.resolve("y");
    await gate.promise;
    await Promise.resolve();
    expect(late?.aborted).toBe(true);
    expect(late?.reason).toBe("late");

    let done: AbortSignal | undefined;
    await run("{{a}}", {
      resolve: async (_p, context) => {
        done = context.signal;
        return "x";
      },
    });
    expect(done?.aborted).toBe(false);
  });

  it("aborts the signal from the options signal and on failure", async () => {
    const abort = new AbortController();
    let seen: AbortSignal | undefined;
    const body = createTokenTransformer({
      signal: abort.signal,
      resolve: (_p, context) => {
        seen = context.signal;
        return new Promise<null>(() => {});
      },
    });
    const ctrl = { enqueue() {} } as unknown as TransformStreamDefaultController<Uint8Array>;
    await body.transform(bytes("{{a}}"), ctrl);
    abort.abort("gone");
    expect(seen?.reason).toBe("gone");

    const failure = new Error("down");
    let failed: AbortSignal | undefined;
    await expect(
      run("{{a}}{{b}}", {
        resolve: (p, context) => {
          failed = context.signal;
          return p[0] === 0x61 ? new Promise<null>(() => {}) : Promise.reject(failure);
        },
      }),
    ).rejects.toBe(failure);
    expect(failed?.reason).toBe(failure);
  });

  it("passes the same context to onResolveError", async () => {
    const contexts: unknown[] = [];
    const out = await run("{{a}}", {
      resolve: (_p, context) => {
        contexts.push(context);
        throw new Error("down");
      },
      onResolveError: (_error, _payload, context) => {
        contexts.push(context);
        return "R";
      },
    });
    expect(out).toBe("R");
    expect(contexts[0]).toBe(contexts[1]);
  });
});

describe("replacement pieces", () => {
  it("encodes string pieces and reads sync iterables", async () => {
    async function* mixed() {
      yield "s";
      yield bytes("b");
      yield "";
    }
    const out = await run("[{{a}}|{{b}}|{{c}}]", {
      resolve: (p) =>
        p[0] === 0x61 ? ["x", bytes("y"), "z"] : p[0] === 0x62 ? mixed() : new Set(["q"]),
    });
    expect(out).toBe("[xyz|sb|q]");
  });

  it("treats strings and bytes as values, not iterables", async () => {
    expect(await run("{{a}}{{b}}", { resolve: (p) => (p[0] === 0x61 ? "ab" : bytes("cd")) })).toBe(
      "abcd",
    );
  });

  it("reads a ReadableStream of strings from TextDecoderStream", async () => {
    const out = await run("<{{a}}>", {
      resolve: () =>
        new ReadableStream<BufferSource>({
          start(controller) {
            controller.enqueue(new Uint8Array([0x68, 0xc3]));
            controller.enqueue(new Uint8Array([0xa9, 0x6c, 0x6c, 0x6f]));
            controller.close();
          },
        }).pipeThrough(new TextDecoderStream()),
    });
    expect(out).toBe(`<h${E}llo>`);
  });
});

describe("resolveName", () => {
  it("decodes the payload as UTF-8", async () => {
    const resolve = resolveName((name) => (name === `${E}t${E}` ? "summer" : name.toUpperCase()));
    expect(await run(`{{ab}} {{${E}t${E}}}`, { resolve })).toBe("AB summer");
  });

  it("keeps a leading BOM", async () => {
    const resolve = resolveName((name) => (name === "\uFEFFa" ? "bom" : name));
    expect(await run("{{\uFEFFa}}{{a}}", { resolve })).toBe("boma");
  });

  it("passes the context and awaits results", async () => {
    let seen: AbortSignal | undefined;
    const resolve = resolveName(async (name, context) => {
      seen = context.signal;
      return `<${name}>`;
    });
    expect(await run("{{x}}", { resolve })).toBe("<x>");
    expect(seen).toBeInstanceOf(AbortSignal);
  });

  it("replaces invalid bytes", async () => {
    const names: string[] = [];
    const resolve = resolveName((name) => {
      names.push(name);
      return null;
    });
    await runStream([new Uint8Array([0x7b, 0x7b, 0xff, 0x7d, 0x7d])], { resolve });
    expect(names).toEqual([String.fromCodePoint(0xfffd)]);
  });
});

describe("delimiter defaults", () => {
  it("defaults to {{ and }}", async () => {
    const out = decoder.decode(
      await runStream([bytes("a{{x}}b")], { resolve: () => "X" } as TokenTransformOptions),
    );
    expect(out).toBe("aXb");
  });

  it("keeps close equal to a given open", async () => {
    expect(
      decoder.decode(await runStream([bytes("a%x%b")], { open: "%", resolve: () => "X" })),
    ).toBe("aXb");
  });

  it("defaults open when only close is given", async () => {
    expect(
      decoder.decode(await runStream([bytes("a{{x]]b")], { close: "]]", resolve: () => "X" })),
    ).toBe("aXb");
  });
});

describe("node factories", () => {
  it("return a Transform", () => {
    expect(createTokenTransform({ resolve: () => null })).toBeInstanceOf(Transform);
    expect(createLiteralTransform({ literals: { a: "b" } })).toBeInstanceOf(Transform);
  });
});

describe("literal lookahead", () => {
  const literal = (options: Omit<LiteralTransformOptions, "literals">, input: string[]) =>
    pipeText(createLiteralStream({ literals: ["a", "b", "c"], ...options }), input);

  it("keeps output order when later lookups settle first", async () => {
    const started: string[] = [];
    const out = await literal(
      {
        resolve: async (bytes, index) => {
          started.push(decoder.decode(bytes));
          await sleep(index === 0 ? 20 : index === 1 ? 10 : 0);
          return decoder.decode(bytes).toUpperCase();
        },
      },
      ["<a|b", "|c>"],
    );
    expect(out).toBe("<A|B|C>");
    expect(started).toEqual(["a", "b", "c"]);
  });

  for (const concurrency of [1, 2]) {
    it(`keeps at most ${concurrency} lookups in flight`, async () => {
      let inFlight = 0;
      let peak = 0;
      const out = await literal(
        {
          concurrency,
          resolve: async (bytes) => {
            peak = Math.max(peak, ++inFlight);
            await sleep(1);
            inFlight--;
            return `<${decoder.decode(bytes)}>`;
          },
        },
        ["xabcab", "cy"],
      );
      expect(out).toBe("x<a><b><c><a><b><c>y");
      expect(peak).toBe(concurrency);
    });
  }

  it("streams replacements and hands out a retainable copy", async () => {
    const seen: Uint8Array[] = [];
    const out = await literal(
      {
        resolve: (bytes, index) => {
          seen.push(bytes);
          if (index === 0) return streamOf(["1", "2"]);
          if (index === 1) return sleep(1).then(() => ["3", bytes]);
          return null;
        },
      },
      ["ab", "c"],
    );
    expect(out).toBe("123bc");
    expect(seen.map((b) => decoder.decode(b))).toEqual(["a", "b", "c"]);
  });

  it("recovers through onResolveError, sync and async", async () => {
    let stats: LiteralStats | undefined;
    const out = await literal(
      {
        resolve: (_bytes, index) => {
          if (index === 0) throw new Error("sync");
          if (index === 1) return Promise.reject(new Error("async"));
          return "C";
        },
        onResolveError: async (error, bytes) =>
          `${(error as Error).message}:${decoder.decode(bytes)}`,
        onDone: (s) => {
          stats = s;
        },
      },
      ["abc"],
    );
    expect(out).toBe("sync:aasync:bC");
    expect(stats).toEqual({ replaced: 3, rejected: 0, bytesIn: 3, bytesOut: out.length });
  });

  it("surfaces a background failure after the write settled", async () => {
    const tx = createLiteralStream({
      literals: ["a"],
      resolve: () => sleep(5).then(() => Promise.reject(new Error("late"))),
    });
    const writer = tx.writable.getWriter();
    const reader = tx.readable.getReader();
    const read = reader.read().then(
      () => "read",
      (error: unknown) => error,
    );
    await writer.write(bytes("a"));
    await expect(Promise.race([read, sleep(500).then(() => "hung")])).resolves.toBeInstanceOf(
      Error,
    );
    writer.releaseLock();
  });

  for (const promised of [false, true]) {
    it(`handles abort inside a ${promised ? "promised" : "direct"} literal resolver`, async () => {
      const abort = new AbortController();
      const failure = new Error("abort in resolver");
      let calls = 0;
      const body = createLiteralTransformer({
        literals: ["a"],
        signal: abort.signal,
        resolve: () => {
          calls++;
          abort.abort(failure);
          return promised ? Promise.reject(new Error("late")) : null;
        },
      });
      const ctrl = { enqueue() {} } as unknown as TransformStreamDefaultController<Uint8Array>;
      await expect(body.transform(bytes("aa"), ctrl)).rejects.toBe(failure);
      expect(calls).toBe(1);
    });
  }

  it("aborts the context signal on cancel", async () => {
    let seen: AbortSignal | undefined;
    const body = createLiteralTransformer({
      literals: ["a"],
      resolve: (_bytes, _index, context) => {
        seen = context.signal;
        return new Promise<null>(() => {});
      },
    });
    const ctrl = { enqueue() {} } as unknown as TransformStreamDefaultController<Uint8Array>;
    await body.transform(bytes("a"), ctrl);
    body.cancel?.("stop");
    expect(seen?.reason).toBe("stop");
  });
});

describe("borrow", () => {
  const literalRun = async (parts: string[], options: LiteralTransformOptions) =>
    decoder.decode(
      await new Response(
        new ReadableStream<Uint8Array>({
          start(c) {
            for (const part of parts) c.enqueue(bytes(part));
            c.close();
          },
        }).pipeThrough(createLiteralStream(options)),
      ).arrayBuffer(),
    );

  it("hands a borrowing resolver the same scratch buffer", async () => {
    const buffers: ArrayBufferLike[] = [];
    await run("{{a}}{{b}}", {
      borrow: true,
      resolve: (p) => {
        buffers.push(p.buffer);
        return bytes("x");
      },
    });
    expect(buffers[0]).toBe(buffers[1]);
  });

  it("copies a returned scratch view before emitting it", async () => {
    expect(await run("{{a}}{{b}}", { borrow: true, resolve: (p) => p })).toBe("ab");
    expect(await run(["{{ab}}{", "{cd}}"], { borrow: true, resolve: (p) => p })).toBe("abcd");
  });

  it("keeps null, thenable and stream results intact after the scratch is reused", async () => {
    const pick = (p: Uint8Array) => decoder.decode(p);
    expect(
      await run("{{a}}{{bb}}", { borrow: true, resolve: (p) => (p.length === 1 ? null : "X") }),
    ).toBe("{{a}}X");
    expect(
      await run("{{a}}{{bb}}{{c}}", {
        borrow: true,
        resolve: (p) => (p.length === 1 ? sleep(2).then(() => null) : "X"),
      }),
    ).toBe("{{a}}X{{c}}");
    expect(
      await run("{{a}}{{b}}", {
        borrow: true,
        resolve: (p) => streamOf([pick(p).toUpperCase()]),
      }),
    ).toBe("AB");
  });

  it("copies a borrowed view returned asynchronously", async () => {
    expect(
      await run("{{a}}{{bb}}{{c}}", { borrow: true, resolve: (p) => sleep(2).then(() => p) }),
    ).toBe("abbc");
    expect(
      await literalRun(["xa", "bxc", "dxa", "b"], {
        literals: ["ab", "cd"],
        borrow: true,
        mergeBytes: 0,
        resolve: (l) => sleep(1).then(() => l),
      }),
    ).toBe("xabxcdxab");
  });

  it("returns the argument safely after a cap abort and with a validator", async () => {
    const echo = { borrow: true, resolve: (p: Uint8Array) => p };
    expect(await run(["{{{{a", "b}}"], { ...echo, maxPayloadBytes: 3 })).toBe("{{ab");
    expect(await run(["{{a", "b}}{{cd}}"], { ...echo, validate: () => true })).toBe("abcd");
  });

  it("borrows through the overlap-heavy literal path", async () => {
    const literals = Array.from({ length: 40 }, (_, k) => `${"a".repeat(k + 1)}b`);
    literals.push("a");
    const input = "a".repeat(3000);
    expect(await literalRun([input], { literals, borrow: true, resolve: (l) => l })).toBe(input);
  });

  it("gives onResolveError a retainable copy", async () => {
    expect(
      await run("{{a}}{{b}}", {
        borrow: true,
        resolve: () => {
          throw new Error("boom");
        },
        onResolveError: async (_error, p) => {
          await sleep(1);
          return p;
        },
      }),
    ).toBe("ab");
  });

  it("works for literals across chunk boundaries", async () => {
    const options = { literals: ["ab", "cd"], borrow: true, resolve: (l: Uint8Array) => l };
    expect(await literalRun(["xa", "bycd"], options)).toBe("xabycd");
    const unmerged = { ...options, mergeBytes: 0 };
    expect(await literalRun(["xa", "bxc", "dxa", "b"], unmerged)).toBe("xabxcdxab");
    expect(
      await literalRun(["xa", "bycd"], {
        literals: ["ab", "cd"],
        borrow: true,
        resolve: (_literal, index) => (index === 0 ? sleep(2).then(() => null) : "Z"),
      }),
    ).toBe("xabyZ");
  });

  it("rejects a non-boolean borrow", () => {
    const borrow = 1 as unknown as boolean;
    expect(() => createTokenStream({ resolve: () => null, borrow })).toThrow(TypeError);
    expect(() => createLiteralStream({ literals: ["a"], resolve: () => null, borrow })).toThrow(
      TypeError,
    );
  });
});
