// The unified resolver contract and ordered lookahead.

import { describe, expect, it } from "vitest";
import { createTokenStream, createTokenTransformer } from "../src/transformer.ts";
import type { TokenStats, TokenTransformOptions } from "../src/types.ts";
import { bytes, concat, decoder, deferred, runStream } from "./helpers.ts";

const base = { open: "{{", close: "}}" };
const run = async (input: string | string[], options: Omit<TokenTransformOptions, "open">) =>
  decoder.decode(
    await runStream((Array.isArray(input) ? input : [input]).map(bytes), { ...base, ...options }),
  );
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
    [[1], "array"],
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
    expect(stats).toMatchObject({ resolved: 2, rejected: 0 });
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

  it("rejects a stream piece that is not a Uint8Array", async () => {
    async function* bad() {
      yield "text" as unknown as Uint8Array;
    }
    await expect(run("{{x}}", { resolve: () => bad() })).rejects.toThrow(
      "replacement stream must yield Uint8Array chunks",
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
    // Iterator return() cancels without a reason.
    expect(cancelled).toHaveLength(1);
    late.resolve(streamOf(["l"], (reason) => cancelled.push(reason)));
    gate.resolve("S");
    await sleep(0);
    expect(cancelled).toHaveLength(2);
  });
});
