import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { describe, expect, it } from "vitest";
import {
  createAsyncTokenTransform,
  createNeedleTransform,
  createTokenTransform,
} from "../src/node.ts";
import { bytes, decoder, deferred } from "./helpers.ts";

const values = new Map([
  ["name", bytes("world")],
  ["n", bytes("42")],
]);

const resolve = (payload: Uint8Array) => values.get(decoder.decode(payload)) ?? null;

async function collect(stream: NodeJS.ReadableStream): Promise<string> {
  const parts: Uint8Array[] = [];
  for await (const part of stream) parts.push(part as Uint8Array);
  return parts.map((part) => decoder.decode(part)).join("");
}

const source = (parts: string[]) => Readable.from(parts.map((part) => Buffer.from(part)));

describe("node adapter", () => {
  it("preserves an async abort when resuming a backpressured write", async () => {
    const abort = new AbortController();
    const failure = new Error("request cancelled");
    const stream = createAsyncTokenTransform(
      {
        open: "{{",
        close: "}}",
        signal: abort.signal,
        resolve: async () => new Uint8Array(65536),
      },
      { highWaterMark: 1024 },
    );
    stream.on("error", () => {});
    const available = new Promise<void>((done) => stream.once("readable", done));
    const written = new Promise<Error | null | undefined>((done) => {
      stream.write(bytes("{{a}}{{b}}"), done);
    });
    await available;
    await new Promise((done) => setTimeout(done, 0));
    abort.abort(failure);
    await expect(collect(stream)).rejects.toBe(failure);
    expect(await written).toBe(failure);
  });

  it("pauses before resolving a needle behind a large prefix", async () => {
    let calls = 0;
    const stream = createNeedleTransform(
      {
        needles: ["{{x}}"],
        resolve: () => {
          calls++;
          return bytes("X");
        },
      },
      { highWaterMark: 1024 },
    );
    const available = new Promise<void>((done) => stream.once("readable", done));
    stream.end(bytes(`${"a".repeat(65536)}{{x}}`));
    await available;
    expect(calls).toBe(0);
    expect((await collect(stream)).length).toBe(65537);
    expect(calls).toBe(1);
  });

  for (const mode of ["sync", "async", "needles", "needle flush"] as const) {
    it(`pauses ${mode} expansion at the readable high-water mark`, async () => {
      let calls = 0;
      const resolve = () => {
        calls++;
        return new Uint8Array(65536).fill(88);
      };
      const options = { open: "{{", close: "}}", resolve };
      const streamOptions = { highWaterMark: 1024 };
      const stream =
        mode === "sync"
          ? createTokenTransform(options, streamOptions)
          : mode === "async"
            ? createAsyncTokenTransform(
                { ...options, resolve: async () => resolve() },
                streamOptions,
              )
            : createNeedleTransform(
                { needles: mode === "needle flush" ? ["a", "aab"] : ["{{x}}"], resolve },
                streamOptions,
              );
      const available = new Promise<void>((done) => stream.once("readable", done));
      stream.end(bytes(mode === "needle flush" ? "aa" : "{{x}}".repeat(100)));
      await available;
      await new Promise((done) => setTimeout(done, 0));
      expect(calls).toBe(1);
      expect(stream.readableLength).toBe(65536);
      let total = 0;
      for await (const part of stream) total += part.length;
      expect(calls).toBe(mode === "needle flush" ? 2 : 100);
      expect(total).toBe(calls * 65536);
    });
  }

  for (const reason of [undefined, null, false, 0, "", "failure"]) {
    for (const mode of ["sync", "async", "needles", "flush"] as const) {
      it(`propagates ${mode} failures with reason ${String(reason)}`, async () => {
        const fail = () => {
          throw reason;
        };
        const options = { open: "{{", close: "}}", resolve: fail };
        const transform =
          mode === "async"
            ? createAsyncTokenTransform({ ...options, resolve: () => Promise.reject(reason) })
            : mode === "needles"
              ? createNeedleTransform({ needles: ["{{x}}"], resolve: fail })
              : createTokenTransform(
                  mode === "flush" ? { ...options, resolve: () => null, onDone: fail } : options,
                );
        await expect(
          pipeline(source(["{{x}}"]), transform, async (output) => {
            for await (const _ of output);
          }),
        ).rejects.toMatchObject({ message: "substitution failed", cause: reason });
      });
    }
  }

  it("substitutes tokens through a pipe", async () => {
    const out = await collect(
      source(["hello {{name}}, ", "you are {{n}}"]).pipe(
        createTokenTransform({ open: "{{", close: "}}", resolve }),
      ),
    );
    expect(out).toBe("hello world, you are 42");
  });

  it("ignores unsupported stream options", async () => {
    const streamOptions = {
      highWaterMark: 1024,
      readableObjectMode: true,
      transform(_chunk: unknown, _encoding: string, callback: () => void) {
        callback();
      },
    };
    const transform = createTokenTransform({ open: "{{", close: "}}", resolve }, streamOptions);
    expect(transform.readableObjectMode).toBe(false);
    expect(await collect(source(["{{name}}"]).pipe(transform))).toBe("world");
  });

  it("carries a token across a chunk boundary", async () => {
    const out = await collect(
      source(["hello {{na", "me}}!"]).pipe(
        createTokenTransform({ open: "{{", close: "}}", resolve }),
      ),
    );
    expect(out).toBe("hello world!");
  });

  it("leaves an unresolved token verbatim", async () => {
    const out = await collect(
      source(["a {{missing}} b"]).pipe(createTokenTransform({ open: "{{", close: "}}", resolve })),
    );
    expect(out).toBe("a {{missing}} b");
  });

  it("substitutes needles", async () => {
    const out = await collect(
      source(["a __ON", "E__ b __TWO__"]).pipe(
        createNeedleTransform({ needles: { __ONE__: "1", __TWO__: "2" } }),
      ),
    );
    expect(out).toBe("a 1 b 2");
  });

  it("awaits an async resolver", async () => {
    const out = await collect(
      source(["hello {{name}}"]).pipe(
        createAsyncTokenTransform({
          open: "{{",
          close: "}}",
          resolve: async (payload) => {
            await Promise.resolve();
            return resolve(payload);
          },
        }),
      ),
    );
    expect(out).toBe("hello world");
  });

  it("fails the stream when the resolver throws", async () => {
    const transform = createTokenTransform({
      open: "{{",
      close: "}}",
      resolve: () => {
        throw new Error("boom");
      },
    });
    await expect(
      pipeline(source(["a {{x}} b"]), transform, async (parts) => {
        for await (const _ of parts);
      }),
    ).rejects.toThrow("boom");
  });

  it("buffers up to highWaterMark before stalling the source", async () => {
    const produce = async (highWaterMark: number) => {
      let produced = 0;
      const source = new Readable({
        read() {
          produced++;
          this.push(Buffer.from("y".repeat(8192)));
        },
      });
      const stream = source.pipe(
        createNeedleTransform({ needles: { __M__: "v" } }, { highWaterMark }),
      );
      const iterator = stream[Symbol.asyncIterator]();
      await iterator.next();
      await new Promise((done) => setTimeout(done, 20));
      await iterator.return?.();
      return produced;
    };

    const small = await produce(16 * 1024);
    const large = await produce(256 * 1024);
    // Bounded either way, and the bigger buffer takes more before it stalls.
    expect(small).toBeLessThan(200);
    expect(large).toBeGreaterThan(small);
  });

  it("destroys the stream when the signal aborts", async () => {
    const controller = new AbortController();
    const source = new Readable({
      read() {
        this.push(Buffer.from("y".repeat(1024)));
      },
    });
    const stream = source.pipe(
      createNeedleTransform({ needles: { __M__: "v" } }, { signal: controller.signal }),
    );

    const drained = (async () => {
      for await (const _ of stream);
    })();
    controller.abort();
    await expect(drained).rejects.toThrow(/abort/i);
  });

  // Duplex.fromWeb turned an abandoned readable into an uncaught TypeError from
  // node:internal/webstreams/adapters, which takes the process down rather than
  // erroring the stream. A client hanging up mid-response is exactly this.
  it("survives being abandoned mid-body", async () => {
    const uncaught: unknown[] = [];
    const onUncaught = (error: unknown) => uncaught.push(error);
    process.on("uncaughtException", onUncaught);
    try {
      const chunks = Array.from({ length: 64 }, () => `${"x".repeat(1000)}{{name}}`);
      const stream = source(chunks).pipe(
        createTokenTransform({ open: "{{", close: "}}", resolve }),
      );

      const iterator = stream[Symbol.asyncIterator]();
      await iterator.next();
      await iterator.return?.();

      // Two macrotasks: the adapter's failure surfaced a tick after teardown.
      await new Promise((done) => setTimeout(done, 10));
      await new Promise((done) => setTimeout(done, 10));
    } finally {
      process.off("uncaughtException", onUncaught);
    }
    expect(uncaught).toEqual([]);
  });

  it("settles a Node write on destroy with its resolver still pending", async () => {
    const gate = deferred<Uint8Array | null>();
    const started = deferred<void>();
    let calls = 0;
    const stream = createAsyncTokenTransform({
      open: "{{",
      close: "}}",
      resolve: () => {
        calls++;
        started.resolve();
        return gate.promise;
      },
    });
    stream.on("error", () => {});
    const written = new Promise<Error | null | undefined>((done) => {
      stream.write(bytes("{{a}}{{b}}"), done);
    });
    await started.promise;
    const closed = new Promise<void>((done) => stream.once("close", done));
    stream.destroy();
    await closed;
    expect(await written).toBeInstanceOf(Error);
    gate.resolve(null);
    await Promise.resolve();
    expect(calls).toBe(1);
  });
});
