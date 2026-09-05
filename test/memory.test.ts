import { describe, expect, it } from "vitest";
import {
  createAsyncTokenStreamPair,
  createAsyncTokenTransformer,
} from "../src/async-transformer.ts";
import { createNeedleStreamPair, createNeedleTransformer } from "../src/needles.ts";
import { createTokenTransform } from "../src/node.ts";
import { createTokenStreamPair, createTokenTransformer } from "../src/transformer.ts";
import { bytes, deferred } from "./helpers.ts";

const gc = globalThis.gc;
const ctrl = { enqueue() {} } as unknown as TransformStreamDefaultController<Uint8Array>;

async function expectCollected(ref: WeakRef<object>) {
  // Cross jobs before collecting WeakRef targets.
  for (let i = 0; i < 3; i++) {
    await new Promise((done) => setTimeout(done, 0));
    gc?.();
  }
  expect(ref.deref()).toBeUndefined();
}

describe.skipIf(!gc)("memory lifecycle", () => {
  it("releases a stream-pair signal after completion", async () => {
    const { stream, ref } = (() => {
      const signal = new AbortController().signal;
      return {
        stream: createAsyncTokenStreamPair({
          open: "{{",
          close: "}}",
          signal,
          resolve: () => null,
        }),
        ref: new WeakRef(signal),
      };
    })();
    const reader = stream.readable.getReader();
    const writer = stream.writable.getWriter();
    const done = reader.read();
    await writer.close();
    await done;
    reader.releaseLock();
    writer.releaseLock();
    await expectCollected(ref);
    expect(stream.readable.locked).toBe(false);
  });

  it("releases a Node input chunk when destroyed under backpressure", async () => {
    const stream = createTokenTransform({
      open: "{{",
      close: "}}",
      resolve: () => new Uint8Array(65536),
    });
    stream.on("error", () => {});
    const ref = await (async () => {
      const input = new Uint8Array(4 * 1024 * 1024).fill(120);
      input.set(bytes("{{x}}".repeat(100)));
      const weak = new WeakRef(input.buffer);
      const available = new Promise<void>((done) => stream.once("readable", done));
      stream.write(input, () => {});
      await available;
      const closed = new Promise<void>((done) => stream.once("close", done));
      stream.destroy();
      await closed;
      return weak;
    })();
    await expectCollected(ref);
    expect(stream.destroyed).toBe(true);
  });

  for (const mode of ["sync", "async", "needles"] as const) {
    it(`releases paused ${mode} input on readable cancellation`, async () => {
      const options = { open: "{{", close: "}}", resolve: () => new Uint8Array(65536) };
      const stream =
        mode === "sync"
          ? createTokenStreamPair(options)
          : mode === "async"
            ? createAsyncTokenStreamPair({
                ...options,
                resolve: async () => options.resolve(),
              })
            : createNeedleStreamPair({ needles: ["{{x}}"], resolve: options.resolve });
      const ref = await (async () => {
        const input = new Uint8Array(4 * 1024 * 1024).fill(120);
        input.set(bytes("{{x}}".repeat(100)));
        const weak = new WeakRef(input.buffer);
        const reader = stream.readable.getReader();
        const writer = stream.writable.getWriter();
        const first = reader.read();
        const written = writer.write(input).catch((error) => error);
        await first;
        await reader.cancel("stop");
        await written;
        reader.releaseLock();
        writer.releaseLock();
        return weak;
      })();
      await expectCollected(ref);
      expect(stream.readable.locked).toBe(false);
    });
  }

  it("releases the signal after completion", async () => {
    const { body, ref } = (() => {
      const signal = new AbortController().signal;
      return {
        body: createAsyncTokenTransformer({ open: "{{", close: "}}", signal, resolve: () => null }),
        ref: new WeakRef(signal),
      };
    })();
    body.flush(ctrl);
    await expectCollected(ref);
    expect(body.flush).toBeTypeOf("function");
  });

  it("releases cancelled input while its resolver stays pending", async () => {
    const gate = deferred<Uint8Array | null>();
    const abort = new AbortController();
    const body = createAsyncTokenTransformer({
      open: "{{",
      close: "}}",
      signal: abort.signal,
      resolve: () => gate.promise,
    });
    const ref = await (async () => {
      const input = new Uint8Array(4 * 1024 * 1024).fill(120);
      input.set(bytes("{{x}}"));
      const weak = new WeakRef(input.buffer);
      const pending = body.transform(input, ctrl);
      abort.abort("stop");
      await expect(pending).rejects.toBe("stop");
      return weak;
    })();
    await expectCollected(ref);
    gate.resolve(null);
    expect(body.transform).toBeTypeOf("function");
  });

  for (const mode of ["sync", "async", "needles"] as const) {
    it(`releases ${mode} resolver configuration after flush`, async () => {
      const abort = new AbortController();
      const { body, ref } = (() => {
        const value = new Uint8Array(4 * 1024 * 1024);
        const options = { open: "{{", close: "}}", resolve: () => value };
        const body =
          mode === "sync"
            ? createTokenTransformer(options)
            : mode === "async"
              ? createAsyncTokenTransformer({ ...options, signal: abort.signal })
              : createNeedleTransformer({ needles: ["{{x}}"], resolve: () => value });
        return { body, ref: new WeakRef(value.buffer) };
      })();
      body.flush(ctrl);
      await expectCollected(ref);
      expect(abort.signal.aborted).toBe(false);
      expect(body.flush).toBeTypeOf("function");
    });
  }

  for (const mode of ["sync", "async", "needles", "validator", "enqueue"] as const) {
    it(`releases input after a ${mode} failure with the transformer still alive`, async () => {
      const failure = new Error("intentional failure");
      const fail = () => {
        throw failure;
      };
      const body =
        mode === "async"
          ? createAsyncTokenTransformer({ open: "{{", close: "}}", resolve: async () => fail() })
          : mode === "needles"
            ? createNeedleTransformer({ needles: ["{{x}}"], resolve: fail })
            : createTokenTransformer({
                open: "{{",
                close: "}}",
                resolve: fail,
                ...(mode === "validator" ? { validate: fail } : {}),
              });
      const ref = await (async () => {
        const input = new Uint8Array(4 * 1024 * 1024).fill(120);
        if (mode !== "enqueue") input.set(bytes("a{{x}}"));
        const weak = new WeakRef(input.buffer);
        const controller =
          mode === "enqueue" ? ({ enqueue: fail } as unknown as typeof ctrl) : ctrl;
        await expect(Promise.resolve().then(() => body.transform(input, controller))).rejects.toBe(
          failure,
        );
        return weak;
      })();
      await expectCollected(ref);
      expect(body.transform).toBeTypeOf("function");
    });
  }

  it("does not retain a caller's controller between chunks", async () => {
    const body = createTokenTransformer({ open: "{{", close: "}}", resolve: () => null });
    const ref = (() => {
      const owner = new Uint8Array(4 * 1024 * 1024);
      const controller = {
        enqueue() {
          expect(owner.length).toBeGreaterThan(0);
        },
      } as unknown as typeof ctrl;
      body.transform(bytes("plain content"), controller);
      return new WeakRef(owner.buffer);
    })();
    await expectCollected(ref);
    body.flush(ctrl);
  });

  for (const failFlush of [false, true]) {
    it(`releases grown payload scratch after ${failFlush ? "failed" : "successful"} flush`, async () => {
      let ref: WeakRef<object> | undefined;
      const body = createTokenTransformer({
        open: "{{",
        close: "}}",
        maxPayloadBytes: 1024 * 1024,
        validate: (payload) => {
          ref = new WeakRef(payload.buffer);
          return true;
        },
        resolve: () => null,
      });
      body.transform(bytes(`{{${"x".repeat(1024 * 1024)}`), ctrl);
      const failure = new Error("flush failed");
      if (failFlush) {
        expect(() =>
          body.flush({
            enqueue() {
              throw failure;
            },
          } as unknown as typeof ctrl),
        ).toThrow(failure);
      } else body.flush(ctrl);
      expect(ref).toBeDefined();
      await expectCollected(ref as WeakRef<object>);
      expect(body.flush).toBeTypeOf("function");
    });
  }

  it("releases needle bridge scratch on flush", async () => {
    let ref: WeakRef<object> | undefined;
    const body = createNeedleTransformer({
      needles: ["a".repeat(4096)],
      resolve: (needle) => {
        ref = new WeakRef(needle.buffer);
        return null;
      },
    });
    body.transform(bytes("a".repeat(2048)), ctrl);
    body.transform(bytes("a".repeat(2048)), ctrl);
    body.flush(ctrl);
    expect(ref).toBeDefined();
    await expectCollected(ref as WeakRef<object>);
    expect(body.flush).toBeTypeOf("function");
  });

  it("releases the resolver when a Node stream is destroyed", async () => {
    const { stream, ref } = (() => {
      const value = new Uint8Array(4 * 1024 * 1024);
      return {
        stream: createTokenTransform({ open: "{{", close: "}}", resolve: () => value }),
        ref: new WeakRef(value.buffer),
      };
    })();
    await new Promise<void>((done) => {
      stream.once("close", done);
      stream.destroy();
    });
    await expectCollected(ref);
    expect(stream.destroyed).toBe(true);
  });
});
