// Run: vitest run -c vitest.workers.config.ts
import { expect, it } from "vitest";
import { createNeedleTransformStream } from "../src/needles.ts";
import { createTokenTransformStream } from "../src/transformer.ts";
import { CHECKS } from "../test/cross-runtime.ts";

it("really is running inside workerd", () => {
  expect(navigator.userAgent).toBe("Cloudflare-Workers");
});

for (const check of CHECKS) {
  it(check.name, check.run);
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const CHUNK_SIZE = 16 * 1024;

function makeDocument(hole: (index: number) => string, holes: number): Uint8Array {
  const parts = [`<!doctype html><html><head><title>${hole(-1)}</title></head><body>`];
  const filler = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(6);
  for (let i = 0; i < holes; i++) {
    parts.push(`<section id="s${i}"><h2>${hole(i)}</h2><p>${filler}</p></section>`);
  }
  parts.push("</body></html>");
  return encoder.encode(parts.join(""));
}

function source(input: Uint8Array): ReadableStream<Uint8Array> {
  let i = 0;
  return new ReadableStream({
    pull(controller) {
      if (i >= input.length) controller.close();
      else {
        controller.enqueue(input.subarray(i, i + CHUNK_SIZE));
        i += CHUNK_SIZE;
      }
    },
  });
}

async function drain(
  input: Uint8Array,
  tx: TransformStream<Uint8Array, Uint8Array>,
): Promise<{ parts: number; bytes: number }> {
  const reader = source(input).pipeThrough(tx).getReader();
  let parts = 0;
  let bytes = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) return { parts, bytes };
    parts++;
    bytes += value.length;
  }
}

const values = new Map<string, Uint8Array>();
for (let i = 0; i < 500; i++) values.set(`h${i}`, encoder.encode(`Heading number ${i}`));
values.set("t0", encoder.encode("Benchmark document"));

const tokenOptions = {
  open: "{{",
  close: "}}",
  resolve: (payload: Uint8Array) => values.get(decoder.decode(payload)) ?? null,
};

it("batches output without changing its size", async () => {
  const input = makeDocument((i) => (i < 0 ? "{{t0}}" : `{{h${i}}}`), 500);

  const buffered = await drain(input, createTokenTransformStream(tokenOptions));
  const unbuffered = await drain(
    input,
    createTokenTransformStream({ ...tokenOptions, flushBytes: 0 }),
  );
  const needles = await drain(
    makeDocument(() => "__ID__", 500),
    createNeedleTransformStream({ needles: { __ID__: "0123456789abcdef" } }),
  );

  expect(buffered.bytes).toBe(unbuffered.bytes);
  expect(needles.bytes).toBe(201475);
  expect(buffered.parts).toBeLessThan(unbuffered.parts);
});

for (const mode of ["tokens", "needles"] as const) {
  it(`streams 256 MiB through workerd in ${mode} mode`, async () => {
    // Fresh chunks exercise buffer lifetimes.
    const chunk = encoder.encode(`${"x".repeat(CHUNK_SIZE - 8)}{{t0}}..`);
    const total = 16384;
    let produced = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (produced++ >= total) controller.close();
        else controller.enqueue(chunk.slice());
      },
    });

    const transform =
      mode === "tokens"
        ? createTokenTransformStream(tokenOptions)
        : createNeedleTransformStream({ needles: { "{{t0}}": "Benchmark document" } });
    const reader = body.pipeThrough(transform).getReader();
    let bytes = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      bytes += value.length;
    }
    expect(bytes).toBe(total * (chunk.length + "Benchmark document".length - "{{t0}}".length));
  }, 120_000);
}
