// Run: npm run bench:workers
//
// Workers freezes the clock: `performance.now()` only advances on I/O, so a
// wall-clock number measured inside workerd is meaningless and none is reported
// here. What workerd can answer is the shape question, which is the one that
// decides whether this fits in a Worker at all: how many output parts a body
// produces, and whether a body far larger than the isolate's memory budget
// still streams through. Timings live in `npm run bench`, on Node.
import { expect, it } from "vitest";
import { createNeedleTransformStream } from "../src/needles.ts";
import { createTokenTransformStream } from "../src/transformer.ts";

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

it("reports output shape on workerd", async () => {
  const input = makeDocument((i) => (i < 0 ? "{{t0}}" : `{{h${i}}}`), 500);
  const chunks = Math.ceil(input.length / CHUNK_SIZE);

  const buffered = await drain(input, createTokenTransformStream(tokenOptions));
  const unbuffered = await drain(
    input,
    createTokenTransformStream({ ...tokenOptions, flushBytes: 0 }),
  );
  const needles = await drain(
    makeDocument(() => "__ID__", 500),
    createNeedleTransformStream({ needles: { __ID__: "0123456789abcdef" } }),
  );

  console.log(`workerd, ${input.length} bytes, 501 holes, ${chunks} chunks in`);
  console.log(`  tokens,  flushBytes 16384: ${buffered.parts} parts, ${buffered.bytes} bytes out`);
  console.log(
    `  tokens,  flushBytes 0:     ${unbuffered.parts} parts, ${unbuffered.bytes} bytes out`,
  );
  console.log(`  needles, flushBytes 16384: ${needles.parts} parts, ${needles.bytes} bytes out`);

  expect(buffered.parts).toBeLessThan(unbuffered.parts);
});

it("streams a body far larger than the isolate budget", async () => {
  // 32 MB through a 128 MB isolate, with the whole document never resident:
  // the source generates chunks, and carried state is a few hundred bytes.
  const chunk = encoder.encode(`${"x".repeat(CHUNK_SIZE - 8)}{{t0}}..`);
  const total = 2048;
  let produced = 0;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (produced++ >= total) controller.close();
      else controller.enqueue(chunk);
    },
  });

  const reader = body.pipeThrough(createTokenTransformStream(tokenOptions)).getReader();
  let bytes = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    bytes += value.length;
  }
  console.log(
    `  streamed ${(chunk.length * total) / 1e6} MB in, ${(bytes / 1e6).toFixed(1)} MB out`,
  );
  expect(bytes).toBeGreaterThan(30e6);
});
