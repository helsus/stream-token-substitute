// Run: npm run bench
// Answers two questions: is streaming worth it, and what should flushBytes be.
import { barplot, bench, run, summary } from "mitata";
import { resolveFrom } from "../src/helpers.ts";
import { createTokenTransformStream } from "../src/transformer.ts";
import type { TokenTransformOptions } from "../src/types.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const bytes = (text: string) => encoder.encode(text);

const CHUNK_SIZE = 16 * 1024;
const TOKENS = 500;

/** ~200 KB of HTML with 501 holes. */
function makeDocument(): Uint8Array {
  const parts = [`<!doctype html><html><head><title>{{t0}}</title></head><body>`];
  const filler = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(6);
  for (let i = 0; i < TOKENS; i++) {
    parts.push(`<section id="s${i}"><h2>{{h${i}}}</h2><p>${filler}</p></section>`);
  }
  parts.push("</body></html>");
  return bytes(parts.join(""));
}

const values = new Map<string, Uint8Array>();
for (let i = 0; i < TOKENS; i++) values.set(`h${i}`, bytes(`Heading number ${i}`));
values.set("t0", bytes("Benchmark document"));

const options: TokenTransformOptions = {
  open: "{{",
  close: "}}",
  resolve: resolveFrom(values),
};

const input = makeDocument();

function cut(size: number): Uint8Array[] {
  const parts: Uint8Array[] = [];
  for (let i = 0; i < input.length; i += size) parts.push(input.subarray(i, i + size));
  return parts;
}

const chunks = cut(CHUNK_SIZE);

function source(parts: Uint8Array[] = chunks): ReadableStream<Uint8Array> {
  let i = 0;
  return new ReadableStream({
    pull(controller) {
      if (i < parts.length) controller.enqueue(parts[i++]);
      else controller.close();
    },
  });
}

/** Returns the number of enqueued output parts: one object plus one read() and
 *  one microtask each, which is where "many small allocations" actually hurts. */
async function drain(flushBytes: number, feed?: Uint8Array[]): Promise<number> {
  const reader = source(feed)
    .pipeThrough(createTokenTransformStream({ ...options, flushBytes }))
    .getReader();
  let parts = 0;
  for (;;) {
    const { done } = await reader.read();
    if (done) return parts;
    parts++;
  }
}

/** Buffer the whole body, decode, String.replace, re-encode. */
function stringReplace(): number {
  const text = decoder.decode(input).replace(/\{\{([^}]*)\}\}/g, (whole, key: string) => {
    const value = values.get(key);
    return value === undefined ? whole : decoder.decode(value);
  });
  return encoder.encode(text).length;
}

/** Median time to the first output byte. mitata times whole calls, so this is
 *  measured separately: it is the number streaming exists to improve. */
async function ttfb(start: () => Promise<unknown>, runs = 50): Promise<number> {
  const samples: number[] = [];
  for (let i = 0; i < runs; i++) {
    const began = performance.now();
    await start();
    samples.push(performance.now() - began);
  }
  samples.sort((a, b) => a - b);
  return samples[samples.length >> 1];
}

console.log(`node ${process.version} on ${process.platform}/${process.arch}`);
console.log(
  `${input.length} bytes, ${TOKENS + 1} tokens, ${chunks.length} chunks of ${CHUNK_SIZE}`,
);

// Smaller chunks put more tokens across a boundary, which is what the carried
// scan state costs.
const SIZES = [1024, 64, 8];
const cuts = new Map(SIZES.map((size) => [size, cut(size)]));
const sizeLabel = (size: number) => `stream, ${size}B chunks`;

barplot(() => {
  summary(() => {
    bench("buffer + String.replace", () => stringReplace()).gc("inner");
    bench("stream, flushBytes 0", () => drain(0)).gc("inner");
    bench("stream, flushBytes 16384", () => drain(16384)).gc("inner");
    for (const size of SIZES) {
      bench(sizeLabel(size), () => drain(16384, cuts.get(size) as Uint8Array[])).gc("inner");
    }
  });
});

const stats = await run();

const firstByte = async (flushBytes: number, feed?: Uint8Array[]) => {
  const reader = source(feed)
    .pipeThrough(createTokenTransformStream({ ...options, flushBytes }))
    .getReader();
  await reader.read();
  await reader.cancel();
};

const ttfbOf = new Map([
  // The buffered baseline emits nothing until the whole body is done, so its
  // time to first byte is its total time.
  ["buffer + String.replace", await ttfb(async () => stringReplace())],
  ["stream, flushBytes 0", await ttfb(() => firstByte(0))],
  ["stream, flushBytes 16384", await ttfb(() => firstByte(16384))],
]);

const partsOf = new Map([
  ["buffer + String.replace", 1],
  ["stream, flushBytes 0", await drain(0)],
  ["stream, flushBytes 16384", await drain(16384)],
]);

for (const size of SIZES) {
  const parts = cuts.get(size) as Uint8Array[];
  ttfbOf.set(sizeLabel(size), await ttfb(() => firstByte(16384, parts)));
  partsOf.set(sizeLabel(size), await drain(16384, parts));
}

const round = (value: number, places: number) => Number(value.toFixed(places));

console.log("");
console.table(
  stats.benchmarks.map((trial) => {
    const run = trial.runs[0]?.stats;
    const median = run?.p50 ?? 0;
    const alloc = run?.heap?.avg ?? 0;
    return {
      benchmark: trial.alias,
      "total ms": round(median / 1e6, 2),
      "MB/s": Math.round(input.length / 1e6 / (median / 1e9)),
      "ttfb ms": round(ttfbOf.get(trial.alias) ?? 0, 3),
      "alloc kB": round(alloc / 1024, 1),
      "alloc % of input": Math.round((alloc / input.length) * 100),
      "out parts": partsOf.get(trial.alias) ?? 0,
    };
  }),
);
