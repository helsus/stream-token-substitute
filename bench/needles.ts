// Run: npm run bench:needles
// Answers how much the chunk boundary costs: the same body, cut into chunks
// large enough that boundaries are rare, then small enough that most needles
// straddle one.
import { barplot, bench, run, summary } from "mitata";
import { compileNeedles, createNeedleTransformStream } from "../src/needles.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const TOKENS = 500;

/** ~200 KB of HTML with 501 holes. */
function makeDocument(): Uint8Array {
  const parts = [`<!doctype html><html><head><title>{{t0}}</title></head><body>`];
  const filler = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(6);
  for (let i = 0; i < TOKENS; i++) {
    parts.push(`<section id="s${i}"><h2>{{h${i}}}</h2><p>${filler}</p></section>`);
  }
  parts.push("</body></html>");
  return encoder.encode(parts.join(""));
}

const table: Record<string, string> = { "{{t0}}": "Benchmark document" };
for (let i = 0; i < TOKENS; i++) table[`{{h${i}}}`] = `Heading number ${i}`;
const needles = compileNeedles(table);

const input = makeDocument();

function cut(size: number): Uint8Array[] {
  const chunks: Uint8Array[] = [];
  for (let i = 0; i < input.length; i += size) chunks.push(input.subarray(i, i + size));
  return chunks;
}

async function drain(chunks: Uint8Array[]): Promise<number> {
  let i = 0;
  const reader = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (i < chunks.length) controller.enqueue(chunks[i++]);
      else controller.close();
    },
  })
    .pipeThrough(createNeedleTransformStream({ needles }))
    .getReader();
  let bytes = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) return bytes;
    bytes += value.length;
  }
}

/** Buffer the whole body and String.replaceAll once per needle. */
function stringReplace(): number {
  let text = decoder.decode(input);
  for (const [needle, value] of Object.entries(table)) text = text.replaceAll(needle, value);
  return encoder.encode(text).length;
}

const sizes = [16384, 1024, 64, 8];
const cuts = new Map(sizes.map((size) => [size, cut(size)]));

console.log(`node ${process.version} on ${process.platform}/${process.arch}`);
console.log(`${input.length} bytes, ${TOKENS + 1} needles`);

barplot(() => {
  summary(() => {
    bench("buffer + String.replaceAll", () => stringReplace()).gc("inner");
    for (const size of sizes) {
      bench(`stream, ${size}B chunks`, () => drain(cuts.get(size) as Uint8Array[])).gc("inner");
    }
  });
});

const stats = await run();

const round = (value: number, places: number) => Number(value.toFixed(places));

console.log("");
console.table(
  stats.benchmarks.map((trial) => {
    const median = trial.runs[0]?.stats?.p50 ?? 0;
    const alloc = trial.runs[0]?.stats?.heap?.avg ?? 0;
    return {
      benchmark: trial.alias,
      "total ms": round(median / 1e6, 2),
      "MB/s": Math.round(input.length / 1e6 / (median / 1e9)),
      "alloc kB": round(alloc / 1024, 1),
      "alloc % of input": Math.round((alloc / input.length) * 100),
    };
  }),
);
