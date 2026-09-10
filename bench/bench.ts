// Run: npm run bench
import assert from "node:assert/strict";
import { PassThrough, Readable } from "node:stream";
import replacestream from "replacestream";
import { createAsyncTokenStreamPair } from "../src/async-transformer.ts";
import { resolveFrom } from "../src/helpers.ts";
import {
  compileNeedles,
  createNeedleStreamPair,
  createNeedleTransformer,
  createNeedleTransformStream,
} from "../src/needles.ts";
import {
  createAsyncTokenTransform,
  createNeedleTransform,
  createTokenTransform,
} from "../src/node.ts";
import {
  createTokenStreamPair,
  createTokenTransformer,
  createTokenTransformStream,
} from "../src/transformer.ts";
import type { TokenTransformOptions } from "../src/types.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const bytes = (text: string) => encoder.encode(text);

const CHUNK_SIZES = [16 * 1024, 1024, 64];
const MAIN_CHUNK = 16 * 1024;
const SECTIONS = 5000;
const WARMUP = 50;
const SAMPLES = 41;

const PROSE = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(6);
/** Single braces: near-misses for a `{{` open, and what real markup looks like. */
const BRACES = ".a{color:red}.b{margin:0}.c{padding:0}".repeat(9);

/** ~2 MB of HTML, one heading per section. Big enough that the differences are
 *  not measurement noise. */
function makeDocument(heading: (i: number) => string, filler = PROSE): Uint8Array {
  const parts = [`<!doctype html><html><head><title>${heading(SECTIONS)}</title></head><body>`];
  for (let i = 0; i < SECTIONS; i++) {
    parts.push(`<section id="s${i}"><h2>${heading(i)}</h2><p>${filler}</p></section>`);
  }
  parts.push("</body></html>");
  return bytes(parts.join(""));
}

function cut(input: Uint8Array, size: number): Uint8Array[] {
  const parts: Uint8Array[] = [];
  for (let i = 0; i < input.length; i += size) parts.push(input.subarray(i, i + size));
  return parts;
}

type Open = (chunks: Uint8Array[]) => AsyncIterable<Uint8Array | string>;

/** Release the whole pipeline after a first-output sample. */
async function* nodeOut(
  chunks: Uint8Array[],
  stream: NodeJS.ReadWriteStream & { destroy(): unknown },
): AsyncIterable<Uint8Array> {
  const upstream = Readable.from(
    chunks.map((part) => Buffer.from(part.buffer, part.byteOffset, part.length)),
  );
  const output = new PassThrough();
  const onError = (error: Error) => output.destroy(error);
  upstream.on("error", onError);
  stream.on("error", onError);
  upstream.pipe(stream).pipe(output);
  try {
    for await (const part of output) yield part;
  } finally {
    upstream.destroy();
    stream.destroy();
    output.destroy();
  }
}

function webSource(parts: Uint8Array[]): ReadableStream<Uint8Array> {
  let i = 0;
  return new ReadableStream({
    pull(controller) {
      if (i < parts.length) controller.enqueue(parts[i++] as Uint8Array);
      else controller.close();
    },
  });
}

async function collect(iterable: AsyncIterable<Uint8Array | string>) {
  const parts: Uint8Array[] = [];
  let total = 0;
  for await (const part of iterable) {
    const view = typeof part === "string" ? bytes(part) : part;
    parts.push(view);
    total += view.length;
  }
  const out = new Uint8Array(total);
  let at = 0;
  for (const part of parts) {
    out.set(part, at);
    at += part.length;
  }
  return { out, parts: parts.length };
}

async function drain(iterable: AsyncIterable<unknown>): Promise<void> {
  for await (const _ of iterable);
}

const same = (a: Uint8Array, b: Uint8Array) =>
  a.length === b.length && a.every((byte, i) => byte === b[i]);

async function median(open: () => AsyncIterable<unknown>): Promise<number> {
  for (let i = 0; i < WARMUP; i++) await drain(open());
  const samples: number[] = [];
  for (let i = 0; i < SAMPLES; i++) {
    const began = performance.now();
    await drain(open());
    samples.push(performance.now() - began);
  }
  samples.sort((a, b) => a - b);
  return samples[samples.length >> 1] as number;
}

/** Median time to the first emitted part. */
async function ttfb(open: () => AsyncIterable<unknown>): Promise<number> {
  const samples: number[] = [];
  for (let i = 0; i < 30; i++) {
    const began = performance.now();
    const iterator = open()[Symbol.asyncIterator]();
    await iterator.next();
    samples.push(performance.now() - began);
    await iterator.return?.();
  }
  samples.sort((a, b) => a - b);
  return samples[samples.length >> 1] as number;
}

// Delimited `{{key}}` grammar, shared by every token scenario.

const templateDoc = makeDocument((i) => `{{h${i}}}`);

const valueText = new Map<string, string>();
for (let i = 0; i <= SECTIONS; i++) {
  valueText.set(`h${i}`, `Heading number ${i}`);
}

valueText.set("big", "x".repeat(256 * 1024));

const tokenOptions = {
  open: "{{",
  close: "}}",
  resolve: resolveFrom(valueText),
};

const substituteTemplate = (text: string) =>
  text.replace(/\{\{([^}]*)\}\}/g, (whole, key: string) => valueText.get(key) ?? whole);

const SET_SIZE = 32;
const needleNames = Array.from({ length: SET_SIZE }, (_, i) => `__MARKER_${i}__`);
const needleValues = Object.fromEntries(
  needleNames.map((name, i) => [name, `value ${i}`]),
) as Record<string, string>;

const needleDoc = makeDocument((i) => needleNames[i % SET_SIZE] as string);

type Contender = { name: string; open: Open; note: string };

type Scenario = {
  title: string;
  doc: Uint8Array;
  expected: Uint8Array;
  contenders: Contender[];
};

async function* once(value: Uint8Array) {
  yield value;
}

/** The same four contenders over whatever document is passed in. */
function tokenScenario(title: string, doc: Uint8Array): Scenario {
  const expected = bytes(substituteTemplate(decoder.decode(doc)));
  return {
    title,
    doc,
    expected,
    contenders: [
      {
        // Decode and encode included: that is what buffering a byte body costs.
        name: "buffer + String.replace",
        open: () => once(bytes(substituteTemplate(decoder.decode(doc)))),
        note: "baseline, holds the body",
      },
      {
        name: "stream-token-substitute",
        open: (chunks) =>
          webSource(chunks).pipeThrough(
            createTokenTransformStream(tokenOptions),
          ) as unknown as AsyncIterable<Uint8Array>,
        note: "web streams",
      },
      {
        name: "stream-token-substitute/node",
        open: (chunks) => nodeOut(chunks, createTokenTransform(tokenOptions)),
        note: "node streams",
      },
      {
        name: "replacestream",
        open: (chunks) =>
          nodeOut(
            chunks,
            replacestream(
              /\{\{([^}]*)\}\}/g,
              (whole: string, key: string) => valueText.get(key) ?? whole,
            ),
          ),
        note: "node streams",
      },
    ],
  };
}

function needleScenario(title: string, doc: Uint8Array, needles: Record<string, string>): Scenario {
  const compiled = compileNeedles(needles);
  // Longest alternative first: a JS alternation is first-match.
  const pattern = Object.keys(needles)
    .sort((a, b) => b.length - a.length)
    .join("|");
  const count = Object.keys(needles).length;
  const substitute = (text: string) => {
    let out = text;
    for (const [name, value] of Object.entries(needles)) out = out.replaceAll(name, value);
    return out;
  };
  const expected = bytes(substitute(decoder.decode(doc)));
  return {
    title,
    doc,
    expected,
    contenders: [
      {
        name: "buffer + single regex",
        open: () =>
          once(
            bytes(decoder.decode(doc).replace(new RegExp(pattern, "g"), (match) => needles[match])),
          ),
        note: "one regex pass, decode and encode included",
      },
      {
        name: `buffer + ${count}x String.replaceAll`,
        open: () => once(bytes(substitute(decoder.decode(doc)))),
        note: "baseline, holds the body",
      },
      {
        name: "stream-token-substitute/needles",
        open: (chunks) =>
          webSource(chunks).pipeThrough(
            createNeedleTransformStream({ needles }),
          ) as unknown as AsyncIterable<Uint8Array>,
        note: "web streams, Aho-Corasick",
      },
      {
        name: "stream-token-substitute/node",
        open: (chunks) => nodeOut(chunks, createNeedleTransform({ needles })),
        note: "node streams, Aho-Corasick",
      },
      {
        name: "stream-token-substitute/needles (compiled)",
        open: (chunks) =>
          webSource(chunks).pipeThrough(
            createNeedleTransformStream({ needles: compiled }),
          ) as unknown as AsyncIterable<Uint8Array>,
        note: "reused automaton",
      },
      {
        name: "replacestream",
        open: (chunks) =>
          nodeOut(
            chunks,
            replacestream(
              new RegExp(`(?:${pattern})`, "g"),
              (match: string) => needles[match] ?? match,
            ),
          ),
        note: "one alternation regex",
      },
    ],
  };
}

/** Same document and same answers, to price the async scanner against the sync
 *  one: a resolver that answers directly never suspends, one that returns a
 *  promise suspends and resumes per token. replacestream has no async mode. */
function asyncScenario(): Scenario {
  const direct = { ...tokenOptions };
  const awaited = {
    ...tokenOptions,
    resolve: (payload: Uint8Array) => Promise.resolve(tokenOptions.resolve(payload)),
  };
  return {
    title: "async resolver, 5001 holes",
    doc: templateDoc,
    expected: bytes(substituteTemplate(decoder.decode(templateDoc))),
    contenders: [
      {
        name: "stream-token-substitute/node",
        open: (chunks) => nodeOut(chunks, createTokenTransform(tokenOptions)),
        note: "sync scanner, for reference",
      },
      {
        name: "stream-token-substitute/node (async)",
        open: (chunks) => nodeOut(chunks, createAsyncTokenTransform(direct)),
        note: "resolver answers directly",
      },
      {
        name: "stream-token-substitute/node (async)",
        open: (chunks) => nodeOut(chunks, createAsyncTokenTransform(awaited)),
        note: "resolver returns a promise",
      },
    ],
  };
}

const heading = (i: number) => `Section ${i}`;
const sparse = (i: number) => (i < 3 ? `{{h${i}}}` : heading(i));

const scenarios: Scenario[] = [
  tokenScenario("dense template, 5001 holes", templateDoc),
  tokenScenario("sparse shell, 3 holes", makeDocument(sparse)),
  tokenScenario("no tokens at all", makeDocument(heading)),
  tokenScenario("inline CSS braces, 3 holes", makeDocument(sparse, BRACES)),
  tokenScenario(
    "one 256 KB replacement",
    makeDocument((i) => (i === 0 ? "{{big}}" : heading(i))),
  ),
  needleScenario(
    "one literal marker",
    makeDocument((i) => (i < 3 ? "__NONCE__" : heading(i))),
    {
      __NONCE__: "r4nd0m",
    },
  ),
  needleScenario("32 literal markers", needleDoc, needleValues),
  asyncScenario(),
];

const round = (value: number, places: number) => Number(value.toFixed(places));

const runtime = process.versions.bun ? `bun ${process.versions.bun}` : `node ${process.version}`;
console.log(`${runtime} on ${process.platform}/${process.arch}`);
console.log(`Timing: warmup ${WARMUP}, median of ${SAMPLES}, no forced GC`);
console.log("TTFB is first output with ready input, excluding network latency.");

for (const scenario of scenarios) {
  console.log(`\n## ${scenario.title}  (${scenario.doc.length} bytes)\n`);

  const wrong: string[] = [];
  for (const contender of scenario.contenders) {
    for (const size of CHUNK_SIZES) {
      const { out } = await collect(contender.open(cut(scenario.doc, size)));
      if (!same(out, scenario.expected)) wrong.push(`${contender.name} @ ${size}B`);
    }
  }
  console.log(
    wrong.length === 0
      ? "all outputs match String.replace\n"
      : `disagrees with String.replace: ${wrong.join(", ")}\n`,
  );
  if (wrong.length > 0) throw new Error(`incorrect output: ${wrong.join(", ")}`);

  const rows = [];
  const feed = cut(scenario.doc, MAIN_CHUNK);
  for (const contender of scenario.contenders) {
    const total = await median(() => contender.open(feed));
    rows.push({
      library: contender.name,
      "total ms": round(total, 2),
      "MB/s": Math.round(scenario.doc.length / 1e6 / (total / 1e3)),
      "ttfb ms": round(await ttfb(() => contender.open(feed)), 3),
      "out parts": (await collect(contender.open(feed))).parts,
      note: contender.note,
    });
  }
  console.table(rows);
}

// One scenario across chunk sizes: the rest of the field moves the same way.
const sweep = scenarios[1] as Scenario;
console.log(`\n## ${sweep.title}, by chunk size\n`);

const sweepRows = [];
for (const contender of sweep.contenders) {
  const row: Record<string, string | number> = { library: contender.name };
  for (const size of CHUNK_SIZES) {
    const feed = cut(sweep.doc, size);
    row[`${size}B ms`] = round(await median(() => contender.open(feed)), 2);
  }
  sweepRows.push(row);
}
console.table(sweepRows);

// A text-mode replacer decodes each chunk alone, so a split character is lost.
console.log("\n## UTF-8 character split across a chunk boundary\n");

const utf8Doc = bytes("h\u00e9llo {{h0}} w\u00f6rld");
const utf8Expected = bytes(substituteTemplate("h\u00e9llo {{h0}} w\u00f6rld"));
const utf8Chunks = [utf8Doc.subarray(0, 2), utf8Doc.subarray(2)];

const utf8Rows = [];
for (const contender of (scenarios[0] as Scenario).contenders.filter(
  (entry) => !entry.name.startsWith("buffer"),
)) {
  const { out } = await collect(contender.open(utf8Chunks));
  utf8Rows.push({
    library: contender.name,
    survives: same(out, utf8Expected),
    "output hex": Buffer.from(out).toString("hex"),
  });
}
console.table(utf8Rows);

function scannerBenchmark() {
  const replacement = bytes("X");
  const options = { open: "{{", close: "}}", resolve: () => replacement };
  type Body = ReturnType<typeof createTokenTransformer>;

  function measure(
    name: string,
    input: Uint8Array,
    expected: Uint8Array,
    factory: () => Body,
    chunkSize = name === "payload cap 1024" ? input.length : 16384,
  ) {
    const chunks: Uint8Array[] = [];
    // A large chunk exposes searches that accidentally scan past the payload cap.
    for (let i = 0; i < input.length; i += chunkSize) chunks.push(input.subarray(i, i + chunkSize));
    let count = 0;
    const ctrl = {
      enqueue: (part: Uint8Array) => (count += part.length),
    } as unknown as TransformStreamDefaultController<Uint8Array>;
    const check: Uint8Array[] = [];
    const checkCtrl = {
      enqueue: (part: Uint8Array) => check.push(part),
    } as unknown as TransformStreamDefaultController<Uint8Array>;
    const body = factory();
    for (const chunk of chunks) body.transform(chunk, checkCtrl);
    body.flush(checkCtrl);
    assert.deepEqual(Buffer.concat(check), Buffer.from(expected));
    const run = () => {
      count = 0;
      const body = factory();
      for (const chunk of chunks) body.transform(chunk, ctrl);
      body.flush(ctrl);
      assert.equal(count, expected.length);
    };
    for (let i = 0; i < 10; i++) run();
    const samples: number[] = [];
    for (let sample = 0; sample < 21; sample++) {
      const started = performance.now();
      run();
      samples.push(performance.now() - started);
    }
    samples.sort((a, b) => a - b);
    console.log(`${name}: ${samples[10].toFixed(3)} ms (${input.length} bytes)`);
  }

  function token(
    name: string,
    input: string,
    expected: string,
    extra: Partial<TokenTransformOptions> = {},
  ) {
    measure(name, bytes(input), bytes(expected), () =>
      createTokenTransformer({ ...options, ...extra }),
    );
  }

  console.log("\n## Scanner cases, milliseconds");
  token("dense tokens", "text {{a}} ".repeat(20000), "text X ".repeat(20000));
  token("sparse tokens", `${"text ".repeat(200000)}{{a}}`, `${"text ".repeat(200000)}X`);
  token(
    "CSS near misses",
    `${".a{color:red}".repeat(60000)}{{a}}`,
    `${".a{color:red}".repeat(60000)}X`,
  );
  const overlap = `${"a".repeat(1023)}b`;
  token("overlapping delimiter", `${"a".repeat(200000)}bx}}`, `${"a".repeat(200000 - 1023)}X`, {
    open: overlap,
  });
  const capped = `{{${"x".repeat(1025)}`.repeat(500);
  token("payload cap 1024", capped, capped, { maxPayloadBytes: 1024 });
  const needles = compileNeedles({ __A__: "X", __B__: "Y" });
  measure("dense needles", bytes("__A____B__".repeat(20000)), bytes("XY".repeat(20000)), () =>
    createNeedleTransformer({ needles }),
  );
  for (const length of [256, 1024]) {
    const overlapping = compileNeedles({ a: "X", [`${"a".repeat(length)}b`]: "Y" });
    measure(`needle overlap ${length}`, bytes("a".repeat(8192)), bytes("X".repeat(8192)), () =>
      createNeedleTransformer({ needles: overlapping }),
    );
  }
  const longOverlap = compileNeedles({ a: "X", [`${"a".repeat(16000)}b`]: "Y" });
  for (const size of [32000, 1]) {
    measure(
      `needle overlap 16000, ${size}B chunks`,
      bytes("a".repeat(32000)),
      bytes("X".repeat(32000)),
      () => createNeedleTransformer({ needles: longOverlap }),
      size,
    );
  }
}

async function delayedInputBenchmark() {
  const needles = compileNeedles({ q: "X", abcdef: "Y" });
  const samples: number[] = [];
  for (let run = 0; run < 15; run++) {
    let sent = false;
    const source = new ReadableStream<Uint8Array>({
      async pull(ctrl) {
        if (!sent) {
          sent = true;
          ctrl.enqueue(bytes("q"));
        } else {
          await new Promise((done) => setTimeout(done, 30));
          ctrl.close();
        }
      },
    });
    const start = performance.now();
    const reader = source.pipeThrough(createNeedleTransformStream({ needles })).getReader();
    const first = await reader.read();
    samples.push(performance.now() - start);
    assert.equal(decoder.decode(first.value), "X");
    while (!(await reader.read()).done);
    reader.releaseLock();
  }
  samples.sort((a, b) => a - b);
  console.log(`\nDecided needle, 30 ms upstream gap: ${samples[7].toFixed(3)} ms first output`);
}

async function memoryBenchmark() {
  const gc = globalThis.gc;
  if (!gc) {
    console.log("Memory skipped: run with Node --expose-gc.");
    return;
  }
  console.log("\n## Memory: stream pairs, 128 MiB streams, sampled process deltas in bytes");
  console.log("Sampled peaks are not exact; negative retained deltas reflect other GC cleanup.");
  const rows: object[] = [];
  async function collect() {
    for (let i = 0; i < 3; i++) {
      await new Promise((done) => setTimeout(done, 0));
      gc?.();
    }
  }
  for (const mode of ["sync", "async", "needles"] as const) {
    const template = bytes(`${"x".repeat(59)}{{x}}`.repeat(1024));
    const replacement = bytes("X");
    const options = { open: "{{", close: "}}", resolve: () => replacement };
    const transformer =
      mode === "sync"
        ? createTokenStreamPair(options)
        : mode === "async"
          ? createAsyncTokenStreamPair({ ...options, resolve: async () => replacement })
          : createNeedleStreamPair({ needles: { "{{x}}": replacement } });
    const total = 2048; // 128 MiB, generated one 64 KiB chunk at a time.
    let sent = 0;
    let output = 0;
    let parts = 0;
    await collect();
    const baseline = process.memoryUsage();
    let peakHeap = baseline.heapUsed;
    let peakBuffers = baseline.arrayBuffers;
    const sample = () => {
      const current = process.memoryUsage();
      peakHeap = Math.max(peakHeap, current.heapUsed);
      peakBuffers = Math.max(peakBuffers, current.arrayBuffers);
    };
    await new ReadableStream<Uint8Array>({
      pull(controller) {
        if (sent++ < total) controller.enqueue(template.slice());
        else controller.close();
      },
    })
      .pipeThrough(transformer)
      .pipeTo(
        new WritableStream<Uint8Array>({
          write(part) {
            output += part.length;
            if (++parts % 64 === 0) sample();
          },
        }),
      );
    sample();
    if (output !== total * 60 * 1024) throw new Error("incorrect output size");
    await collect();
    const after = process.memoryUsage();
    rows.push({
      mode,
      inputMiB: 128,
      peakHeapDelta: peakHeap - baseline.heapUsed,
      peakArrayBufferDelta: peakBuffers - baseline.arrayBuffers,
      retainedHeapDelta: after.heapUsed - baseline.heapUsed,
      retainedArrayBufferDelta: after.arrayBuffers - baseline.arrayBuffers,
    });
  }

  console.table(rows);
}

function compilationBenchmark() {
  console.log("\n## Needle compilation, median of 11 runs");
  console.log("Memory is sampled allocation growth, not an exact peak.");
  const rows = [];
  for (const [name, source] of [
    ["32 markers", needleNames],
    ["100K-byte needle", ["a".repeat(100000)]],
  ] as const) {
    const times: number[] = [];
    const memory: number[] = [];
    for (let run = 0; run < 14; run++) {
      globalThis.gc?.();
      const before = process.memoryUsage();
      const start = performance.now();
      compileNeedles(source);
      const elapsed = performance.now() - start;
      const after = process.memoryUsage();
      if (run < 3) continue;
      times.push(elapsed);
      memory.push(after.heapUsed + after.arrayBuffers - before.heapUsed - before.arrayBuffers);
    }
    times.sort((a, b) => a - b);
    memory.sort((a, b) => a - b);
    rows.push({
      case: name,
      ms: round(times[5], 3),
      "heap + buffers MiB": round(memory[5] / 2 ** 20, 3),
    });
  }
  console.table(rows);
}

async function expansionBenchmark() {
  console.log("\n## Stalled reader: stream pairs, one chunk, 100 fresh 256 KiB replacements");
  const rows = [];
  for (const mode of ["sync", "async", "needles"] as const) {
    let calls = 0;
    const resolve = () => {
      calls++;
      return new Uint8Array(256 * 1024);
    };
    const options = { open: "{{", close: "}}", resolve };
    const tx =
      mode === "sync"
        ? createTokenStreamPair(options)
        : mode === "async"
          ? createAsyncTokenStreamPair({ ...options, resolve: async () => resolve() })
          : createNeedleStreamPair({ needles: ["{{x}}"], resolve });
    const reader = tx.readable.getReader();
    const writer = tx.writable.getWriter();
    const first = reader.read();
    const written = writer.write(bytes("{{x}}".repeat(100))).then(() => writer.close());
    let total = (await first).value?.length ?? 0;
    await new Promise((done) => setTimeout(done, 10));
    rows.push({
      mode,
      "resolver calls": calls,
      "queued replacement MiB": (calls * 256 * 1024 - total) / 2 ** 20,
    });
    assert.ok(calls <= 2);
    for (;;) {
      const part = await reader.read();
      if (part.done) break;
      total += part.value.length;
    }
    await written;
    assert.equal(total, 100 * 256 * 1024);
    reader.releaseLock();
    writer.releaseLock();
  }
  console.table(rows);
}

scannerBenchmark();
await delayedInputBenchmark();
compilationBenchmark();
await expansionBenchmark();
await memoryBenchmark();
