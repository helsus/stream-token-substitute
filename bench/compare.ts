// Run: npm run bench:compare
//
// Against `replacestream`. Bytes in, bytes out, warmed median, no forced GC:
// `.gc("inner")` costs about what these runs take and reorders the field.
// Memory is bench.ts's job. Output is checked against String.replace first.
import { PassThrough, Readable } from "node:stream";
import replacestream from "replacestream";
import { resolveFrom } from "../src/helpers.ts";
import { createNeedleTransformStream } from "../src/needles.ts";
import {
  createAsyncTokenTransform,
  createNeedleTransform,
  createTokenTransform,
} from "../src/node.ts";
import { createTokenTransformStream } from "../src/transformer.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const bytes = (text: string) => encoder.encode(text);

const CHUNK_SIZES = [16 * 1024, 1024, 64];
const MAIN_CHUNK = 16 * 1024;
const SECTIONS = 5000;
const WARMUP = 8;
const SAMPLES = 21;

const PROSE = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(6);
/** Single braces: near-misses for a `{{` open, and what real markup looks like. */
const BRACES = ".a{color:red}.b{margin:0}.c{padding:0}".repeat(9);

/** ~2 MB of HTML, one heading per section. Big enough that the differences are
 *  not measurement noise. */
function makeDocument(heading: (i: number) => string, filler = PROSE): Uint8Array {
  const parts = [`<!doctype html><html><head><title>${heading(0)}</title></head><body>`];
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

const nodeSource = (parts: Uint8Array[]) =>
  Readable.from(parts.map((part) => Buffer.from(part.buffer, part.byteOffset, part.length)));

/** replacestream is readable-stream v2, not async iterable. Same hop for every
 *  node contender so it is not charged to one. */
const nodeOut = (stream: NodeJS.ReadableStream) =>
  stream.pipe(new PassThrough()) as unknown as AsyncIterable<Uint8Array>;

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
for (let i = 0; i < SECTIONS; i++) {
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
        open: (chunks) => nodeOut(nodeSource(chunks).pipe(createTokenTransform(tokenOptions))),
        note: "node streams",
      },
      {
        name: "replacestream",
        open: (chunks) =>
          nodeOut(
            nodeSource(chunks).pipe(
              replacestream(
                /\{\{([^}]*)\}\}/g,
                (whole: string, key: string) => valueText.get(key) ?? whole,
              ),
            ),
          ),
        note: "node streams",
      },
    ],
  };
}

function needleScenario(title: string, doc: Uint8Array, needles: Record<string, string>): Scenario {
  // Longest alternative first: a JS alternation is first-match.
  const pattern = Object.keys(needles)
    .sort((a, b) => b.length - a.length)
    .join("|");
  const count = Object.keys(needles).length;
  const substitute = (text: string) => {
    let out = text;
    for (const [name, value] of Object.entries(needles)) out = out.split(name).join(value);
    return out;
  };
  const expected = bytes(substitute(decoder.decode(doc)));
  return {
    title,
    doc,
    expected,
    contenders: [
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
        open: (chunks) => nodeOut(nodeSource(chunks).pipe(createNeedleTransform({ needles }))),
        note: "node streams, Aho-Corasick",
      },
      {
        name: "replacestream",
        open: (chunks) =>
          nodeOut(
            nodeSource(chunks).pipe(
              replacestream(
                new RegExp(`(?:${pattern})`, "g"),
                (match: string) => needles[match] ?? match,
              ),
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
        open: (chunks) => nodeOut(nodeSource(chunks).pipe(createTokenTransform(tokenOptions))),
        note: "sync scanner, for reference",
      },
      {
        name: "stream-token-substitute/async",
        open: (chunks) => nodeOut(nodeSource(chunks).pipe(createAsyncTokenTransform(direct))),
        note: "resolver answers directly",
      },
      {
        name: "stream-token-substitute/async",
        open: (chunks) => nodeOut(nodeSource(chunks).pipe(createAsyncTokenTransform(awaited))),
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

console.log(`node ${process.version} on ${process.platform}/${process.arch}`);
console.log(`warmup ${WARMUP}, median of ${SAMPLES}, no forced GC`);

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
for (const contender of (scenarios[0] as Scenario).contenders.slice(1)) {
  const { out } = await collect(contender.open(utf8Chunks));
  utf8Rows.push({
    library: contender.name,
    survives: same(out, utf8Expected),
    output: decoder.decode(out),
  });
}
console.table(utf8Rows);
