// Akamai EdgeWorkers exposes streams and encoding as built-in *modules*, not
// globals: `import { TransformStream } from 'streams'`. This file deletes the
// globals to prove the package still imports and still substitutes when the
// constructors arrive as module bindings.
//
// It has to stay its own file. The lazy `TextEncoder` in bytes.ts is cached in
// module scope, so a test that has already imported src/ under normal globals
// would leave nothing to throw. Vitest isolates per file; that isolation is the
// mechanism under test.

import { expect, it } from "vitest";

const GLOBALS = [
  "TextEncoder",
  "TextDecoder",
  "TransformStream",
  "ReadableStream",
  "WritableStream",
] as const;

type Ambient = Record<string, unknown>;

/** Run `body` with every stream/encoding global removed, mimicking EdgeWorkers.
 *  The saved constructors stand in for the module bindings a bundle imports. */
async function withoutGlobals<T>(body: () => Promise<T>): Promise<T> {
  const ambient = globalThis as unknown as Ambient;
  const saved = GLOBALS.map((name) => [name, ambient[name]] as const);
  for (const name of GLOBALS) delete ambient[name];
  try {
    return await body();
  } finally {
    for (const [name, value] of saved) ambient[name] = value;
  }
}

// Captured while the globals still exist, exactly as an EdgeWorkers bundle
// captures them at the top of the file via import.
const StreamsTransformStream = TransformStream;
const encoding = { TextEncoder, TextDecoder };

it("imports and substitutes with no stream or encoding globals", async () => {
  const result = await withoutGlobals(async () => {
    // Fresh evaluation under the stripped globals: a module-scope
    // `new TextEncoder()` anywhere in the package would throw right here.
    const { createTokenTransformer, createTokenTransformStream } = await import("../src/index.ts");

    const encoder = new encoding.TextEncoder();
    const decoder = new encoding.TextDecoder();
    const transform = createTokenTransformer({
      // Delimiters are bytes here: string delimiters would need a global
      // TextEncoder, which EdgeWorkers does not have. The module-imported
      // encoder does the job.
      open: encoder.encode("{{"),
      close: encoder.encode("}}"),
      resolve: (payload) => (decoder.decode(payload) === "who" ? encoder.encode("edge") : null),
    });

    const stream = new StreamsTransformStream<Uint8Array, Uint8Array>(transform);
    const writer = stream.writable.getWriter();
    void writer
      .write(encoder.encode("hello {{wh"))
      .then(() => writer.write(encoder.encode("o}} and {{nope}}")))
      .then(() => writer.close());

    const parts: string[] = [];
    const reader = stream.readable.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      parts.push(decoder.decode(value));
    }

    const thrown: string[] = [];
    // The wrapper is the one path that needs the global TransformStream.
    try {
      createTokenTransformStream({ open: encoder.encode("{{"), resolve: () => null });
    } catch (error) {
      thrown.push((error as Error).constructor.name);
    }
    // String delimiters are the one path that needs the global TextEncoder,
    // and they fail with an actionable message rather than a ReferenceError.
    for (const options of [
      { open: "{{", resolve: () => null },
      { open: encoder.encode("{{"), close: "}}", resolve: () => null },
    ]) {
      try {
        createTokenTransformer(options);
      } catch (error) {
        thrown.push((error as Error).message);
      }
    }
    return { output: parts.join(""), thrown };
  });

  expect(result.output).toBe("hello edge and {{nope}}");
  expect(result.thrown).toEqual([
    "ReferenceError",
    "open must be a Uint8Array in runtimes without a global TextEncoder",
    "close must be a Uint8Array in runtimes without a global TextEncoder",
  ]);
});

it("accepts Uint8Array delimiters without ever constructing a TextEncoder", async () => {
  const output = await withoutGlobals(async () => {
    const { createTokenTransformer } = await import("../src/index.ts");
    const transform = createTokenTransformer({
      open: new Uint8Array([0x7b, 0x7b]),
      close: new Uint8Array([0x7d, 0x7d]),
      resolve: () => new Uint8Array([0x21]),
    });
    const stream = new StreamsTransformStream<Uint8Array, Uint8Array>(transform);
    const writer = stream.writable.getWriter();
    void writer.write(new Uint8Array([0x7b, 0x7b, 0x61, 0x7d, 0x7d])).then(() => writer.close());
    const { value } = await stream.readable.getReader().read();
    return value;
  });

  expect(output).toEqual(new Uint8Array([0x21]));
});

it("imports and substitutes needles with no stream or encoding globals", async () => {
  const result = await withoutGlobals(async () => {
    // Same contract as the token entrypoint: a module-scope `new TextEncoder()`
    // in needles.ts or aho-corasick.ts would throw on this import.
    const { createNeedleTransformer } = await import("../src/needles.ts");

    const thrown: string[] = [];
    try {
      // A string needle needs a global TextEncoder, which EdgeWorkers lacks.
      createNeedleTransformer({ needles: { __A__: "1" } });
    } catch (error) {
      thrown.push((error as Error).message);
    }

    const transform = createNeedleTransformer({
      needles: new Map([[new Uint8Array([0x61, 0x62]), new Uint8Array([0x21])]]),
    });
    const stream = new StreamsTransformStream<Uint8Array, Uint8Array>(transform);
    const writer = stream.writable.getWriter();
    void writer.write(new Uint8Array([0x78, 0x61, 0x62, 0x79])).then(() => writer.close());
    const { value } = await stream.readable.getReader().read();
    return { thrown, value };
  });

  expect(result.thrown).toEqual([
    "needle must be a Uint8Array in runtimes without a global TextEncoder",
  ]);
  expect(result.value).toEqual(new Uint8Array([0x78, 0x21, 0x79]));
});
