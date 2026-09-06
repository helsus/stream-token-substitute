# stream-token-substitute

Replace tokens in a `ReadableStream<Uint8Array>` without buffering the whole body. Matches survive chunk boundaries. Large unmatched spans pass through without copying. Zero runtime dependencies. ESM, Node 18+, Cloudflare Workers, Bun, and Deno.

```sh
npm install stream-token-substitute
```

## Usage

```ts
import { resolveFrom, substituteResponse } from "stream-token-substitute/helpers"

const shell = await fetch("https://example.com/shell.html")
const response = substituteResponse(shell, {
  open: "{{",
  close: "}}",
  resolve: resolveFrom({ nonce: crypto.randomUUID() }),
})
```

`resolveFrom` accepts a record or `Map` of names to strings or bytes. Unknown tokens stay unchanged. `substituteResponse` preserves status and headers except stale `Content-Length`, `ETag`, `Digest`, and `Content-Digest`. Bodyless responses are unchanged.

For raw streams, import `createTokenTransformStream` from the main entrypoint and use `input.pipeThrough(createTokenTransformStream(options))`. The `create*TransformStream` factories return native `TransformStream` instances. Create a new transformer per stream. Replacement bytes are never scanned again.

## Token options

| Option | Default | Behavior |
| --- | --- | --- |
| `open` | required | Non-empty string or `Uint8Array`. |
| `close` | `open` | Non-empty string or `Uint8Array`. |
| `resolve(payload)` | required | Return bytes, empty bytes to remove, or `null` to preserve the token. |
| `validate(payload, next)` | none | Return `false` to abort before committing the next payload byte. |
| `maxPayloadBytes` | `64` | Abort tokens exceeding this payload size. |
| `flushBytes` | `16384` | Merge small output pieces at this threshold. `0` disables merging. |
| `onResolveError(error, payload)` | none | Recover from resolver errors with bytes or `null`. |
| `onDone(stats)` | none | On flush: `resolved`, `rejected`, `aborted`, `bytesIn`, `bytesOut`. |

Matching is byte-exact. String delimiters are UTF-8 encoded. The first closing delimiter ends a token and takes precedence over validation. Rejected tokens (`null`) pass through intact. Aborted tokens emit their opening delimiter and re-scan their payload. Incomplete tokens pass through at end of stream.

Invalid options throw synchronously. Unhandled resolver errors and validator errors fail the stream.

## Literal replacement

```ts
import { compileNeedles, createNeedleTransformStream } from "stream-token-substitute/needles"

const needles = compileNeedles({ __BUILD_ID__: "v1.3.0", __APP_NAME__: "Example" })
const output = input.pipeThrough(createNeedleTransformStream({ needles }))
```

Reuse compiled needles across streams. `needles` also accepts a record, a `Map` with string or byte keys, or an array with `resolve(needle, index)`. Matches are leftmost-longest. Duplicate needles keep the first entry. The default transition-table limit is 16 MiB, configurable with `maxTableBytes`. Needle options also accept `flushBytes` and `onDone`. Stats contain `substituted`, `rejected`, `bytesIn`, and `bytesOut`.

## Async and Node streams

`stream-token-substitute/async` exports `createAsyncTokenTransformStream` with the same token options. Its resolver accepts values, promises, or thenables and runs in stream order. Initial buffered output is sent before awaiting a lookup. Use the transform with `substituteResponse` or `pipeThrough`.

Pass `signal: AbortSignal` in async options to detach pending resolution on abort, including on runtimes without `Transformer.cancel` support. Cancel resolver-owned I/O separately.

For Node `pipeline()` and `.pipe()`, `stream-token-substitute/node` exports `createTokenTransform`, `createAsyncTokenTransform`, and `createNeedleTransform`. Each accepts an optional second argument: `{ highWaterMark, signal }`. Web factories have `create*Transformer` counterparts for bare transformer bodies.

## Escaping and memory

Escape replacements for their destination context:

- `/html`: `htmlEscapeBytes` for HTML text and quoted attributes. `attrEscapeBytes` also handles unquoted attributes.
- Main entrypoint: `jsonEscapeBytes` for JSON string contents embedded in HTML. It does not add quotes or serialize objects.

These are not sanitizers. HTML escaping does not make URLs or scripts safe.

Treat input and replacement bytes as immutable, even after a write completes: downstream output may reference them. Sync resolver and validator payloads are scratch views valid only during the callback. Async resolver and error-handler payloads are retainable copies.

For intra-chunk backpressure, use `createTokenStreamPair` from the main entrypoint, `createAsyncTokenStreamPair` from `/async`, or `createNeedleStreamPair` from `/needles`. These return readable/writable pairs, not native `TransformStream` instances. Both `pipeThrough` and `substituteResponse` accept them.

Stream pairs pause within expanding chunks and final output using a 16 KiB queue budget. Node transforms use their readable high-water mark. A replacement or unmatched span can exceed the budget. Native Web transforms and bare transformer bodies do not provide intra-chunk backpressure. Bound input chunks, replacement sizes, and concurrency. Overlap-heavy needles use a rolling index with work proportional to input length times pattern count. The transition-table limit does not cap total compilation memory.

## Development

```sh
npm ci
npm test
npm run test:gc
npm run lint
npm run typecheck
npm run build
```

Runtime checks: `test:workers`, `test:bun`, `test:deno`. Fuzzing: `test:fuzz`, with optional `FUZZ_SEED` and `FUZZ_ROUNDS`.

`npm run bench` measures throughput, first-output latency, compilation, adversarial overlap, and memory with 128 MiB streams. It compares Web/Node transforms with native replacement and replacestream. Performance depends on workload, chunk size, and runtime. `test:workers` also checks 256 MiB streams.

MIT licensed.
