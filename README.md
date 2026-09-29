# stream-token-substitute

Replace `{{tokens}}` or literal strings in a `ReadableStream<Uint8Array>` without buffering the body. Matches survive chunk boundaries, unmatched spans pass through without copying, and resolvers may be sync or async. Zero runtime dependencies. ESM, Node 22+, Cloudflare Workers, Bun, Deno.

```sh
npm install stream-token-substitute
```

## Usage

```ts
import { resolveFrom, substituteResponse } from "stream-token-substitute"

const shell = await fetch("https://example.com/shell.html")
const response = substituteResponse(shell, {
  resolve: resolveFrom({ nonce: crypto.randomUUID() }),
})
```

`resolveFrom` takes a record or `Map` of names to strings or bytes. Unknown tokens stay unchanged. `resolveName((name, context) => ...)` hands your function the payload decoded as UTF-8. `substituteResponse` also accepts any readable/writable pair, such as `createLiteralStream(...)`.

## Raw streams

```ts
import { createTokenStream, createTokenTransformer } from "stream-token-substitute"

input.pipeThrough(createTokenStream(options))
input.pipeThrough(new TransformStream(createTokenTransformer(options)))
```

- `createTokenStream` returns a readable/writable pair with backpressure inside a chunk: output is budgeted at 16 KiB, and a slow reader stops the scanner and further resolver calls. Use it by default.
- `createTokenTransformer` returns a `Transformer` body for `new TransformStream(...)`. Every replacement for a chunk is enqueued before the next read, so one chunk with many large replacements is fully expanded in memory. Use it where you need a native `TransformStream` or already bound replacement sizes.

Create one per stream. Replacement bytes are never re-scanned.

## Token options

| Option | Default | Behavior |
| --- | --- | --- |
| `open` | `"{{"` | Non-empty string or `Uint8Array`. |
| `close` | `open`, or `"}}"` if `open` is omitted | Non-empty string or `Uint8Array`. |
| `resolve(payload, context)` | required | See return types below. |
| `validate(payload, next)` | none | Return `false` to abort before committing `next`. `payload` is a view valid only during the call. |
| `maxPayloadBytes` | `64` | Abort tokens with a longer payload. |
| `concurrency` | `4` | Pending async results scanned ahead of output. |
| `borrow` | `false` | Pass `resolve` a view instead of a copy. See Ownership. |
| `mergeBytes` | `16384` | Merge small output pieces up to this size. `0` disables. |
| `onResolveError(error, payload, context)` | none | Recover from a throw or rejection with a replacement or `null`. |
| `signal` | none | Abort stops scanning and errors the stream with the reason. |
| `onDone(stats)` | none | On flush: `replaced`, `rejected`, `aborted`, `bytesIn`, `bytesOut`. |

`resolve` may return `Uint8Array`, `string` (UTF-8 encoded), `null` (keep the token verbatim), a `ReadableStream`, async iterable or iterable of `Uint8Array` or `string` pieces (streamed in order), or a promise of any of these. Output order always matches input order.

`context.signal` is an `AbortSignal` that aborts with the reason when the stream stops early (cancel, abort, or failure), never on normal completion. It is created on first read, so an unused signal costs nothing.

Matching is byte-exact. The first closing delimiter ends a token. Aborted tokens emit their opening delimiter and re-scan the payload. Incomplete tokens pass through at end of stream. Invalid options throw synchronously. Unhandled resolver errors and validator errors fail the stream.

## Ownership

- `payload` is a fresh copy: retaining it or returning it is safe. With `borrow: true` it is a view valid only during the synchronous call, which skips a copy per token. Do not store it, return a view of it, or read it after an await. Returning it unchanged is safe. `resolveFrom` and `resolveName` always borrow.
- Returned replacement bytes are enqueued by reference. Do not mutate them afterwards.
- Unmatched input spans are enqueued as views of the input chunks. Do not reuse input buffers.
- Close, cancel or abort every stream. An abandoned stream keeps its listener on a shared `signal` and its pending lookups alive.

## Literals

```ts
import { compileLiterals, createLiteralStream } from "stream-token-substitute"

const literals = compileLiterals({ __BUILD_ID__: "v2.0.0", __APP_NAME__: "Example" })
input.pipeThrough(createLiteralStream({ literals }))
```

Compile once and reuse across streams. `literals` also accepts an uncompiled record, a `Map` with string or byte keys, or an array with `resolve(literal, index, context)`. Matching is leftmost-longest, byte-exact, and duplicates keep the first entry. Options: `literals`, `resolve`, `maxMemoryBytes`, `concurrency`, `borrow`, `mergeBytes`, `onResolveError`, `signal`, `onDone` (`replaced`, `rejected`, `bytesIn`, `bytesOut`). `resolve` follows the token contract: it gets a fresh copy of the literal, or a view with `borrow: true`, and may return anything a token resolver can. `createLiteralTransformer` is the `TransformStream` body counterpart.

## Node

```ts
import { createLiteralTransform, createTokenTransform } from "stream-token-substitute/node"

await pipeline(source, createTokenTransform(options, { highWaterMark, signal }), sink)
```

Both return a Node `Transform`. Output parts are `Buffer`-compatible views. Backpressure uses the readable high-water mark.

## Escaping

`escapeHtml` (text and quoted attributes), `escapeAttr` (also unquoted attributes), and `escapeJson` (JSON string contents embedded in HTML, without quotes) take and return `Uint8Array`. They are not sanitizers: they do not make URLs or scripts safe.

## HTTP contract

- The body must be decoded, as `fetch()` yields it. Compressed bytes will not match.
- Removed headers: `Content-Length`, `Content-Encoding`, `ETag`, `Digest`, `Content-Digest`, `Repr-Digest`, `Content-MD5`, `Accept-Ranges`. Status and other headers are kept, including `Cache-Control` and `Last-Modified`. For per-request values such as a nonce, set `Cache-Control: private, no-store` yourself.
- A `206` or any `Content-Range` response throws `TypeError`: request the full representation.
- Bodyless responses are returned unchanged.

## Limits and costs

- Token scanning is linear in input. A payload cap abort without `validate` stays linear. With `validate`, an abort replays the payload, so cost grows with `maxPayloadBytes`: 50 KB of `{{` with an always-true validator and a 1024 cap takes about 0.8 s. Keep the cap small and reject delimiter bytes in `validate`.
- Literal cost is about input length plus matches. Overlap-heavy sets switch to a rolling index.
- `compileLiterals(source, { maxMemoryBytes })` refuses sets whose estimated compile memory exceeds `DEFAULT_MAX_MEMORY_BYTES` (16 MiB) unless raised.
- A single replacement or unmatched span may exceed the 16 KiB output budget. Bound replacement sizes and `concurrency`.

## Development

```sh
npm ci
npm test
npm run test:gc
npm run lint
npm run typecheck
npm run build
```

Runtime checks: `test:workers`, `test:bun`, `test:deno`. Fuzzing: `test:fuzz`, with optional `FUZZ_SEED` and `FUZZ_ROUNDS`. `npm run bench` compares throughput, first-output latency, async lookahead, slow-reader expansion, and memory against `String.replace`, replacestream, and replace-content-transformer. It runs TypeScript directly, so it needs Node 22.18+.

MIT licensed.
