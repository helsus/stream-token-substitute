# stream-token-substitute

Fill `{{token}}` holes in a streaming response body, at the edge, without buffering it.

A `TransformStream<Uint8Array, Uint8Array>` that scans bytes. It never decodes text, passes the
spans between tokens through without copying, and gets chunk boundaries right. Zero dependencies,
ESM only, ~6x faster to first byte than buffer-and-replace.

```sh
npm install stream-token-substitute
```

A whole edge handler: a cached shell from the origin, a per-request nonce in it, first byte out
before the last byte is in.

```ts
import { resolveFrom, substituteResponse } from "stream-token-substitute/helpers";

export default {
  async fetch(request: Request): Promise<Response> {
    const nonce = crypto.randomUUID();
    const shell = await fetch("https://origin.example/shell.html"); // {{title}}, {{nonce}}

    const response = substituteResponse(shell, {
      open: "{{",
      close: "}}",
      resolve: resolveFrom({ title: "Hello", nonce }),
    });

    response.headers.set("content-security-policy", `script-src 'nonce-${nonce}'`);
    return response;
  },
};
```

`substituteResponse` pipes the body through a transformer and drops the headers that no longer
describe it. Without it, the same thing by hand:

```ts
import { createTokenTransformStream } from "stream-token-substitute";

const decoder = new TextDecoder();

const transformer = createTokenTransformStream({
  open: "{{",
  close: "}}",
  resolve: (payload) => values.get(decoder.decode(payload)) ?? null, // null leaves it verbatim
});

return new Response(shell.body.pipeThrough(transformer), {
  headers: { "content-type": "text/html; charset=utf-8" },
});
```

Node 18+ and Cloudflare Workers, both tested in CI.

## Why

You have one cached or pre-rendered body and a few per-request values to drop into it: a CSP
nonce, a cart count, a signed URL in a manifest, SSR hydration state.

`String.replace` needs the whole document in memory three times over and sends nothing until the
last byte is transformed. An HTML parser streams but only sees elements. Hand-rolled chunk
scanning forgets the token split across two chunks. This does the one job, in linear time, with
carried state bounded by `maxPayloadBytes` rather than body size.

Not for structural edits (rewriting attributes, injecting elements, CSS selectors). Use a
streaming HTML parser for that.

## Two modes

| | `stream-token-substitute` | `/needles` |
|---|---|---|
| matches | `open` payload `close` | a set of literals |
| decides by | a resolver over the payload | a table, or a resolver |
| use for | a template you designed | markers already in the body |
| grammar | delimited, leftmost-shortest | leftmost-longest, Aho-Corasick |

Both: bytes in, bytes out, nothing decoded, chunk boundaries handled, substituted output never
re-scanned.

```ts
import { createNeedleTransformStream } from "stream-token-substitute/needles";

const transformer = createNeedleTransformStream({
  needles: { __BUILD_ID__: buildId, __NONCE__: nonce },
});
```

`needles` takes a record, a `Map` (byte keys allowed), or an array plus `resolve`. Reuse a set
across requests with `compileNeedles(...)` at module scope: it is the only per-stream cost that
grows with the set.

## API

Six entrypoints. The core carries nothing the others add.

| import | exports |
|---|---|
| `stream-token-substitute` | `createTokenTransformStream`, `createTokenTransformer`, `jsonEscapeBytes` |
| `stream-token-substitute/needles` | `createNeedleTransformStream`, `createNeedleTransformer`, `compileNeedles` |
| `stream-token-substitute/async` | `createAsyncTokenTransformStream`, `createAsyncTokenTransformer` |
| `stream-token-substitute/html` | `htmlEscapeBytes`, `attrEscapeBytes` |
| `stream-token-substitute/helpers` | `resolveFrom`, `substituteResponse` |
| `stream-token-substitute/node` | `createTokenTransform`, `createAsyncTokenTransform`, `createNeedleTransform` |

Transformers are single-use: construct one per stream. `create*Transformer` returns the bare
`{ transform, flush }` for runtimes where `TransformStream` is not a global. `resolveFrom` builds
a resolver over a record or `Map`, matching names as bytes bucketed by length so a lookup never
decodes the payload; unknown names resolve to `null` and pass through verbatim.

### `TokenTransformOptions`

| option | type | default | meaning |
|---|---|---|---|
| `open` | `string \| Uint8Array` | required | Opening delimiter, non-empty. |
| `close` | `string \| Uint8Array` | `open` | Closing delimiter. May share bytes with `open`. |
| `resolve` | `(payload) => Uint8Array \| null` | required | Bytes to substitute, empty for nothing, `null` to reject. |
| `validate` | `(payload, next) => boolean` | none | Per-byte payload check. `false` aborts the token. |
| `maxPayloadBytes` | `number` | `64` | Payload cap. Exceeding it aborts the token. |
| `flushBytes` | `number` | `16384` | Output merge threshold. `0` enqueues every piece. |
| `onResolveError` | `(error, payload) => Uint8Array \| null` | none | Makes a throwing `resolve` recoverable. |
| `onDone` | `(stats: TokenStats) => void` | none | Fires once from `flush` with `{ resolved, rejected, aborted, bytesIn, bytesOut }`. |

A `validate` narrowing the payload alphabet is worth adding: it bounds how far a malformed token
runs before the scanner gives up.

### Async resolvers

```ts
import { createAsyncTokenTransformStream } from "stream-token-substitute/async";

createAsyncTokenTransformStream({
  open: "{{",
  close: "}}",
  resolve: async (payload) => encoder.encode(await kv.get(decoder.decode(payload))),
});
```

Only a returned thenable suspends the scan, so a cache hit costs one `typeof` check. Tokens
resolve in stream order, which keeps output byte order right and makes a slow resolver
head-of-line blocking. The async resolver gets a copied payload, valid across the await. Same
scanner, suspended and resumed; the differential fuzzer holds it to byte-identical output.

### Escaping

Escaping is yours to apply: a resolver's bytes go into the body as-is.

```ts
import { attrEscapeBytes, htmlEscapeBytes } from "stream-token-substitute/html";

resolve: (payload) => htmlEscapeBytes(values.get(decoder.decode(payload)) ?? EMPTY);
```

`htmlEscapeBytes` covers `& < > " '`; `attrEscapeBytes` adds whitespace and `/ = \`` for unquoted
attributes; `jsonEscapeBytes` (core) covers `<script type="application/json">`. All byte-level,
all return `src` itself when nothing needs escaping. None is a sanitizer.

### Errors

Bad options throw synchronously (`TypeError`, `RangeError`). A non-`Uint8Array` chunk errors the
stream. Exceptions from `resolve` and `validate` propagate unless `onResolveError` handles them,
returning bytes or `null` to ship the placeholder verbatim. That matters at the edge: by the time
a resolver fails, the headers and the bytes before the token are already gone, so the alternative
to a recoverable miss is a truncated document.

### Node streams

```ts
import { pipeline } from "node:stream/promises";
import { createNeedleTransform } from "stream-token-substitute/node";

await pipeline(createReadStream("shell.html"), createNeedleTransform({ needles }), response);
```

## Semantics

Unrecognized input passes through byte for byte. Malformed tokens, unterminated tokens at end of
stream and bare delimiter prefixes all appear unchanged, which is what makes this safe to point
at a template you do not control.

0. **Bytes, not text.** UTF-8 is the assumed case; multi-byte characters work by construction.
   UTF-16 templates are not supported.
1. **Leftmost, shortest.** A payload never contains `close`, only proper prefixes of it.
2. **No backtracking into a consumed `open`.** After an abort, re-scanning starts at the first
   payload byte.
3. **Close first.** In-token bytes feed the close matcher before the validator.
4. **Abort re-scans.** An aborted token emits `open` verbatim and re-scans its payload, so the
   inner token in `{{a{{b}}` is still substituted.
5. **`null` is atomic.** A rejected token is one opaque span, emitted verbatim, never re-scanned,
   so a payload cannot smuggle out a second token.

**No regex, so no ReDoS.** KMP over the delimiters, Aho-Corasick over the needles. An adversarial
template is a passthrough, not a denial of service.

### Buffer ownership

Zero-copy cuts both ways. Do not mutate a chunk or reuse its backing buffer after `write()`
resolves; the transformer may hand views into it downstream. Every runtime tested here is fine, a
pooled-buffer source is not. In return, enqueued parts are never written to again and can be
retained. The `payload` passed to a sync `resolve`/`validate` is scratch, valid only during the
call; bytes returned from `resolve` may be enqueued by reference.

Substitution changes the body length, so do not forward the upstream `Content-Length`, `ETag` or
`Digest`. `substituteResponse` drops them for you.

### Correctness

> For every input and every possible chunking of it, the output is byte-for-byte identical to the
> same substitution run over the whole input at once.

A differential fuzzer runs the streaming implementations against naive references over a
delimiter matrix, random and exhaustive chunkings, and a matrix of validators and resolvers, in
Node and again under workerd. The seed is fixed so a failure reproduces exactly; override it with
`FUZZ_SEED` and `FUZZ_ROUNDS` to search harder.

## Performance

197,355-byte HTML document, 501 tokens, 13 chunks of 16 KB. Node v24.2.0, linux/x64, Ryzen 9
5950X, medians from [mitata](https://github.com/evanwashere/mitata). `npm run bench`.

| | total | MB/s | TTFB | alloc/run | out parts |
|---|---|---|---|---|---|
| buffer + `String.replace` | 0.48 ms | **413** | 0.42 ms | 498 kB (259%) | 1 |
| stream, `flushBytes: 0` | 1.50 ms | 131 | 0.081 ms | 832 kB (431%) | 1015 |
| stream, `flushBytes: 16384` | **1.12 ms** | 176 | **0.064 ms** | **150 kB (78%)** | 25 |

TTFB is the point: `String.replace` emits nothing until the whole body is done, and that gap
grows with body size while the throughput gap does not. Allocation is under the body itself and
independent of it, so a 5 MB body runs in a 2 MB budget. Spans of 1 kB or more are always
enqueued by reference, so a sparse shell is effectively zero-copy; this benchmark is dense on
purpose (a token every 390 bytes), which is the worst case for the merge accumulator.

Time is O(n) unless `open[0]` is a valid payload byte, in which case O(n `*` `maxPayloadBytes`),
which the naive reference pays too. Space is O(`maxPayloadBytes` + |`open`| + |`close`|) carried,
never O(body).

## Runtimes

Node 18, 20, 22 and 24, and Cloudflare Workers (workerd), are tested in CI.

The core touches two globals, `TransformStream` and (for string delimiters only) `TextEncoder`,
both avoidable via `createTokenTransformer` and byte delimiters, and constructs nothing at import
time, so it runs where streams and encoding are modules rather than globals.
`test/no-globals.test.ts` deletes both and substitutes anyway. `/node` requires `node:stream`;
`/helpers` needs `Response` and `Headers`.

## Development

```
npm test               # units, properties, differential fuzz, security, portability
npm run test:fuzz      # FUZZ_SEED=12345 FUZZ_ROUNDS=2000 to override
npm run test:workers   # the same contract checks under workerd
npm run bench          # bench:workers for output shape and the memory ceiling
```

## License

MIT
