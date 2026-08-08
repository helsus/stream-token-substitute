# stream-token-substitute

Fill `{{token}}` holes in a streaming response body, at the edge, without buffering it.

A `TransformStream<Uint8Array, Uint8Array>` that scans bytes. It never decodes text, passes the
spans between tokens through without copying, and gets chunk boundaries right. Zero dependencies,
ESM only, ~10x faster to first byte than buffer-and-replace.

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
const encoder = new TextEncoder();
const values = new Map([["title", encoder.encode("Hello")]]);

const transformer = createTokenTransformStream({
  open: "{{",
  close: "}}",
  resolve: (payload) => values.get(decoder.decode(payload)) ?? null, // null leaves it verbatim
});

return new Response(shell.body.pipeThrough(transformer), {
  headers: { "content-type": "text/html; charset=utf-8" },
});
```

Node 18+ and Cloudflare Workers, tested in CI on Node 20, 22 and 24.

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

Matching is on bytes, not characters: no Unicode normalization, so needles and body must agree on
composition. Duplicates keep the first entry. A set large enough to need more than
`DEFAULT_MAX_TABLE_BYTES` (16 MiB) of transition table is refused at compile time rather than at
request time; pass `maxTableBytes` to raise or lower that.

## API

Six entrypoints. The core carries nothing the others add.

| import | exports |
|---|---|
| `stream-token-substitute` | `createTokenTransformStream`, `createTokenTransformer`, `jsonEscapeBytes` |
| `stream-token-substitute/needles` | `createNeedleTransformStream`, `createNeedleTransformer`, `compileNeedles`, `DEFAULT_MAX_TABLE_BYTES` |
| `stream-token-substitute/async` | `createAsyncTokenTransformStream`, `createAsyncTokenTransformer` |
| `stream-token-substitute/html` | `htmlEscapeBytes`, `attrEscapeBytes` |
| `stream-token-substitute/helpers` | `resolveFrom`, `substituteResponse` |
| `stream-token-substitute/node` | `createTokenTransform`, `createAsyncTokenTransform`, `createNeedleTransform` |

Transformers are single-use: construct one per stream. `create*Transformer` returns the bare
`{ transform, flush }` for runtimes where `TransformStream` is not a global. `resolveFrom` builds
a resolver over a record or `Map`, matching names as bytes and hashing larger length buckets so a
lookup never decodes the payload. Unknown names resolve to `null` and pass through verbatim.

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

Only a returned thenable suspends the scan. A direct return avoids a microtask. Tokens
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

Each takes an optional second argument, `{ highWaterMark, signal }`. Object mode is disabled and string writes are decoded because the scanner reads `Uint8Array` chunks and pushes `Buffer` views over the same bytes.

```ts
createNeedleTransform({ needles }, { highWaterMark: 64 * 1024, signal });
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
delimiter matrix, random and exhaustive chunkings, and a matrix of validators and resolvers. The
seed is fixed so a failure reproduces exactly; override it with `FUZZ_SEED` and `FUZZ_ROUNDS` to
search harder. It runs on Node and again under workerd, where chunking and backpressure differ,
though only the Node run takes those two variables. Bun and Deno re-run the smaller contract
checks.

## Performance

197,355-byte HTML document, 501 tokens via `resolveFrom`, 13 chunks of 16 KB. Node v24.2.0, linux/x64, Ryzen 9
5950X, medians from [mitata](https://github.com/evanwashere/mitata). `npm run bench`.

| | total | MB/s | TTFB | alloc/run | out parts |
|---|---|---|---|---|---|
| buffer + `String.replace` | **0.48 ms** | **410** | 0.454 ms | 497 kB (258%) | 1 |
| stream, `flushBytes: 0` | 1.68 ms | 117 | 0.057 ms | 862 kB (447%) | 1015 |
| stream, `flushBytes: 16384` | 0.54 ms | 367 | **0.046 ms** | **196 kB (102%)** | 25 |

TTFB is the point: `String.replace` emits nothing until the whole body is done, and that gap
grows with body size while the throughput gap does not. Allocation is under the body itself and
independent of it, so a 5 MB body runs in a 2 MB budget. Spans of 1 kB or more are always
enqueued by reference, so a sparse shell is effectively zero-copy; this benchmark is dense on
purpose (a token every 390 bytes), which is the worst case for the merge accumulator.

Time is O(n) unless `open[0]` is a valid payload byte, in which case O(n `*` `maxPayloadBytes`),
which the naive reference pays too. Space is O(`maxPayloadBytes` + |`open`| + |`close`|) carried,
never O(body).

### Compared to `replacestream`

[`replacestream`](https://www.npmjs.com/package/replacestream) is a mature Node text-replacement stream and is better at a job this one does not do.

| | `stream-token-substitute` | `replacestream` 4.0.3 |
|---|---|---|
| operates on | bytes | text, decoded per chunk |
| matches | a delimited token, or a set of literals | a string, or any regex |
| capture groups, `$1` | no | yes |
| case | byte-exact | a string search is case-**insensitive** unless given `ignoreCase: false` |
| decides per match | resolver over the payload bytes | replacement function |
| awaitable resolver | `stream-token-substitute/async` | no |
| streams | Web Streams, plus `/node` | Node only, via `readable-stream` v2 |
| runtimes | Node, Workers, Deno, Bun | Node |
| dependencies | 0 | 3 |
| carried state | `maxPayloadBytes`, 64 by default | `maxMatchLen`, 100 by default |
| unmatched spans | large spans enqueued by reference | re-concatenated as strings |
| multi-byte character split across chunks | preserved | lost |

`replacestream` calls `buf.toString()` on each chunk alone, so a character straddling the boundary decodes as two invalid halves. The bench cuts `"h\u00e9llo {{h0}} w\u00f6rld"` one byte into the `\u00e9`:

```
stream-token-substitute   h\u00e9llo Heading number 0 w\u00f6rld
replacestream             h\ufffd\ufffdllo Heading number 0 w\u00f6rld
```

Two more differences matter. A regex without `g` replaces once and stops. The case-insensitive default means `__NONCE__` matches `__nonce__`.

Seven shapes of a ~2 MB document, 16 KB chunks, warmed medians with `resolveFrom`. `npm run bench:compare`.

| total | `String.replace` | `stream-token-substitute` | `stream-token-substitute/node` | `replacestream` |
|---|---|---|---|---|
| dense template, 5001 holes | 3.57 ms | 2.81 ms | **2.05 ms** | 4.13 ms |
| sparse shell, 3 holes | 2.74 ms | 0.67 ms | **0.34 ms** | 2.32 ms |
| no tokens at all | 2.15 ms | 0.71 ms | **0.35 ms** | 2.34 ms |
| inline CSS braces, 3 holes | 2.87 ms | 2.87 ms | 3.24 ms | **2.45 ms** |
| one 256 KB replacement | 3.16 ms | 0.70 ms | **0.38 ms** | 2.84 ms |
| one literal marker | 2.33 ms | 0.69 ms | **0.31 ms** | 1.95 ms |
| 32 literal markers | 36.03 ms | 1.89 ms | **1.58 ms** | 3.76 ms |

| first byte | `String.replace` | `stream-token-substitute` | `stream-token-substitute/node` | `replacestream` |
|---|---|---|---|---|
| dense template, 5001 holes | 3.555 ms | **0.056 ms** | 0.235 ms | 0.323 ms |
| sparse shell, 3 holes | 2.711 ms | **0.032 ms** | 0.067 ms | 0.218 ms |
| no tokens at all | 2.121 ms | **0.026 ms** | 0.064 ms | 0.209 ms |
| inline CSS braces, 3 holes | 2.844 ms | **0.049 ms** | 0.327 ms | 0.223 ms |
| one 256 KB replacement | 3.086 ms | **0.026 ms** | 0.087 ms | 0.665 ms |
| one literal marker | 2.312 ms | **0.042 ms** | 0.063 ms | 0.182 ms |
| 32 literal markers | 36.107 ms | **0.086 ms** | 0.236 ms | 0.374 ms |

Sparse is the common shape. Most of a shell is bytes nobody is matching. They go out by reference here and through a decode, a regex and a concat there. `inline CSS braces` is a loss on both counts. A lone `{` is a partial `{{`, so CSS rules keep waking the per-byte path that `indexOf` otherwise skips. A delimiter that does not collide with the body's punctuation avoids it.

Chunk size, on the sparse shell:

| | 16 KB | 1 KB | 64 B |
|---|---|---|---|
| `String.replace` | 2.72 ms | 2.72 ms | 2.71 ms |
| `stream-token-substitute` | 0.71 ms | 4.01 ms | 60.09 ms |
| `stream-token-substitute/node` | **0.30 ms** | **1.08 ms** | **16.37 ms** |
| `replacestream` | 2.26 ms | 3.81 ms | 29.53 ms |

Node to node this wins at every size. The web path loses below 16 KB, by 2x at 64 B. Node's readable merges buffered reads, so `/node` emits 35 parts where the web path emits 123. That is also why the web path reaches first byte sooner.

The async scanner on the dense template, same answers:

| | total | first byte |
|---|---|---|
| `stream-token-substitute/node`, sync | 1.97 ms | 0.203 ms |
| `stream-token-substitute/async`, resolver answers directly | 2.16 ms | 0.200 ms |
| `stream-token-substitute/async`, resolver returns a promise | 2.98 ms | 0.271 ms |

The async scanner costs 10% when nothing awaits and 51% when every one of 5001 tokens suspends and resumes. `replacestream` has no async mode.

Bun 1.3.14 runs the same bench, `npm run bench:compare:bun`. Totals, 16 KB chunks:

| total | `String.replace` | `stream-token-substitute` | `replacestream` |
|---|---|---|---|
| dense template, 5001 holes | 1.87 ms | **1.73 ms** | 2.22 ms |
| sparse shell, 3 holes | 0.64 ms | **0.33 ms** | 1.04 ms |
| no tokens at all | 1.00 ms | **0.31 ms** | 0.96 ms |
| inline CSS braces, 3 holes | **0.75 ms** | 2.73 ms | 1.12 ms |
| one 256 KB replacement | 1.82 ms | **0.30 ms** | 1.29 ms |
| one literal marker | 1.48 ms | **0.33 ms** | 0.88 ms |
| 32 literal markers | 10.91 ms | **2.06 ms** | 3.35 ms |

JSC's `String.replace` stays competitive on the dense row and takes CSS. `/node` is left out because it moved 3x between runs on Bun's `node:stream` shim. Use the Web Streams entrypoint there.

If you need a pattern rather than a literal or a delimited token, use `replacestream`. This library cannot do that job.

## Runtimes

The full suite runs on Node 20, 22 and 24 in CI. Node 18 is still supported but not exercised:
the test runner does not start on it. Cloudflare Workers (workerd), Bun and Deno
run the five cross-runtime contract checks in `test/cross-runtime.ts`, each under that runtime's
own test runner.

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
npm run test:bun       # test:deno for the other runtimes
npm run bench          # bench:needles, bench:workers for the other paths
```

## License

MIT
