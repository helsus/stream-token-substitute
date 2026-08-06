import { createTokenTransformStream } from "./transformer.ts";
import type { TokenResolver, TokenTransformOptions } from "./types.ts";

/** These describe the upstream body, which substitution has just changed. */
const STALE_HEADERS = ["content-length", "etag", "digest", "content-digest"];

let encoder: TextEncoder | undefined;

/** Lazy for the same reason the delimiter encoder is: nothing is constructed at
 *  import time, so this module stays importable where TextEncoder is not global. */
function encode(value: string): Uint8Array {
  if (encoder === undefined) {
    if (typeof TextEncoder === "undefined") {
      throw new TypeError("resolveFrom needs a global TextEncoder; pass Uint8Array values");
    }
    encoder = new TextEncoder();
  }
  return encoder.encode(value);
}

interface Entry {
  key: Uint8Array;
  value: Uint8Array;
}

/**
 * A resolver over a fixed set of names.
 *
 * Keys are matched as bytes, bucketed by length, so a lookup is a byte compare
 * against the few keys of the right length. That skips the `TextDecoder.decode`
 * per token that the hand-rolled Map version pays, and with it the string
 * allocation. Unknown names resolve to null, which emits them verbatim.
 */
export function resolveFrom(
  values: Record<string, string | Uint8Array> | Map<string, string | Uint8Array>,
): TokenResolver {
  const entries: [string, string | Uint8Array][] =
    values instanceof Map ? [...values] : Object.entries(values);

  const byLength = new Map<number, Entry[]>();
  for (const [name, value] of entries) {
    const key = encode(name);
    const bytes = typeof value === "string" ? encode(value) : new Uint8Array(value);
    const bucket = byLength.get(key.length);
    if (bucket === undefined) byLength.set(key.length, [{ key, value: bytes }]);
    else bucket.push({ key, value: bytes });
  }

  return (payload) => {
    const bucket = byLength.get(payload.length);
    if (bucket === undefined) return null;
    for (let e = 0; e < bucket.length; e++) {
      const key = bucket[e].key;
      let i = 0;
      while (i < key.length && key[i] === payload[i]) i++;
      if (i === key.length) return bucket[e].value;
    }
    return null;
  };
}

/**
 * Pipe a response body through a substitution and drop the headers that no
 * longer describe it: `Content-Length`, `ETag`, `Digest`, `Content-Digest`.
 *
 * Pass options to build a sync transformer, or a `TransformStream` you built
 * yourself, which is how an async resolver gets here without this module
 * depending on the async entrypoint.
 *
 * A response with no body (204, HEAD) is returned untouched.
 */
export function substituteResponse(
  response: Response,
  substitution: TokenTransformOptions | TransformStream<Uint8Array, Uint8Array>,
): Response {
  if (response.body === null) return response;

  const transform =
    substitution instanceof TransformStream
      ? substitution
      : createTokenTransformStream(substitution);

  const headers = new Headers(response.headers);
  for (const name of STALE_HEADERS) headers.delete(name);

  return new Response(response.body.pipeThrough(transform), {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}
