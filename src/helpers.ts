import { createTokenStream } from "./transformer.ts";
import { BORROWING, type TokenResolver, type TokenTransformOptions } from "./types.ts";

/** Headers describing the upstream bytes, which substitution changes. */
const STALE_HEADERS = [
  "content-length",
  "content-encoding",
  "etag",
  "digest",
  "content-digest",
  "repr-digest",
  "content-md5",
  "accept-ranges",
];

let encoder: TextEncoder | undefined;

/** Lazy, so importing needs no global TextEncoder. */
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

const HASH_BUCKET_THRESHOLD = 8;

function hashBytes(bytes: Uint8Array): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < bytes.length; i++) {
    hash = Math.imul(hash ^ bytes[i], 0x01000193);
  }
  return hash >>> 0;
}

/**
 * A resolver over a fixed set of names.
 *
 * Keys are matched as bytes, linearly in small length buckets and by hash in
 * larger ones. That skips the `TextDecoder.decode` and string allocation per
 * token. Unknown names resolve to null, which emits them verbatim.
 */
export function resolveFrom(
  values: Record<string, string | Uint8Array> | Map<string, string | Uint8Array>,
): TokenResolver {
  const entries: [string, string | Uint8Array][] =
    values instanceof Map ? [...values] : Object.entries(values);

  const entriesByLength = new Map<number, Entry[]>();
  for (const [name, value] of entries) {
    const key = encode(name);
    const bytes = typeof value === "string" ? encode(value) : new Uint8Array(value);
    const bucket = entriesByLength.get(key.length);
    if (bucket === undefined) entriesByLength.set(key.length, [{ key, value: bytes }]);
    else bucket.push({ key, value: bytes });
  }

  const byLength = new Map<number, Entry[] | Map<number, Entry[]>>();
  for (const [length, bucket] of entriesByLength) {
    if (bucket.length <= HASH_BUCKET_THRESHOLD) {
      byLength.set(length, bucket);
      continue;
    }
    const byHash = new Map<number, Entry[]>();
    for (const entry of bucket) {
      const hash = hashBytes(entry.key);
      const collisions = byHash.get(hash);
      if (collisions === undefined) byHash.set(hash, [entry]);
      else collisions.push(entry);
    }
    byLength.set(length, byHash);
  }

  const resolver: TokenResolver = (payload) => {
    const bucket = byLength.get(payload.length);
    if (bucket === undefined) return null;
    const candidates = bucket instanceof Map ? bucket.get(hashBytes(payload)) : bucket;
    if (candidates === undefined) return null;
    for (let e = 0; e < candidates.length; e++) {
      const key = candidates[e].key;
      let i = 0;
      while (i < key.length && key[i] === payload[i]) i++;
      if (i === key.length) return candidates[e].value;
    }
    return null;
  };
  BORROWING.add(resolver);
  return resolver;
}

/**
 * Pipe a decoded response body (as `fetch()` yields it) through a substitution
 * and drop headers that described the original bytes. Throws on partial content.
 */
export function substituteResponse(
  response: Response,
  substitution: TokenTransformOptions | ReadableWritablePair<Uint8Array, Uint8Array>,
): Response {
  if (response.body === null) return response;
  if (response.status === 206 || response.headers.has("content-range")) {
    throw new TypeError("partial content cannot be rewritten: request the full representation");
  }

  const transform =
    "readable" in substitution && "writable" in substitution
      ? substitution
      : createTokenStream(substitution);

  const headers = new Headers(response.headers);
  for (const name of STALE_HEADERS) headers.delete(name);

  return new Response(response.body.pipeThrough(transform), {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}
