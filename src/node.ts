import { Duplex } from "node:stream";
import { createAsyncTokenTransformStream } from "./async-transformer.ts";
import { createNeedleTransformStream, type NeedleTransformOptions } from "./needles.ts";
import { createTokenTransformStream } from "./transformer.ts";
import type { AsyncTokenTransformOptions, TokenTransformOptions } from "./types.ts";

/** node:stream/web's TransformStream, structurally the DOM one. */
type WebPair = Parameters<typeof Duplex.fromWeb>[0];

const toDuplex = (tx: TransformStream<Uint8Array, Uint8Array>): Duplex =>
  Duplex.fromWeb(tx as unknown as WebPair);

/** For `pipeline()` and `.pipe()`. A `Buffer` is a `Uint8Array`, so nothing is
 *  decoded on the way in; parts come out as plain `Uint8Array`s. Buffer
 *  ownership still applies: a source that recycles one pooled buffer must copy
 *  first. `fs`, `http` and `zlib` allocate per read. */
export function createTokenTransform(options: TokenTransformOptions): Duplex {
  return toDuplex(createTokenTransformStream(options));
}

/** Same, with an awaitable resolver. */
export function createAsyncTokenTransform(options: AsyncTokenTransformOptions): Duplex {
  return toDuplex(createAsyncTokenTransformStream(options));
}

/** Literal multi-pattern substitution as a Node stream. */
export function createNeedleTransform(options: NeedleTransformOptions): Duplex {
  return toDuplex(createNeedleTransformStream(options));
}
