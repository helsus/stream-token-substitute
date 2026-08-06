export const EMPTY = new Uint8Array(0);

/** Below this a byte loop beats subarray().set(): the view it allocates costs
 *  more than the bytes moved. */
const COPY_LOOP_MAX = 128;

/** Copy src[start..end) into dst at w. Returns the new write position. */
export function copyBytes(
  dst: Uint8Array,
  w: number,
  src: Uint8Array<ArrayBufferLike>,
  start: number,
  end: number,
): number {
  const len = end - start;
  if (len < COPY_LOOP_MAX) {
    for (let p = start; p < end; p++) dst[w++] = src[p];
    return w;
  }
  dst.set(start === 0 && end === src.length ? src : src.subarray(start, end), w);
  return w + len;
}

/** Grow a scratch buffer to `need`, preserving `used` bytes. */
export function grow(
  buf: Uint8Array<ArrayBuffer>,
  used: number,
  need: number,
  min: number,
): Uint8Array<ArrayBuffer> {
  if (need <= buf.length) return buf;
  let size = buf.length === 0 ? min : buf.length * 2;
  if (size < need) size = need;
  const next = new Uint8Array(size);
  next.set(buf.subarray(0, used));
  return next;
}

/** Make room for `head` bytes at the front of a pushback queue, relocating its
 *  unconsumed tail to `head`. In place when it fits. */
export function requeue(
  queue: Uint8Array<ArrayBuffer>,
  head: number,
  rest: number,
  restStart: number,
  min: number,
): Uint8Array<ArrayBuffer> {
  if (queue.length < head + rest) {
    const next = new Uint8Array(Math.max(head + rest, queue.length * 2, min));
    if (rest > 0) next.set(queue.subarray(restStart, restStart + rest), head);
    return next;
  }
  if (rest > 0) queue.copyWithin(head, restStart, restStart + rest);
  return queue;
}

/** Lazy: EdgeWorkers has no global TextEncoder, so constructing one at module
 *  scope would throw on import. */
let encoder: TextEncoder | undefined;

/** Encode, or copy. Copied because these bytes are enqueued by reference. */
export function encodeText(value: string | Uint8Array, name: string): Uint8Array<ArrayBuffer> {
  if (typeof value === "string") {
    if (encoder === undefined) {
      if (typeof TextEncoder === "undefined") {
        throw new TypeError(
          `${name} must be a Uint8Array in runtimes without a global TextEncoder`,
        );
      }
      encoder = new TextEncoder();
    }
    return encoder.encode(value);
  }
  if (value instanceof Uint8Array) return new Uint8Array(value);
  throw new TypeError(`${name} must be a string or Uint8Array`);
}
