import { createAsyncTokenTransformStream } from "../src/async-transformer.ts";
import { createTokenTransformStream } from "../src/transformer.ts";
import type {
  AsyncTokenTransformOptions,
  TokenResolver,
  TokenTransformOptions,
} from "../src/types.ts";

export const encoder = new TextEncoder();
export const decoder = new TextDecoder();
export const bytes = (s: string) => encoder.encode(s);

export function concat(parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let w = 0;
  for (const p of parts) {
    out.set(p, w);
    w += p.length;
  }
  return out;
}

/** Pipe chunks through the transformer, return every enqueued chunk. */
export async function runStreamParts(
  chunks: Uint8Array[],
  options: TokenTransformOptions,
): Promise<Uint8Array[]> {
  const tx = createTokenTransformStream(options);
  const writer = tx.writable.getWriter();
  const reader = tx.readable.getReader();
  const out: Uint8Array[] = [];
  const pump = (async () => {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      out.push(value);
    }
  })();
  try {
    for (const c of chunks) await writer.write(c);
    await writer.close();
  } catch (error) {
    // The reader rejects with the same cause; the write error is the useful
    // one, so swallow the duplicate rather than leave it unhandled.
    pump.catch(() => {});
    throw error;
  }
  await pump;
  return out;
}

export async function runStream(
  chunks: Uint8Array[],
  options: TokenTransformOptions,
): Promise<Uint8Array> {
  return concat(await runStreamParts(chunks, options));
}

/** Same, through the async transformer. */
export async function runAsyncStreamParts(
  chunks: Uint8Array[],
  options: AsyncTokenTransformOptions,
): Promise<Uint8Array[]> {
  const tx = createAsyncTokenTransformStream(options);
  const writer = tx.writable.getWriter();
  const reader = tx.readable.getReader();
  const out: Uint8Array[] = [];
  const pump = (async () => {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      out.push(value);
    }
  })();
  try {
    for (const c of chunks) await writer.write(c);
    await writer.close();
  } catch (error) {
    // The reader rejects with the same cause; the write error is the useful
    // one, so swallow the duplicate rather than leave it unhandled.
    pump.catch(() => {});
    throw error;
  }
  await pump;
  return out;
}

export async function runAsyncStream(
  chunks: Uint8Array[],
  options: AsyncTokenTransformOptions,
): Promise<Uint8Array> {
  return concat(await runAsyncStreamParts(chunks, options));
}

/** Wrap a sync resolver so every token suspends the scanner. `mode` picks how
 *  far the resume is pushed out: a resolved promise is a microtask, a timer is a
 *  full macrotask turn, and "sync" returns the value directly so the async
 *  transformer takes its non-suspending path. */
export function deferResolver(
  resolve: TokenResolver,
  mode: "sync" | "micro" | "macro" = "micro",
): (payload: Uint8Array) => Uint8Array | null | Promise<Uint8Array | null> {
  return (payload) => {
    // The payload handed to an async resolver must survive the await, so a
    // correct implementation can read it after suspending. Read it late on
    // purpose: if it were the shared scratch, this would see the next token.
    if (mode === "sync") return resolve(payload);
    if (mode === "micro") return Promise.resolve().then(() => resolve(payload));
    return new Promise((done) => setTimeout(() => done(resolve(payload)), 0));
  };
}

export function splitAt(input: Uint8Array, cuts: number[]): Uint8Array[] {
  const parts: Uint8Array[] = [];
  let prev = 0;
  for (const c of cuts) {
    parts.push(input.subarray(prev, c));
    prev = c;
  }
  parts.push(input.subarray(prev));
  return parts;
}

/** mulberry32 */
export function prng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function hex(u8: Uint8Array): string {
  return Array.from(u8, (x) => x.toString(16).padStart(2, "0")).join(" ");
}
export function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
