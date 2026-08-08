import { Substituter } from "./transformer.ts";
import type {
  AsyncTokenTransformer,
  AsyncTokenTransformOptions,
  TokenTransformOptions,
} from "./types.ts";

export type {
  AsyncTokenResolver,
  AsyncTokenTransformer,
  AsyncTokenTransformOptions,
} from "./types.ts";

type Controller = TransformStreamDefaultController<Uint8Array>;

function isThenable(value: unknown): value is Promise<Uint8Array | null> {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    typeof (value as PromiseLike<unknown>).then === "function"
  );
}

/**
 * The sync scanner, driven across awaits.
 *
 * The only suspend point is a completed token whose resolver returned a
 * thenable. `pump()` unwinds without advancing, the driver awaits, and
 * `resume()` re-enters with every field exactly as it was. A resolver that
 * answers synchronously never parks, so it pays only the thenable check.
 */
class AsyncSubstituter extends Substituter {
  private pending: Promise<Uint8Array | null> | null = null;

  protected override invokeResolve(): Uint8Array | null {
    // A copy, not the scratch view: the payload has to outlive the await, and
    // an async resolver is exactly the caller most likely to retain it.
    const payload = this.payloadCopy();
    let value: Uint8Array | null | Promise<Uint8Array | null>;
    try {
      value = this.resolve(payload);
    } catch (error) {
      if (this.onResolveError === undefined) throw error;
      return this.onResolveError(error, payload);
    }
    if (!isThenable(value)) return value;
    this.pending = this.settle(value, payload);
    this.suspended = true;
    return null;
  }

  private async settle(
    value: Promise<Uint8Array | null>,
    payload: Uint8Array,
  ): Promise<Uint8Array | null> {
    try {
      return await value;
    } catch (error) {
      if (this.onResolveError === undefined) throw error;
      return this.onResolveError(error, payload);
    }
  }

  async transformAsync(chunk: Uint8Array, ctrl: Controller): Promise<void> {
    this.begin(chunk, ctrl);
    this.pump();
    while (this.suspended) {
      const value = await (this.pending as Promise<Uint8Array | null>);
      this.pending = null;
      // Finishes the parked token and scans on; may park again.
      this.resume(value);
    }
  }
}

/** Async transformer body, for runtimes where `TransformStream` is not a global.
 *  Construct one per stream. `resolve` may answer synchronously; only a returned
 *  thenable suspends the scan. */
export function createAsyncTokenTransformer(
  options: AsyncTokenTransformOptions,
): AsyncTokenTransformer {
  const s = new AsyncSubstituter(options as TokenTransformOptions);
  return {
    transform: (chunk, ctrl) => s.transformAsync(chunk, ctrl),
    flush: (ctrl) => s.flush(ctrl),
  };
}

/** Single-use TransformStream with an awaitable resolver. Construct one per
 *  stream. Backpressure holds in both directions: the next chunk is not written
 *  until this one is scanned and every token in it resolved, and it does not
 *  settle while the readable is over its high-water mark. */
export function createAsyncTokenTransformStream(
  options: AsyncTokenTransformOptions,
): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>(createAsyncTokenTransformer(options));
}
