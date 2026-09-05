import { type Duplex, Transform, type TransformCallback } from "node:stream";
import { createAsyncTokenTransformer } from "./async-transformer.ts";
import { BLOCKED, BUFFER_LIMIT, type FlowBody } from "./flow.ts";
import { createNeedleTransformer, type NeedleTransformOptions } from "./needles.ts";
import { createTokenTransformer } from "./transformer.ts";
import type {
  AsyncTokenTransformer,
  AsyncTokenTransformOptions,
  TokenTransformer,
  TokenTransformOptions,
} from "./types.ts";

type Controller = TransformStreamDefaultController<Uint8Array>;
type Body = FlowBody;

function streamError(reason: unknown): Error {
  return reason instanceof Error ? reason : new Error("substitution failed", { cause: reason });
}

/**
 * The Node stream options that mean something here.
 *
 * The rest are fixed: the scanner takes `Uint8Array` chunks and emits them, so
 * object mode, encodings and string decoding would only break that contract.
 */
export interface NodeStreamOptions {
  /** Bytes buffered on each side before backpressure. The Node default is
   *  version-dependent. Raising it trades memory for fewer stalls on a fast source. */
  highWaterMark?: number;
  /** Destroys the stream when aborted, failing whatever is piping it. */
  signal?: AbortSignal;
}

/**
 * The transformer bodies driven by a Node `Transform` rather than by
 * `Duplex.fromWeb`.
 *
 * Not a style preference: `Duplex.fromWeb` registers one `done` as both the
 * fulfil and the reject handler of `writer.ready`, and its `writev` `done`
 * starts with `error.filter(...)`. A rejected `ready` therefore hands a lone
 * Error to a function expecting an array, and the TypeError escapes the promise
 * as an uncaught exception. `ready` rejects whenever the readable side is
 * abandoned mid-body, which for a server is just a client hanging up.
 *
 * The bodies only ever call `enqueue`, so the controller is one method.
 */
class SubstituteTransform extends Transform {
  #body: Body | undefined;
  readonly #controller: Controller;
  #blocked = false;
  #continue: (() => void) | undefined;
  #callback: TransformCallback | undefined;

  constructor(body: TokenTransformer | AsyncTokenTransformer, stream: NodeStreamOptions = {}) {
    super({
      highWaterMark: stream.highWaterMark,
      signal: stream.signal,
      objectMode: false,
      decodeStrings: true,
    });
    this.#body = body;
    this.#controller = {
      enqueue: (part: Uint8Array): void => {
        this.#blocked = !this.push(part);
      },
      [BLOCKED]: () => this.#blocked,
      [BUFFER_LIMIT]: Math.max(1, this.readableHighWaterMark),
    } as unknown as Controller;
  }

  override _transform(chunk: Uint8Array, _encoding: string, callback: TransformCallback): void {
    this.#callback = callback;
    this.#run(() => (this.#body as Body).transform(chunk, this.#controller));
  }

  #run(action: () => void | Promise<void>): void {
    let pending: void | Promise<void>;
    try {
      pending = action();
    } catch (error) {
      this.#finish(streamError(error));
      return;
    }
    // The sync body returns undefined and has already pushed everything.
    if (pending === undefined) this.#settle();
    else
      pending.then(
        () => this.#settle(),
        (error) => this.#finish(streamError(error)),
      );
  }

  #finish(error?: Error): void {
    const callback = this.#callback;
    this.#callback = undefined;
    callback?.(error);
  }

  #settle(): void {
    if (this.#body?.paused) {
      const resume = () => this.#run(() => this.#body?.resume?.(this.#controller));
      if (this.#blocked) this.#continue = resume;
      else resume();
    } else this.#finish();
  }

  override _read(size: number): void {
    this.#blocked = false;
    const resume = this.#continue;
    this.#continue = undefined;
    if (resume !== undefined) resume();
    else super._read(size);
  }

  override _flush(callback: TransformCallback): void {
    this.#callback = callback;
    this.#run(() => (this.#body as Body).flush(this.#controller));
  }

  override _destroy(error: Error | null, callback: (error?: Error | null) => void): void {
    // Late resolutions must not restart a destroyed stream.
    this.#body?.cancel?.(error ?? undefined);
    this.#body = undefined;
    this.#continue = undefined;
    this.#finish(error ?? new Error("stream destroyed"));
    callback(error);
  }
}

/** For `pipeline()` and `.pipe()`. A `Buffer` is a `Uint8Array`, so nothing is
 *  decoded on the way in; parts come out as `Buffer` views over the same bytes.
 *  Buffer ownership still applies: a source that recycles one pooled buffer must
 *  copy first. `fs`, `http` and `zlib` allocate per read. */
export function createTokenTransform(
  options: TokenTransformOptions,
  stream?: NodeStreamOptions,
): Duplex {
  return new SubstituteTransform(createTokenTransformer(options), stream);
}

/** Same, with an awaitable resolver. */
export function createAsyncTokenTransform(
  options: AsyncTokenTransformOptions,
  stream?: NodeStreamOptions,
): Duplex {
  return new SubstituteTransform(createAsyncTokenTransformer(options), stream);
}

/** Literal multi-pattern substitution as a Node stream. */
export function createNeedleTransform(
  options: NeedleTransformOptions,
  stream?: NodeStreamOptions,
): Duplex {
  return new SubstituteTransform(createNeedleTransformer(options), stream);
}
