import { type Duplex, Transform, type TransformCallback } from "node:stream";
import { BLOCKED, BUFFER_LIMIT, type FlowBody, type OutputController } from "./flow.ts";
import { createLiteralTransformer, type LiteralTransformOptions } from "./literals.ts";
import { createTokenTransformer } from "./transformer.ts";
import type { TokenTransformOptions } from "./types.ts";

function streamError(reason: unknown): Error {
  return reason instanceof Error ? reason : new Error("substitution failed", { cause: reason });
}

/** The Node stream options that apply. Chunks are always bytes. */
export interface NodeStreamOptions {
  /** Bytes buffered on each side before backpressure. */
  highWaterMark?: number;
  /** Destroys the stream when aborted. */
  signal?: AbortSignal;
}

class NodeController implements OutputController {
  blocked = false;
  readonly #stream: Transform;
  readonly [BUFFER_LIMIT]: number;

  constructor(stream: Transform) {
    this.#stream = stream;
    this[BUFFER_LIMIT] = Math.max(1, stream.readableHighWaterMark);
  }

  get desiredSize(): number {
    return this.#stream.readableHighWaterMark - this.#stream.readableLength;
  }

  enqueue(part: Uint8Array): void {
    this.blocked = !this.#stream.push(part);
  }

  error(reason?: unknown): void {
    this.#stream.destroy(streamError(reason));
  }

  terminate(): void {
    this.#stream.push(null);
  }

  [BLOCKED] = (): boolean => this.blocked;
}

// Not Duplex.fromWeb: a rejected writer.ready there escapes as an uncaught TypeError.
class SubstituteTransform extends Transform {
  #body: FlowBody | undefined;
  readonly #controller: NodeController;
  #continue: (() => void) | undefined;
  #callback: TransformCallback | undefined;

  constructor(body: FlowBody, stream: NodeStreamOptions = {}) {
    super({
      highWaterMark: stream.highWaterMark,
      signal: stream.signal,
      objectMode: false,
      decodeStrings: true,
    });
    this.#body = body;
    this.#controller = new NodeController(this);
  }

  override _transform(chunk: Uint8Array, _encoding: string, callback: TransformCallback): void {
    this.#callback = callback;
    this.#run((body) => body.transform(chunk, this.#controller));
  }

  #run(action: (body: FlowBody) => void | Promise<void>): void {
    const body = this.#body;
    if (body === undefined) {
      this.#finish(new Error("stream destroyed"));
      return;
    }
    let pending: void | Promise<void>;
    try {
      pending = action(body);
    } catch (error) {
      this.#finish(streamError(error));
      return;
    }
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
      const resume = () => this.#run((body) => body.resume?.(this.#controller));
      if (this.#controller.blocked) this.#continue = resume;
      else resume();
    } else this.#finish();
  }

  override _read(size: number): void {
    this.#controller.blocked = false;
    const resume = this.#continue;
    this.#continue = undefined;
    if (resume !== undefined) resume();
    else super._read(size);
  }

  override _flush(callback: TransformCallback): void {
    this.#callback = callback;
    this.#run((body) => body.flush(this.#controller));
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

/** Token substitution as a Node `Transform`. Output parts are views, not copies. */
export function createTokenTransform(
  options: TokenTransformOptions,
  stream?: NodeStreamOptions,
): Duplex {
  return new SubstituteTransform(createTokenTransformer(options), stream);
}

/** Literal substitution as a Node `Transform`. */
export function createLiteralTransform(
  options: LiteralTransformOptions,
  stream?: NodeStreamOptions,
): Duplex {
  return new SubstituteTransform(createLiteralTransformer(options), stream);
}
