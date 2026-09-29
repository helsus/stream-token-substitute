/** @internal */
export function checkSignal(signal: AbortSignal | undefined): void {
  if (signal !== undefined && !(signal instanceof AbortSignal)) {
    throw new TypeError("signal must be an AbortSignal");
  }
}

/** @internal */
export type Controller = TransformStreamDefaultController<Uint8Array>;

/** @internal A controller that may report output backpressure. */
export interface OutputController extends Controller {
  blocked?(): boolean;
  bufferLimit?: number;
}

/** @internal */
export interface FlowBody {
  transform(chunk: Uint8Array, ctrl: OutputController): void | Promise<void>;
  flush(ctrl: OutputController): void | Promise<void>;
  cancel?(reason?: unknown): void;
  readonly paused?: boolean;
  resume?(ctrl: OutputController): void | Promise<void>;
  /** Output has room again, so a background drain may continue. */
  poke?(): void;
}

/** @internal An error whose stack is formatted now, so it does not pin the callers. */
export function detachedError(message: string, Kind: ErrorConstructor = Error): Error {
  const error = new Kind(message);
  void error.stack;
  return error;
}

/** @internal A single-use scanner with its abort wiring. Once closed, calls fail with `inactive()`. */
export class Session<S extends { cancel(reason?: unknown): void }> {
  scanner: S | undefined;
  #closed = false;
  #failure: { reason: unknown } | undefined;
  #signal: AbortSignal | undefined;
  readonly #onAbort = () => this.cancel(this.#signal?.reason);

  constructor(signal: AbortSignal | undefined) {
    checkSignal(signal);
    this.#signal = signal;
  }

  open(scanner: S): void {
    this.scanner = scanner;
    if (this.#signal?.aborted) this.#onAbort();
    else this.#signal?.addEventListener("abort", this.#onAbort, { once: true });
  }

  readonly close = (failure?: { reason: unknown }): void => {
    if (this.#closed) return;
    this.#closed = true;
    this.#failure = failure;
    this.scanner = undefined;
    this.#signal?.removeEventListener("abort", this.#onAbort);
    this.#signal = undefined;
  };

  cancel(reason?: unknown): void {
    this.scanner?.cancel(reason);
    this.close({ reason });
  }

  inactive(): unknown {
    return this.#failure === undefined
      ? detachedError("transformer is no longer active", TypeError)
      : this.#failure.reason;
  }
}

/** @internal Byte-budgeted output, with one atomic replacement of overshoot. */
export function flowStream(
  body: FlowBody,
  signal?: AbortSignal,
): ReadableWritablePair<Uint8Array, Uint8Array> {
  let output: ReadableStreamDefaultController<Uint8Array>;
  let input: WritableStreamDefaultController;
  let wake: (() => void) | undefined;
  let reject: ((reason: unknown) => void) | undefined;
  let stopped = false;
  let demand = false;
  let reason: unknown;
  const blocked = () => (output.desiredSize ?? 0) <= -16384;
  const ready = (initial = false) => {
    if (stopped) return Promise.reject(reason);
    if (initial ? demand : !blocked()) return Promise.resolve();
    return new Promise<void>((resolve, fail) => {
      wake = resolve;
      reject = fail;
    });
  };
  const cleanup = () => {
    input.signal?.removeEventListener("abort", abort);
    signal?.removeEventListener("abort", onSignal);
    signal = undefined;
  };
  const stop = (failure: unknown) => {
    if (stopped) return;
    stopped = true;
    reason = failure;
    body.cancel?.(failure);
    cleanup();
    output.error(failure);
    if (!input.signal?.aborted) input.error(failure);
    reject?.(failure);
    wake = reject = undefined;
  };
  const abort = () => stop(input.signal.reason);
  const onSignal = () => stop(signal?.reason);
  const ctrl: OutputController = {
    get desiredSize() {
      return output.desiredSize;
    },
    enqueue: (part: Uint8Array) => {
      demand = false;
      output.enqueue(part);
    },
    error: stop,
    terminate: () => output.close(),
    blocked,
    bufferLimit: 16384,
  };
  const drain = async () => {
    while (body.paused) {
      await ready();
      if (stopped) throw reason;
      await body.resume?.(ctrl);
    }
  };
  const readable = new ReadableStream<Uint8Array>(
    {
      start(controller) {
        output = controller;
      },
      pull() {
        demand = true;
        if (blocked()) return;
        const resolve = wake;
        wake = reject = undefined;
        resolve?.();
        body.poke?.();
      },
      cancel: stop,
    },
    { highWaterMark: 0, size: (part) => part.byteLength },
  );
  const writable = new WritableStream<Uint8Array>({
    start(controller) {
      input = controller;
      input.signal?.addEventListener("abort", abort, { once: true });
    },
    async write(chunk) {
      try {
        if (!demand) await ready(true);
        if (stopped) throw reason;
        await body.transform(chunk, ctrl);
        await drain();
        if (stopped) throw reason;
      } catch (error) {
        stop(error);
        throw error;
      }
    },
    async close() {
      try {
        await body.flush(ctrl);
        await drain();
        if (stopped) throw reason;
        output.close();
        cleanup();
      } catch (error) {
        stop(error);
        throw error;
      }
    },
    abort: stop,
  });
  if (signal?.aborted) onSignal();
  else signal?.addEventListener("abort", onSignal, { once: true });
  return { readable, writable };
}
