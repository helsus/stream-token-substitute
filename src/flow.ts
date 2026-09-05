export const BLOCKED = Symbol("output backpressure");
export const BUFFER_LIMIT = Symbol("output buffer limit");

export type OutputController = TransformStreamDefaultController<Uint8Array> & {
  [BLOCKED]?: () => boolean;
  [BUFFER_LIMIT]?: number;
};

export interface FlowBody {
  transform(chunk: Uint8Array, ctrl: OutputController): void | Promise<void>;
  flush(ctrl: OutputController): void;
  cancel?(reason?: unknown): void;
  readonly paused?: boolean;
  resume?(ctrl: OutputController): void | Promise<void>;
}

/** Byte-budgeted output, with one atomic replacement of overshoot. */
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
  const ctrl = {
    enqueue: (part: Uint8Array) => {
      demand = false;
      output.enqueue(part);
    },
    [BLOCKED]: blocked,
    [BUFFER_LIMIT]: 16384,
  } as OutputController;
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
        body.flush(ctrl);
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
