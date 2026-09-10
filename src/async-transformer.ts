import { flowStream } from "./flow.ts";
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

/** Reactions retain this detachable task, not the scanner. */
interface ResolutionTask {
  owner: AsyncSubstituter | undefined;
  resolve: (() => void) | undefined;
  reject: ((reason: unknown) => void) | undefined;
  onValue: (value: Uint8Array | null) => void;
  onError: (error: unknown) => void;
}

function resolutionTask(
  owner: AsyncSubstituter,
  resolve: () => void,
  reject: (reason: unknown) => void,
): ResolutionTask {
  const task: ResolutionTask = {
    owner,
    resolve,
    reject,
    onValue: (value) => task.owner?.advance(value),
    onError: (error) => task.owner?.rejectPending(error),
  };
  return task;
}

/** Scanner with detachable async resolution. */
class AsyncSubstituter extends Substituter {
  private pending: Promise<Uint8Array | null> | undefined;
  private pendingPayload: Uint8Array | undefined;
  private task: ResolutionTask | undefined;
  private stopped = false;
  private stopReason: unknown;
  private readonly onClose: (reason: unknown) => void;

  constructor(options: AsyncTokenTransformOptions, onClose: (reason: unknown) => void) {
    super(options as TokenTransformOptions);
    this.onClose = onClose;
  }

  protected override invokeResolve(): Uint8Array | null {
    const payload = this.payloadCopy();
    try {
      const value = this.resolve(payload);
      if (value instanceof Promise) {
        // Normalize subclasses, while leaving ordinary native promises alone.
        this.pending = Promise.resolve(value);
      } else {
        const then =
          value !== null && (typeof value === "object" || typeof value === "function")
            ? (value as PromiseLike<Uint8Array | null>).then
            : undefined;
        if (typeof then !== "function") {
          if (this.stopped) throw this.stopReason;
          return value as Uint8Array | null;
        }
        // Read the original getter once; preserve the thenable's receiver.
        // biome-ignore lint/suspicious/noThenProperty: intentional thenable assimilation
        this.pending = Promise.resolve({ then: then.bind(value) });
      }
    } catch (error) {
      if (this.stopped) throw this.stopReason;
      if (this.onResolveError === undefined) throw error;
      const recovered = this.onResolveError(error, payload);
      if (this.stopped) throw this.stopReason;
      return recovered;
    }
    if (this.stopped) throw this.stopReason;
    this.pendingPayload = this.onResolveError === undefined ? undefined : payload;
    this.suspended = true;
    return null;
  }

  transformAsync(chunk: Uint8Array | undefined, ctrl: Controller): Promise<void> {
    try {
      if (chunk === undefined) this.resumeOutput(ctrl);
      else {
        this.begin(chunk, ctrl);
        this.pump();
      }
      if (this.stopped) return this.stoppedResult();
      if (!this.suspended) return Promise.resolve();
      return new Promise<void>((resolve, reject) => {
        const task = resolutionTask(this, resolve, reject);
        this.task = task;
        try {
          this.watch(task);
        } catch (error) {
          this.fail(error);
        }
      });
    } catch (error) {
      if (this.stopped) return this.stoppedResult();
      this.fail(error);
      return Promise.reject(error);
    }
  }

  private watch(task: ResolutionTask): void {
    this.out.flushInitial();
    if (this.stopped) return;
    const pending = this.pending as Promise<Uint8Array | null>;
    this.pending = undefined;
    // Match native await semantics without capturing the scanner.
    Promise.prototype.then.call(pending, task.onValue, task.onError);
  }

  advance(value: Uint8Array | null): void {
    this.pendingPayload = undefined;
    try {
      this.resume(value);
      if (this.stopped) {
        this.discardPending();
        this.reset();
        return;
      }
      if (this.suspended) this.watch(this.task as ResolutionTask);
      else this.finishTask();
    } catch (error) {
      this.fail(error);
    }
  }

  rejectPending(error: unknown): void {
    if (this.onResolveError === undefined) {
      this.fail(error);
      return;
    }
    try {
      const recovered = this.onResolveError(error, this.pendingPayload as Uint8Array);
      if (!this.stopped) this.advance(recovered);
    } catch (failure) {
      this.fail(failure);
    }
  }

  private finishTask(reason?: unknown, failed = false): void {
    const task = this.task;
    if (task === undefined) return;
    this.task = undefined;
    const resolve = task.resolve;
    const reject = task.reject;
    task.owner = undefined;
    task.resolve = undefined;
    task.reject = undefined;
    if (failed) reject?.(reason);
    else resolve?.();
  }

  override cancel(reason: unknown = new Error("transformer cancelled")): void {
    this.fail(reason);
  }

  private discardPending(): void {
    if (this.pending !== undefined) Promise.prototype.then.call(this.pending, undefined, () => {});
    this.pending = undefined;
  }

  private stoppedResult(): Promise<void> {
    this.discardPending();
    this.reset();
    return Promise.reject(this.stopReason);
  }

  private fail(reason: unknown): void {
    this.discardPending();
    this.reset();
    if (this.stopped) return;
    this.stopped = true;
    this.stopReason = reason;
    this.pendingPayload = undefined;
    this.onClose(reason);
    this.finishTask(reason, true);
  }
}

/** Single-use body. cancel() detaches pending resolution without flushing. */
export function createAsyncTokenTransformer(
  options: AsyncTokenTransformOptions,
): AsyncTokenTransformer {
  let signal = options?.signal;
  if (
    signal !== undefined &&
    (typeof signal.addEventListener !== "function" ||
      typeof signal.removeEventListener !== "function" ||
      typeof signal.aborted !== "boolean")
  ) {
    throw new TypeError("signal must be an AbortSignal");
  }
  let s: AsyncSubstituter | undefined;
  let terminalReason: unknown;
  const onClose = (reason: unknown) => {
    terminalReason = reason;
    s = undefined;
    signal?.removeEventListener("abort", onAbort);
    signal = undefined;
  };
  const onAbort = () => s?.cancel(signal?.reason);
  s = new AsyncSubstituter(options, onClose);
  if (signal?.aborted) onAbort();
  else signal?.addEventListener("abort", onAbort, { once: true });
  const body = {
    get paused() {
      return s?.paused ?? false;
    },
    resume: (ctrl: Controller) =>
      s === undefined ? Promise.reject(terminalReason) : s.transformAsync(undefined, ctrl),
    transform: (chunk: Uint8Array, ctrl: Controller) =>
      s === undefined ? Promise.reject(terminalReason) : s.transformAsync(chunk, ctrl),
    flush: (ctrl: Controller) => {
      if (s === undefined) throw terminalReason;
      const active = s;
      onClose(new TypeError("transformer is no longer active"));
      active.flush(ctrl);
    },
    cancel: (reason?: unknown) => s?.cancel(reason),
  };
  return body;
}

/** Use signal where the runtime lacks Transformer.cancel. */
export function createAsyncTokenTransformStream(
  options: AsyncTokenTransformOptions,
): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>(createAsyncTokenTransformer(options));
}

/** Single-use pair with intra-chunk backpressure. */
export function createAsyncTokenStreamPair(
  options: AsyncTokenTransformOptions,
): ReadableWritablePair<Uint8Array, Uint8Array> {
  return flowStream(createAsyncTokenTransformer(options), options.signal);
}
