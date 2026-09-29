import { EMPTY, encodeText } from "./bytes.ts";
import { type Controller, detachedError, type FlowBody, Session } from "./flow.ts";
import { Emitter } from "./output.ts";
import type { ResolveContext, ResolveErrorHandler, TokenTransformer } from "./types.ts";

/** Bytes held behind pending slots before scanning waits. */
const HELD_LIMIT = 65536;

type Piece = Uint8Array | string;
type PieceIterator = AsyncIterator<Piece> | Iterator<Piece>;

/** A pending match plus the output queued behind it. */
interface Slot {
  owner: Lookahead | undefined;
  /** undefined while pending. A drained stream becomes EMPTY. */
  value: Uint8Array | PieceIterator | undefined;
  /** Rejected: value is the match, emitted verbatim. */
  verbatim: boolean;
  /** Copy for onResolveError and a late null. */
  payload: Uint8Array;
  /** Borrowed view the resolver got. Settling to it emits `payload` instead. */
  arg: Uint8Array | undefined;
  reading: boolean;
  /** Already went through onResolveError. */
  recovered: boolean;
  after: Uint8Array[];
  /** Why the slot was released, passed to a late stream. */
  reason: unknown;
  /** Bytes this slot contributes to `held`. */
  held: number;
}

interface Waiter {
  resolve: () => void;
  reject: (reason: unknown) => void;
}

const noop = () => {};

function isObject(value: unknown): value is object {
  return value !== null && (typeof value === "object" || typeof value === "function");
}

function isAsyncIterable(value: object): value is AsyncIterable<Piece> {
  return Symbol.asyncIterator in value;
}

function isIterable(value: object): value is Iterable<Piece> {
  return Symbol.iterator in value;
}

/** The iterator of a stream or iterable result. Bytes and strings are not iterables here. */
function iteratorOf(value: unknown): PieceIterator | undefined {
  if (!isObject(value) || value instanceof Uint8Array) return undefined;
  if (isAsyncIterable(value)) return value[Symbol.asyncIterator]();
  return isIterable(value) ? value[Symbol.iterator]() : undefined;
}

function invalid(value: unknown): TypeError {
  const kind = value === null ? "null" : Array.isArray(value) ? "array" : typeof value;
  return new TypeError(
    `resolve must return Uint8Array, string, null, a stream, or a promise of one; got ${kind}`,
  );
}

/** @internal A native promise for a thenable, or undefined. Reads `then` once. */
export function toPromise(value: unknown): Promise<unknown> | undefined {
  if (value instanceof Promise) return Promise.resolve(value);
  if (!isObject(value) || value instanceof Uint8Array) return undefined;
  const then: unknown = Reflect.get(value, "then");
  if (typeof then !== "function") return undefined;
  // biome-ignore lint/suspicious/noThenProperty: intentional thenable assimilation
  return Promise.resolve({ then: then.bind(value) });
}

function quiet(pending: unknown): void {
  if (pending instanceof Promise) Promise.prototype.then.call(pending, undefined, noop);
}

/** Cancel a stream nobody will read. A ReadableStream gets `reason` as its cancel reason. */
function closeIterator(iter: PieceIterator | undefined, reason: unknown): void {
  try {
    quiet(iter?.return?.(reason));
  } catch {}
}

function releaseSlot(slot: Slot, reason: unknown): void {
  slot.owner = undefined;
  slot.reason = reason;
  slot.after.length = 0;
  slot.payload = EMPTY;
  slot.arg = undefined;
  const value = slot.value;
  slot.value = EMPTY;
  if (value !== undefined && !(value instanceof Uint8Array)) closeIterator(value, reason);
}

/** The signal is created on first read, so an unused context costs nothing. */
class StreamContext implements ResolveContext {
  #controller: AbortController | undefined;
  #stopped: { reason: unknown } | undefined;

  get signal(): AbortSignal {
    if (this.#controller === undefined) {
      this.#controller = new AbortController();
      if (this.#stopped !== undefined) this.#controller.abort(this.#stopped.reason);
    }
    return this.#controller.signal;
  }

  abort(reason: unknown): void {
    this.#stopped ??= { reason };
    this.#controller?.abort(reason);
  }
}

/** @internal Settings the lookahead needs from either scanner. */
export interface LookaheadSettings {
  mergeBytes: number;
  concurrency: number;
  onResolveError: ResolveErrorHandler | undefined;
}

/** @internal Ordered output with lookahead. Scanners supply the matching. */
export abstract class Lookahead {
  paused = false;
  protected readonly out: Emitter;
  protected readonly context = new StreamContext();
  protected replaced = 0;
  protected rejected = 0;
  protected bytesIn = 0;
  /** The input is done, and `step` works through the held tail. */
  protected flushing = false;
  private readonly concurrency: number;
  private readonly onResolveError: ResolveErrorHandler | undefined;
  private readonly onClose: (failure?: { reason: unknown }) => void;
  private scanning = false;
  protected slots: Slot[] = [];
  /** Slots still pending or streaming. */
  private live = 0;
  private held = 0;
  /** Inside transform, resume or flush, until its result settles. */
  private active = false;
  private waiter: Waiter | undefined;
  private finished = false;
  protected stopped = false;
  private stopReason: unknown;
  private readonly toOut = (part: Uint8Array) => this.out.emit(part);
  private readonly toQueue = (part: Uint8Array) => this.emit(part);

  constructor(settings: LookaheadSettings, onClose: (failure?: { reason: unknown }) => void) {
    this.out = new Emitter(settings.mergeBytes);
    this.concurrency = settings.concurrency;
    this.onResolveError = settings.onResolveError;
    this.onClose = onClose;
  }

  /** Position the scanner at the start of `chunk`. */
  protected abstract begin(chunk: Uint8Array): void;
  /** End of input: emit or stage the held tail. True when `step` has more to do. */
  protected abstract beginFlush(): boolean;
  /** Continue the chunk or the tail. True when done, false on a halt. */
  protected abstract step(): boolean;
  /** Emit a rejected match verbatim. */
  protected abstract verbatim(payload: Uint8Array, sink: (part: Uint8Array) => void): void;
  /** Deliver the counts. */
  protected abstract report(bytesOut: number): void;
  /** Drop scanner state and buffers. */
  protected abstract clear(): void;

  transform(chunk: Uint8Array, ctrl: Controller): void | Promise<void> {
    if (this.stopped) return Promise.reject(this.stopReason);
    if (this.flushing) return Promise.reject(new TypeError("transformer is no longer active"));
    try {
      if (!(chunk instanceof Uint8Array)) throw new TypeError("chunk must be a Uint8Array");
      this.out.ctrl = ctrl;
      this.bytesIn += chunk.length;
      this.begin(chunk);
      this.scanning = true;
      return this.run();
    } catch (error) {
      return this.failed(error);
    }
  }

  resume(ctrl: Controller): void | Promise<void> {
    if (this.stopped) return Promise.reject(this.stopReason);
    if (!this.paused) return;
    this.out.ctrl = ctrl;
    try {
      return this.run();
    } catch (error) {
      return this.failed(error);
    }
  }

  flush(ctrl: Controller): void | Promise<void> {
    if (this.stopped) throw this.stopReason;
    this.out.ctrl = ctrl;
    try {
      this.flushing = true;
      this.scanning = this.beginFlush();
      return this.run();
    } catch (error) {
      this.fail(error, false);
      throw this.stopReason;
    }
  }

  cancel(reason: unknown = detachedError("transformer cancelled")): void {
    this.fail(reason, false);
  }

  /** Output has room again. Continues a background drain. */
  poke(): void {
    if (!this.active && this.slots.length > 0) this.wake();
  }

  /** Output is blocked or the lookahead is full. */
  protected halted(): boolean {
    return this.out.blocked || (this.slots.length > 0 && this.full());
  }

  /** Route output behind the last slot, or straight out. */
  protected emit(part: Uint8Array): void {
    const n = this.slots.length;
    if (n === 0) this.out.emit(part);
    else if (part.length > 0) {
      const slot = this.slots[n - 1];
      slot.after.push(part);
      slot.held += part.length;
      this.held += part.length;
    }
  }

  /** Emit src[from..to). Reused buffers are copied, caller chunks go out by reference. */
  protected emitSpan(
    src: Uint8Array<ArrayBufferLike>,
    from: number,
    to: number,
    owned: boolean,
  ): void {
    if (owned) this.emit(src.slice(from, to));
    else if (this.slots.length === 0) this.out.emitRange(src, from, to);
    else this.emit(src.subarray(from, to));
  }

  /**
   * Take a resolver result. `failed` means the resolver threw `error`. `payload`
   * is a retainable copy of the match, `arg` the borrowed view if any.
   */
  protected accept(
    value: unknown,
    promise: Promise<unknown> | undefined,
    failed: boolean,
    error: unknown,
    payload: Uint8Array,
    arg?: Uint8Array,
  ): void {
    if (this.stopped) {
      quiet(promise);
      if (!failed) closeIterator(iteratorOf(value), this.stopReason);
      throw this.stopReason;
    }
    let recovered = false;
    if (failed) {
      if (this.onResolveError === undefined) throw error;
      recovered = true;
      value = this.onResolveError(error, payload, this.context);
      promise = toPromise(value);
    }
    if (this.stopped) {
      quiet(promise);
      closeIterator(iteratorOf(value), this.stopReason);
      throw this.stopReason;
    }
    if (promise !== undefined) {
      const slot = this.addSlot(undefined, payload);
      slot.recovered = recovered;
      slot.arg = arg;
      this.watch(slot, promise);
      this.out.flushInitial();
      return;
    }
    if (value === null) {
      // Null is atomic: verbatim, and the span is not re-scanned.
      this.verbatim(payload, this.toQueue);
      this.rejected++;
      return;
    }
    if (value instanceof Uint8Array) this.emit(value);
    else if (typeof value === "string") {
      if (value.length > 0) this.emit(encodeText(value, "resolve result"));
    } else {
      const iter = iteratorOf(value);
      if (iter === undefined) throw invalid(value);
      this.addSlot(iter, EMPTY);
    }
    this.replaced++;
  }

  private run(): void | Promise<void> {
    this.active = true;
    this.advance();
    if (this.stopped) throw this.stopReason;
    if (this.ready()) {
      this.idle();
      return;
    }
    this.out.flushInitial();
    return new Promise<void>((resolve, reject) => {
      this.waiter = { resolve, reject };
    });
  }

  private failed(error: unknown): Promise<never> {
    this.fail(error, false);
    return Promise.reject(this.stopReason);
  }

  /** Drain, scan, and finish a flush once everything is out. */
  private advance(): void {
    this.progress();
    // Background wakes keep `paused`: the host still owes a resume.
    if (this.active) {
      this.paused = this.out.blocked && (this.scanning || this.headReady()) && !this.stopped;
    }
    if (
      this.flushing &&
      !this.scanning &&
      !this.finished &&
      !this.stopped &&
      this.slots.length === 0
    ) {
      this.finish();
    }
  }

  private progress(): void {
    for (;;) {
      this.drain();
      if (this.stopped || !this.active || !this.scanning || this.halted()) return;
      if (!this.step()) continue;
      if (this.slots.length === 0) this.out.flush();
      this.scanning = false;
    }
  }

  private ready(): boolean {
    if (this.paused || this.finished) return true;
    if (this.flushing) return false;
    return !this.scanning && !(this.slots.length > 0 && this.full());
  }

  /** The current call settled. Only a pending queue keeps the controller. */
  private idle(): void {
    this.active = false;
    this.out.flush();
    if (this.slots.length === 0 && !this.scanning) this.out.ctrl = undefined;
  }

  /** A slot settled or a stream piece arrived. */
  private wake(): void {
    if (this.stopped || this.finished) return;
    try {
      this.advance();
      if (this.stopped) return;
      if (this.waiter === undefined) {
        if (!this.active) this.idle();
        return;
      }
      if (!this.ready()) return;
      const waiter = this.waiter;
      this.waiter = undefined;
      this.idle();
      waiter.resolve();
    } catch (error) {
      this.fail(error);
    }
  }

  private finish(): void {
    this.finished = true;
    try {
      this.out.flush();
    } finally {
      this.reset();
      // No error here: its stack trace would retain this scanner.
      this.onClose();
    }
    this.report(this.out.bytesOut);
  }

  private fail(reason: unknown, notify = true): void {
    if (this.stopped) return;
    // With no call to reject, the failure goes to the output directly.
    const ctrl = notify && !this.active && this.waiter === undefined ? this.out.ctrl : undefined;
    this.stopped = true;
    this.stopReason = reason;
    this.reset(reason);
    this.context.abort(reason);
    this.onClose({ reason });
    const waiter = this.waiter;
    this.waiter = undefined;
    waiter?.reject(reason);
    try {
      ctrl?.error(reason);
    } catch {}
  }

  private full(): boolean {
    return this.live >= this.concurrency || this.held > HELD_LIMIT;
  }

  private headReady(): boolean {
    const head = this.slots[0];
    return head !== undefined && head.value !== undefined && !head.reading;
  }

  /** Emit settled slots at the head of the queue, in order. */
  private drain(): void {
    const slots = this.slots;
    while (slots.length > 0 && !this.stopped) {
      const slot = slots[0];
      const value = slot.value;
      if (value === undefined || slot.reading || this.out.blocked) return;
      if (!(value instanceof Uint8Array)) {
        this.pull(slot, value);
        return;
      }
      if (slot.verbatim) this.verbatim(value, this.toOut);
      else this.out.emit(value);
      const after = slot.after;
      for (let p = 0; p < after.length && !this.stopped; p++) this.out.emit(after[p]);
      this.held -= slot.held;
      slot.owner = undefined;
      slots.shift();
    }
  }

  /** Read one piece of the head stream. */
  private pull(slot: Slot, iter: PieceIterator): void {
    slot.reading = true;
    Promise.prototype.then.call(
      Promise.resolve(iter.next()),
      (result: IteratorResult<Piece>) => slot.owner?.piece(slot, result),
      (error: unknown) => slot.owner?.fail(error),
    );
  }

  private piece(slot: Slot, result: IteratorResult<Piece>): void {
    slot.reading = false;
    try {
      if (result.done) {
        slot.value = EMPTY;
        this.live--;
      } else if (result.value instanceof Uint8Array) this.out.emit(result.value);
      else if (typeof result.value === "string") {
        if (result.value.length > 0) this.out.emit(encodeText(result.value, "stream piece"));
      } else throw new TypeError("replacement stream must yield Uint8Array or string chunks");
    } catch (error) {
      this.fail(error);
      return;
    }
    this.wake();
  }

  private addSlot(value: PieceIterator | undefined, payload: Uint8Array): Slot {
    const slot: Slot = {
      owner: this,
      value,
      verbatim: false,
      payload,
      arg: undefined,
      reading: false,
      recovered: false,
      after: [],
      reason: undefined,
      held: 0,
    };
    this.slots.push(slot);
    this.live++;
    return slot;
  }

  private watch(slot: Slot, promise: Promise<unknown>): void {
    Promise.prototype.then.call(
      promise,
      (value: unknown) => {
        const owner = slot.owner;
        if (owner !== undefined) owner.settle(slot, value);
        else closeIterator(iteratorOf(value), slot.reason);
      },
      (error: unknown) => slot.owner?.rejectSlot(slot, error),
    );
  }

  private settle(slot: Slot, value: unknown): void {
    let bytes: Uint8Array | undefined;
    try {
      if (value === null) {
        slot.verbatim = true;
        bytes = slot.payload;
      } else if (value instanceof Uint8Array) bytes = value === slot.arg ? slot.payload : value;
      else if (typeof value === "string") bytes = encodeText(value, "resolve result");
      else {
        const iter = iteratorOf(value);
        if (iter === undefined) throw invalid(value);
        slot.value = iter;
      }
    } catch (error) {
      this.fail(error);
      return;
    }
    if (bytes !== undefined) {
      slot.value = bytes;
      let size = 0;
      if (slot.verbatim) this.verbatim(bytes, (part) => (size += part.length));
      else size = bytes.length;
      this.live--;
      slot.held += size;
      this.held += size;
    }
    slot.payload = EMPTY;
    slot.arg = undefined;
    if (slot.verbatim) this.rejected++;
    else this.replaced++;
    this.wake();
  }

  private rejectSlot(slot: Slot, error: unknown): void {
    if (this.onResolveError === undefined || slot.recovered) {
      this.fail(error);
      return;
    }
    slot.recovered = true;
    let value: unknown;
    let promise: Promise<unknown> | undefined;
    try {
      value = this.onResolveError(error, slot.payload, this.context);
      promise = toPromise(value);
    } catch (failure) {
      this.fail(failure);
      return;
    }
    if (this.stopped) quiet(promise);
    else if (promise !== undefined) this.watch(slot, promise);
    else this.settle(slot, value);
  }

  private reset(reason?: unknown): void {
    for (const slot of this.slots) releaseSlot(slot, reason);
    this.slots = [];
    this.live = 0;
    this.held = 0;
    this.paused = false;
    this.active = false;
    this.scanning = false;
    this.clear();
    this.out.reset();
  }
}

/** @internal Single-use transformer body around a scanner. */
export function lookaheadBody<S extends Lookahead>(
  signal: AbortSignal | undefined,
  create: (onClose: (failure?: { reason: unknown }) => void) => S,
): TokenTransformer & FlowBody {
  const session = new Session<S>(signal);
  session.open(create(session.close));
  return {
    get paused() {
      return session.scanner?.paused ?? false;
    },
    resume: (ctrl: Controller) => {
      const s = session.scanner;
      return s === undefined ? Promise.reject(session.inactive()) : s.resume(ctrl);
    },
    transform: (chunk: Uint8Array, ctrl: Controller) => {
      const s = session.scanner;
      return s === undefined ? Promise.reject(session.inactive()) : s.transform(chunk, ctrl);
    },
    flush: (ctrl: Controller) => {
      const s = session.scanner;
      if (s === undefined) throw session.inactive();
      return s.flush(ctrl);
    },
    poke: () => session.scanner?.poke(),
    cancel: (reason?: unknown) => session.cancel(reason),
  };
}
