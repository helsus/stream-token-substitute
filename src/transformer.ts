import { copyBytes, EMPTY, encodeText, requeue } from "./bytes.ts";
import { checkSignal, type FlowBody, flowStream } from "./flow.ts";
import { COMPLETE, DelimiterMatcher, REJECTED } from "./matcher.ts";
import { Emitter } from "./output.ts";
import {
  compileOptions,
  type PayloadValidator,
  type ResolveErrorHandler,
  type TokenResolver,
  type TokenStats,
  type TokenTransformer,
  type TokenTransformOptions,
} from "./types.ts";

const OUTSIDE = 0;
const IN_TOKEN = 1;

/** Cached payload views per length, so validator calls allocate nothing. */
const VIEW_CACHE_MAX = 128;

/** Bytes held behind pending slots before scanning waits. */
const HELD_LIMIT = 65536;

type Controller = TransformStreamDefaultController<Uint8Array>;

/** A pending token plus the output queued behind it. */
interface Slot {
  owner: Substituter | undefined;
  /** undefined while pending. A drained stream becomes EMPTY. */
  value: Uint8Array | AsyncIterator<Uint8Array> | undefined;
  /** Rejected: value is the payload, emitted between the delimiters. */
  verbatim: boolean;
  /** Copy for onResolveError and a late null. */
  payload: Uint8Array;
  reading: boolean;
  /** Already went through onResolveError. */
  recovered: boolean;
  after: Uint8Array[];
  /** Bytes this slot contributes to `held`. */
  held: number;
}

const noop = () => {};

function isObject(value: unknown): value is object {
  return value !== null && (typeof value === "object" || typeof value === "function");
}

/** The iterator of a stream or async iterable result, or undefined. */
function iteratorOf(value: unknown): AsyncIterator<Uint8Array> | undefined {
  if (!isObject(value) || !(Symbol.asyncIterator in value)) return undefined;
  const make = value[Symbol.asyncIterator];
  return typeof make === "function" ? make.call(value) : undefined;
}

function invalid(value: unknown): TypeError {
  const kind = value === null ? "null" : Array.isArray(value) ? "array" : typeof value;
  return new TypeError(
    `resolve must return Uint8Array, string, null, a stream, or a promise of one; got ${kind}`,
  );
}

/** A native promise for a thenable, or undefined. Reads `then` once. */
function toPromise(value: unknown): Promise<unknown> | undefined {
  // Normalize subclasses, while leaving ordinary native promises alone.
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

/** Cancel a stream nobody will read. */
function closeIterator(iter: AsyncIterator<Uint8Array> | undefined): void {
  try {
    quiet(iter?.return?.());
  } catch {}
}

function releaseSlot(slot: Slot): void {
  slot.owner = undefined;
  slot.after.length = 0;
  slot.payload = EMPTY;
  const value = slot.value;
  slot.value = EMPTY;
  if (value !== undefined && !(value instanceof Uint8Array)) closeIterator(value);
}

interface Waiter {
  resolve: () => void;
  reject: (reason: unknown) => void;
}

/** Token scanner. State lives in fields, so it can halt and resume at a token boundary. */
export class Substituter {
  paused = false;
  private readonly openBytes: Uint8Array;
  private readonly closeBytes: Uint8Array;
  private readonly openFirst: number;
  private readonly closeFirst: number;
  private readonly resolve: TokenResolver;
  private readonly borrows: boolean;
  private readonly concurrency: number;
  private readonly validate: PayloadValidator | undefined;
  private readonly onResolveError: ResolveErrorHandler | undefined;
  private readonly onDone: ((stats: TokenStats) => void) | undefined;
  private readonly onClose: (failure?: { reason: unknown }) => void;
  private readonly maxPayloadBytes: number;
  private readonly scratchLimit: number;
  private readonly openM: DelimiterMatcher;
  private readonly closeM: DelimiterMatcher;

  /** Present only when `onDone` is, so the default path stays untouched. */
  private readonly stats: TokenStats | undefined;

  private state: typeof OUTSIDE | typeof IN_TOKEN = OUTSIDE;
  private carry = 0;
  // The payload is payload[base..payloadEnd).
  private payload: Uint8Array;
  private base = 0;
  private payloadEnd = 0;
  // Cached payload.subarray(0, len) by len, dropped when the scratch grows.
  private payloadViews: (Uint8Array | undefined)[] = [];

  // Open positions in the payload, so a cap abort without a validator stays linear.
  private readonly openScan: DelimiterMatcher | undefined;
  private scanned = 0;
  private opens: number[] = [];
  private openHead = 0;

  // Chunks may be backed by any ArrayBufferLike, including SharedArrayBuffer.
  private chunk: Uint8Array<ArrayBufferLike> = EMPTY;
  private scanning = false;

  // `srcOwned`: src is a reused scratch, so its spans are copied out.
  private src: Uint8Array<ArrayBufferLike> = EMPTY;
  private srcEnd = 0;
  private srcOwned = false;
  private i = 0;
  private flushStart = 0;

  private readonly out: Emitter;

  // Re-scan queue, live bytes are queue[0..queueLen).
  private inQueue = false;
  private queue = EMPTY;
  private queueLen = 0;
  // The chunk cursor, parked while the queue is scanned.
  private chunkI = 0;
  private chunkFlushStart = 0;
  /** An abort re-entered the queue, so the cursor must not advance. */
  private restarted = false;

  // Ordered output queue.
  private slots: Slot[] = [];
  /** Slots still pending or streaming. */
  private live = 0;
  private held = 0;

  /** Inside transform, resume or flush, until its result settles. */
  private active = false;
  private waiter: Waiter | undefined;
  private flushing = false;
  private finished = false;
  private stopped = false;
  private stopReason: unknown;

  constructor(
    options: TokenTransformOptions,
    onClose: (failure?: { reason: unknown }) => void = noop,
  ) {
    const compiled = compileOptions(options);
    this.openBytes = compiled.openBytes;
    this.closeBytes = compiled.closeBytes;
    this.openFirst = compiled.openBytes[0];
    this.closeFirst = compiled.closeBytes[0];
    this.resolve = compiled.resolve;
    this.borrows = compiled.borrows;
    this.concurrency = compiled.concurrency;
    this.validate = compiled.validate;
    this.onResolveError = compiled.onResolveError;
    this.onDone = compiled.onDone;
    this.onClose = onClose;
    this.maxPayloadBytes = compiled.maxPayloadBytes;
    // Twice the cap without a validator, so compaction stays amortized.
    this.scratchLimit =
      compiled.validate === undefined ? compiled.maxPayloadBytes * 2 : compiled.maxPayloadBytes;
    this.out = new Emitter(compiled.flushBytes);
    this.openM = new DelimiterMatcher(compiled.openBytes);
    this.closeM = new DelimiterMatcher(compiled.closeBytes);
    this.openScan =
      compiled.validate === undefined ? new DelimiterMatcher(compiled.openBytes) : undefined;
    this.stats =
      compiled.onDone === undefined
        ? undefined
        : { resolved: 0, rejected: 0, aborted: 0, bytesIn: 0, bytesOut: 0 };
    // The default cap fits the initial scratch, so steady state never grows it.
    this.payload = new Uint8Array(Math.min(compiled.maxPayloadBytes, 64));
  }

  transform(chunk: Uint8Array, ctrl: Controller): void | Promise<void> {
    if (this.stopped) return Promise.reject(this.stopReason);
    if (this.flushing) return Promise.reject(new TypeError("transformer is no longer active"));
    try {
      if (!(chunk instanceof Uint8Array)) throw new TypeError("chunk must be a Uint8Array");
      this.chunk = chunk;
      this.out.ctrl = ctrl;
      this.enter(chunk, chunk.length, false, 0, 0);
      this.scanning = true;
      if (this.stats !== undefined) this.stats.bytesIn += chunk.length;
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
      if (this.state === IN_TOKEN) {
        this.emit(this.openBytes);
        if (this.payloadEnd > this.base) this.emit(this.payloadCopy());
        if (this.closeM.k > 0) this.emit(this.closeBytes.slice(0, this.closeM.k));
      } else if (this.openM.k > 0) {
        this.emit(this.openBytes.slice(0, this.openM.k));
      }
      this.endToken();
      this.flushing = true;
      return this.run();
    } catch (error) {
      this.fail(error);
      throw this.stopReason;
    }
  }

  cancel(reason: unknown = new Error("transformer cancelled")): void {
    this.fail(reason);
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
    this.fail(error);
    return Promise.reject(this.stopReason);
  }

  /** Drain, scan, and finish a flush once everything is out. */
  private advance(): void {
    this.progress();
    // Background wakes keep `paused`: the host still owes a resume.
    if (this.active) {
      this.paused = this.out.blocked && (this.scanning || this.headReady()) && !this.stopped;
    }
    if (this.flushing && !this.finished && !this.stopped && this.slots.length === 0) {
      this.finish();
    }
  }

  private progress(): void {
    for (;;) {
      this.drain();
      if (this.stopped || !this.active || !this.scanning || this.halted()) return;
      if (!this.scan()) continue;
      this.park();
      // Must precede dropping the chunk: buffered spans are views into it.
      if (this.slots.length === 0) this.out.flush();
      this.scanning = false;
      this.chunk = EMPTY;
      this.src = EMPTY;
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
      if (this.stats !== undefined) this.stats.bytesOut = this.out.bytesOut;
      this.reset();
      // No error here: its stack trace would retain this scanner.
      this.onClose();
    }
    if (this.stats !== undefined) this.onDone?.(this.stats);
  }

  private fail(reason: unknown): void {
    if (this.stopped) return;
    this.stopped = true;
    this.stopReason = reason;
    this.reset();
    this.onClose({ reason });
    const waiter = this.waiter;
    this.waiter = undefined;
    waiter?.reject(reason);
  }

  private halted(): boolean {
    return this.out.blocked || (this.slots.length > 0 && this.full());
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
      if (slot.verbatim) this.out.emit(this.openBytes);
      this.out.emit(value);
      if (slot.verbatim) this.out.emit(this.closeBytes);
      const after = slot.after;
      for (let p = 0; p < after.length && !this.stopped; p++) this.out.emit(after[p]);
      this.held -= slot.held;
      slot.owner = undefined;
      slots.shift();
    }
  }

  /** Read one piece of the head stream. */
  private pull(slot: Slot, iter: AsyncIterator<Uint8Array>): void {
    slot.reading = true;
    Promise.prototype.then.call(
      Promise.resolve(iter.next()),
      (result: IteratorResult<Uint8Array>) => slot.owner?.piece(slot, result),
      (error: unknown) => slot.owner?.fail(error),
    );
  }

  private piece(slot: Slot, result: IteratorResult<Uint8Array>): void {
    slot.reading = false;
    try {
      if (result.done) {
        slot.value = EMPTY;
        this.live--;
      } else if (result.value instanceof Uint8Array) this.out.emit(result.value);
      else throw new TypeError("replacement stream must yield Uint8Array chunks");
    } catch (error) {
      this.fail(error);
      return;
    }
    this.wake();
  }

  /** Route output behind the last slot, or straight out. */
  private emit(part: Uint8Array): void {
    const n = this.slots.length;
    if (n === 0) this.out.emit(part);
    else if (part.length > 0) {
      const slot = this.slots[n - 1];
      slot.after.push(part);
      slot.held += part.length;
      this.held += part.length;
    }
  }

  private enter(
    src: Uint8Array<ArrayBufferLike>,
    end: number,
    owned: boolean,
    i: number,
    flushStart: number,
  ): void {
    this.src = src;
    this.srcEnd = end;
    this.srcOwned = owned;
    this.i = i;
    this.flushStart = flushStart;
  }

  /** True at the end of the chunk, false on a halt. */
  private scan(): boolean {
    for (;;) {
      // Re-read per iteration: an abort inside the queue replaces the buffer.
      while (this.i < this.srcEnd) {
        const src = this.src;
        const end = this.srcEnd;

        if (this.state === OUTSIDE) {
          // No held state: a contained delimiter settles by indexOf and compare.
          if (this.openM.k === 0) {
            const open = this.openBytes;
            const len = open.length;
            let matched = false;
            for (;;) {
              const idx = src.indexOf(this.openFirst, this.i);
              if (idx < 0 || idx >= end) {
                this.i = end;
                break;
              }
              if (idx + len > end) {
                this.i = idx;
                break;
              }
              let m = 1;
              while (m < len && src[idx + m] === open[m]) m++;
              if (m === len) {
                this.i = idx + len - 1;
                this.startToken();
                matched = true;
                break;
              }
              if (m > 1) {
                // Preserve the prefix to avoid quadratic comparisons.
                this.openM.k = m;
                this.i = idx + m;
                break;
              }
              // Skip frequent near misses with a short two-byte search.
              let next = idx + 1;
              const limit = Math.min(next + 64, end - 1);
              const first = this.openFirst;
              const second = open[1];
              while (next < limit) {
                const byte = src[next + 1];
                if (byte === second && src[next] === first) break;
                next += byte === first ? 1 : 2;
              }
              this.i = next;
            }
            if (this.i >= end) break;
            if (matched) {
              this.i++;
              if (this.halted()) return false;
              continue;
            }
          }
        } else if (this.closeM.k === 0 && this.validate === undefined) {
          // No validator: bulk-copy up to the next possible closePat start.
          const j = this.findClose(src, this.i, end);
          if (j > this.i) {
            this.ensureScratch(j - this.i);
            copyBytes(this.payload, this.payloadEnd, src, this.i, j);
            this.payloadEnd += j - this.i;
            this.i = j;
          }
          if (this.i >= end) continue;
          const close = this.closeBytes;
          const len = close.length;
          // Short, contained delimiters need no matcher state.
          if (len <= 8 && this.i + len <= end && src[this.i] === this.closeFirst) {
            let m = 1;
            while (m < len && src[this.i + m] === close[m]) m++;
            if (m === len) {
              this.i += len - 1;
              this.finishToken();
              this.i++;
              if (this.halted()) return false;
              continue;
            }
          }
        }

        this.step(src[this.i]);
        if (this.restarted) {
          this.restarted = false;
          if (this.halted()) return false;
          continue;
        }
        this.i++;
        if (this.halted()) return false;
      }

      if (!this.inQueue) return true;
      this.leaveQueue();
    }
  }

  /** Park the chunk cursor and scan the queue in its place. */
  private enterQueue(): void {
    this.chunkI = this.i;
    this.chunkFlushStart = this.flushStart;
    this.inQueue = true;
    this.enter(this.queue, this.queueLen, true, 0, 0);
  }

  private leaveQueue(): void {
    this.park();
    this.inQueue = false;
    this.queueLen = 0;
    // The byte that filled the queue is consumed. Carry on past it.
    this.enter(this.chunk, this.chunk.length, false, this.chunkI + 1, this.chunkFlushStart);
  }

  /** End of a buffer: emit the settled span. Held candidates become virtual. */
  private park(): void {
    if (this.state !== OUTSIDE) return;
    // IN_TOKEN: in-token bytes are never part of a pending span.
    const k = this.openM.k;
    if (k > 0) {
      this.flushSpan(this.srcEnd - (k - this.carry));
      this.carry = k;
    } else {
      this.flushSpan(this.srcEnd);
    }
  }

  /** Per-byte automaton. The semantic model. The loops above are its fast paths. */
  private step(byte: number): void {
    if (this.state === OUTSIDE) {
      const res = this.openM.feed(byte);
      if (res === COMPLETE) {
        this.startToken();
        return;
      }
      // Either way the byte stays inside the current buffer's pending span.
      this.releaseOpen(this.openM.released);
      return;
    }

    const res = this.closeM.feed(byte);
    if (res === COMPLETE) {
      this.finishToken();
      return;
    }

    // Released closePat-candidate bytes are committed now, oldest first.
    const closePat = this.closeBytes;
    const released = this.closeM.released;
    if (res === REJECTED) {
      for (let pos = 0; pos < released; pos++) {
        if (!this.tryCommit(closePat[pos])) {
          this.abortToken(pos, released, 0, byte);
          return;
        }
      }
      if (!this.tryCommit(byte)) this.abortToken(released, released, 0, byte);
      return;
    }
    for (let pos = 0; pos < released; pos++) {
      if (!this.tryCommit(closePat[pos])) {
        this.abortToken(pos, released, this.closeM.k, -1);
        return;
      }
    }
  }

  private startToken(): void {
    this.flushSpan(this.i + 1 - (this.openBytes.length - this.carry));
    this.flushStart = this.i + 1;
    this.carry = 0;
    this.state = IN_TOKEN;
    this.clearPayload();
    this.closeM.reset();
  }

  /** A complete token. The resolver runs now, its result may settle later. */
  private finishToken(): void {
    this.flushStart = this.i + 1;
    const copy = this.borrows ? undefined : this.payloadCopy();
    let value: unknown;
    let promise: Promise<unknown> | undefined;
    let recovered = false;
    try {
      value = this.resolve(copy ?? this.payloadScratch());
      promise = toPromise(value);
    } catch (error) {
      if (this.stopped) throw this.stopReason;
      if (this.onResolveError === undefined) throw error;
      recovered = true;
      value = this.onResolveError(error, copy ?? this.payloadCopy());
      promise = toPromise(value);
    }
    if (this.stopped) {
      quiet(promise);
      if (!(value instanceof Uint8Array)) closeIterator(iteratorOf(value));
      throw this.stopReason;
    }
    if (promise !== undefined) {
      const slot = this.addSlot(undefined, copy ?? this.payloadCopy());
      slot.recovered = recovered;
      this.watch(slot, promise);
      this.out.flushInitial();
    } else this.place(value);
    this.endToken();
  }

  /** A synchronous replacement. */
  private place(value: unknown): void {
    if (value === null) {
      // Null is atomic: verbatim, and the span is not re-scanned.
      this.emit(this.openBytes);
      if (this.payloadEnd > this.base) this.emit(this.payloadCopy());
      this.emit(this.closeBytes);
      if (this.stats !== undefined) this.stats.rejected++;
      return;
    }
    if (value instanceof Uint8Array) {
      if (value.length > 0) this.emit(value);
    } else if (typeof value === "string") {
      if (value.length > 0) this.emit(encodeText(value, "resolve result"));
    } else {
      const iter = iteratorOf(value);
      if (iter === undefined) throw invalid(value);
      this.addSlot(iter, EMPTY);
    }
    if (this.stats !== undefined) this.stats.resolved++;
  }

  private addSlot(value: AsyncIterator<Uint8Array> | undefined, payload: Uint8Array): Slot {
    const slot: Slot = {
      owner: this,
      value,
      verbatim: false,
      payload,
      reading: false,
      recovered: false,
      after: [],
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
        else if (!(value instanceof Uint8Array)) closeIterator(iteratorOf(value));
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
      } else if (value instanceof Uint8Array) bytes = value;
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
      const size = slot.verbatim
        ? this.openBytes.length + bytes.length + this.closeBytes.length
        : bytes.length;
      this.live--;
      slot.held += size;
      this.held += size;
    }
    slot.payload = EMPTY;
    if (this.stats !== undefined) {
      if (slot.verbatim) this.stats.rejected++;
      else this.stats.resolved++;
    }
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
      value = this.onResolveError(error, slot.payload);
      promise = toPromise(value);
    } catch (failure) {
      this.fail(failure);
      return;
    }
    if (this.stopped) quiet(promise);
    else if (promise !== undefined) this.watch(slot, promise);
    else this.settle(slot, value);
  }

  /** A retainable copy of the committed payload. */
  private payloadCopy(): Uint8Array {
    return this.payload.slice(this.base, this.payloadEnd);
  }

  /** Scratch view for a borrowing resolver, valid only during the call. */
  private payloadScratch(): Uint8Array {
    return this.base === 0
      ? this.payloadView(this.payloadEnd)
      : this.payload.subarray(this.base, this.payloadEnd);
  }

  private abortToken(from: number, released: number, heldK: number, trailing: number): void {
    if (this.openScan !== undefined) {
      this.capAbort(this.openScan, from, released, heldK, trailing);
      return;
    }
    // Validator: emit `open` verbatim, then re-scan the payload and the tail.
    this.flushStart = this.i + 1;
    this.emit(this.openBytes);
    if (this.stats !== undefined) this.stats.aborted++;
    this.requeueTail(this.payload, this.base, this.payloadEnd, from, released, heldK, trailing);
    this.endToken();
  }

  /** Linear cap abort: restart the payload at the next open inside it. */
  private capAbort(
    openScan: DelimiterMatcher,
    from: number,
    released: number,
    heldK: number,
    trailing: number,
  ): void {
    this.flushStart = this.i + 1;
    const open = this.openBytes;
    const payload = this.payload;
    for (let p = this.scanned; p < this.payloadEnd; p++) {
      if (openScan.feed(payload[p]) === COMPLETE) this.opens.push(p + 1 - open.length);
    }
    this.scanned = this.payloadEnd;
    const opens = this.opens;
    for (;;) {
      this.emit(open);
      if (this.stats !== undefined) this.stats.aborted++;
      while (this.openHead < opens.length && opens[this.openHead] < this.base) this.openHead++;
      if (this.openHead === opens.length) break;
      const at = opens[this.openHead++];
      if (at > this.base) this.emit(payload.slice(this.base, at));
      this.base = at + open.length;
      if (this.payloadEnd - this.base <= this.maxPayloadBytes) {
        this.closeM.reset();
        this.requeueTail(EMPTY, 0, 0, from, released, heldK, trailing);
        return;
      }
    }
    // No open inside. A trailing open candidate is re-scanned with the tail.
    const keep = Math.min(openScan.k, this.payloadEnd - this.base);
    const cut = this.payloadEnd - keep;
    if (cut > this.base) this.emit(payload.slice(this.base, cut));
    this.requeueTail(payload, cut, this.payloadEnd, from, released, heldK, trailing);
    this.endToken();
  }

  /** Re-scan src[start..end) and the held close tail before unconsumed input. */
  private requeueTail(
    src: Uint8Array,
    start: number,
    end: number,
    from: number,
    released: number,
    heldK: number,
    trailing: number,
  ): void {
    const rest = this.inQueue ? this.queueLen - this.i - 1 : 0;
    const restStart = this.i + 1;
    const head = end - start + released - from + heldK + (trailing >= 0 ? 1 : 0);
    const q = (this.queue = requeue(this.queue, head, rest, restStart, 0));
    let w = copyBytes(q, 0, src, start, end);
    for (let pos = from; pos < released; pos++) q[w++] = this.closeBytes[pos];
    for (let pos = 0; pos < heldK; pos++) q[w++] = this.closeBytes[pos];
    if (trailing >= 0) q[w++] = trailing;
    this.queueLen = head + rest;
    // Already inside the queue means the buffer was just replaced: restart on it.
    if (this.inQueue) this.enter(this.queue, this.queueLen, true, 0, 0);
    else this.enterQueue();
    this.restarted = true;
  }

  /** First closeFirst in src[from..end), clamped to the cap so aborts stay linear. */
  private findClose(src: Uint8Array<ArrayBufferLike>, from: number, end: number): number {
    const room = this.maxPayloadBytes - (this.payloadEnd - this.base);
    const limit = room < end - from ? from + room : end;
    if (limit - from <= 512) {
      let j = from;
      while (j < limit && src[j] !== this.closeFirst) j++;
      return j;
    }
    const bounded = limit < src.length ? src.subarray(0, limit) : src;
    const j = bounded.indexOf(this.closeFirst, from);
    return j < 0 || j > limit ? limit : j;
  }

  private tryCommit(byte: number): boolean {
    if (this.validate !== undefined && !this.validate(this.payloadView(this.payloadEnd), byte))
      return false;
    if (this.payloadEnd - this.base + 1 > this.maxPayloadBytes) return false;
    this.ensureScratch(1);
    this.payload[this.payloadEnd++] = byte;
    return true;
  }

  /** Scratch view for validate, valid only during the call. */
  private payloadView(len: number): Uint8Array {
    if (len >= VIEW_CACHE_MAX) return this.payload.subarray(0, len);
    const cached = this.payloadViews[len];
    if (cached !== undefined) return cached;
    const view = this.payload.subarray(0, len);
    this.payloadViews[len] = view;
    return view;
  }

  /** Room for `extra` more bytes. Compacts once base passes half the scratch. */
  private ensureScratch(extra: number): void {
    if (this.payloadEnd + extra <= this.payload.length) return;
    if (this.base > 0 && this.base >= this.payload.length >> 1) {
      this.shift(this.payload);
      if (this.payloadEnd + extra <= this.payload.length) return;
    }
    let size = this.payload.length === 0 ? 16 : this.payload.length * 2;
    const need = this.payloadEnd - this.base + extra;
    if (size < need) size = need;
    if (size > this.scratchLimit) size = this.scratchLimit;
    this.shift(new Uint8Array(size));
    this.payloadViews.length = 0;
  }

  /** Move the live payload to the start of `into`. */
  private shift(into: Uint8Array): void {
    const base = this.base;
    if (into === this.payload) into.copyWithin(0, base, this.payloadEnd);
    else into.set(this.payload.subarray(base, this.payloadEnd));
    this.payload = into;
    this.payloadEnd -= base;
    this.scanned = this.scanned > base ? this.scanned - base : 0;
    this.base = 0;
    const opens = this.opens;
    let w = 0;
    for (let r = this.openHead; r < opens.length; r++) {
      if (opens[r] >= base) opens[w++] = opens[r] - base;
    }
    opens.length = w;
    this.openHead = 0;
  }

  /** Emit held open bytes that left the candidacy, oldest first. */
  private releaseOpen(released: number): void {
    const count = released < this.carry ? released : this.carry;
    if (count === 0) return;
    this.emit(this.openBytes.subarray(0, count));
    this.carry -= count;
  }

  /** Reused buffers are copied out, caller chunks go out by reference. */
  private flushSpan(end: number): void {
    if (end > this.flushStart) {
      const src = this.src;
      if (this.srcOwned) this.emit(src.slice(this.flushStart, end));
      else if (this.slots.length === 0) this.out.emitRange(src, this.flushStart, end);
      else this.emit(src.subarray(this.flushStart, end));
      this.flushStart = end;
    }
  }

  private clearPayload(): void {
    this.base = 0;
    this.payloadEnd = 0;
    this.scanned = 0;
    this.opens.length = 0;
    this.openHead = 0;
    this.openScan?.reset();
  }

  private endToken(): void {
    this.state = OUTSIDE;
    this.clearPayload();
    this.closeM.reset();
    this.openM.reset();
    this.carry = 0;
  }

  private reset(): void {
    for (const slot of this.slots) releaseSlot(slot);
    this.slots = [];
    this.live = 0;
    this.held = 0;
    this.paused = false;
    this.active = false;
    this.scanning = false;
    this.endToken();
    this.payload = EMPTY;
    this.payloadViews.length = 0;
    this.opens = [];
    this.chunk = EMPTY;
    this.src = EMPTY;
    this.srcEnd = 0;
    this.i = 0;
    this.flushStart = 0;
    this.queue = EMPTY;
    this.queueLen = 0;
    this.inQueue = false;
    this.restarted = false;
    this.out.reset();
  }
}

/** Body for `new TransformStream(...)`. Single use. No backpressure inside a
 *  chunk: every replacement for a chunk is enqueued before the next read. */
export function createTokenTransformer(options: TokenTransformOptions): TokenTransformer {
  let signal = options?.signal;
  checkSignal(signal);
  let s: Substituter | undefined;
  let failure: { reason: unknown } | undefined;
  const onClose = (closed?: { reason: unknown }) => {
    failure = closed;
    s = undefined;
    signal?.removeEventListener("abort", onAbort);
    signal = undefined;
  };
  const onAbort = () => s?.cancel(signal?.reason);
  const inactive = () =>
    failure === undefined ? new TypeError("transformer is no longer active") : failure.reason;
  s = new Substituter(options, onClose);
  if (signal?.aborted) onAbort();
  else signal?.addEventListener("abort", onAbort, { once: true });
  const body: TokenTransformer & FlowBody = {
    get paused() {
      return s?.paused ?? false;
    },
    resume: (ctrl: Controller) => (s === undefined ? Promise.reject(inactive()) : s.resume(ctrl)),
    transform: (chunk: Uint8Array, ctrl: Controller) =>
      s === undefined ? Promise.reject(inactive()) : s.transform(chunk, ctrl),
    flush: (ctrl: Controller) => {
      if (s === undefined) throw inactive();
      return s.flush(ctrl);
    },
    cancel: (reason?: unknown) => s?.cancel(reason),
  };
  return body;
}

/** Backpressured pair for `pipeThrough`. Single use. */
export function createTokenStream(
  options: TokenTransformOptions,
): ReadableWritablePair<Uint8Array, Uint8Array> {
  return flowStream(createTokenTransformer(options), options?.signal);
}
