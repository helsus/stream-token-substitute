import { copyBytes, EMPTY, requeue } from "./bytes.ts";
import { flowStream } from "./flow.ts";
import { Lookahead, lookaheadBody, toPromise } from "./lookahead.ts";
import { COMPLETE, DelimiterMatcher, REJECTED } from "./matcher.ts";
import {
  compileOptions,
  type PayloadValidator,
  type TokenResolver,
  type TokenStats,
  type TokenTransformer,
  type TokenTransformOptions,
} from "./types.ts";

/** Cached payload views per length, so validator calls allocate nothing. */
const VIEW_CACHE_MAX = 128;

/** @internal Token scanner. State lives in fields, so it can halt and resume at a token boundary. */
export class Substituter extends Lookahead {
  private readonly openBytes: Uint8Array;
  private readonly closeBytes: Uint8Array;
  private readonly openFirst: number;
  private readonly closeFirst: number;
  private readonly resolve: TokenResolver;
  private readonly borrows: boolean;
  private readonly validate: PayloadValidator | undefined;
  private readonly onDone: ((stats: TokenStats) => void) | undefined;
  private readonly maxPayloadBytes: number;
  private readonly scratchLimit: number;
  private readonly openM: DelimiterMatcher;
  private readonly closeM: DelimiterMatcher;
  private aborted = 0;

  private inToken = false;
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
  /** A cap abort used the open search, so it needs a reset. */
  private openSearched = false;

  // Chunks may be backed by any ArrayBufferLike, including SharedArrayBuffer.
  private chunk: Uint8Array<ArrayBufferLike> = EMPTY;

  // `srcOwned`: src is a reused scratch, so its spans are copied out.
  private src: Uint8Array<ArrayBufferLike> = EMPTY;
  private srcEnd = 0;
  private srcOwned = false;
  private i = 0;
  private flushStart = 0;

  // Re-scan queue, live bytes are queue[0..queueLen).
  private inQueue = false;
  private queue = EMPTY;
  private queueLen = 0;
  // The chunk cursor, parked while the queue is scanned.
  private chunkI = 0;
  private chunkFlushStart = 0;
  /** An abort re-entered the queue, so the cursor must not advance. */
  private restarted = false;

  constructor(options: TokenTransformOptions, onClose: (failure?: { reason: unknown }) => void) {
    const compiled = compileOptions(options);
    super(compiled, onClose);
    this.openBytes = compiled.openBytes;
    this.closeBytes = compiled.closeBytes;
    this.openFirst = compiled.openBytes[0];
    this.closeFirst = compiled.closeBytes[0];
    this.resolve = compiled.resolve;
    this.borrows = compiled.borrows;
    this.validate = compiled.validate;
    this.onDone = compiled.onDone;
    this.maxPayloadBytes = compiled.maxPayloadBytes;
    // Twice the cap without a validator, so compaction stays amortized.
    this.scratchLimit =
      compiled.validate === undefined ? compiled.maxPayloadBytes * 2 : compiled.maxPayloadBytes;
    this.openM = new DelimiterMatcher(compiled.openBytes);
    this.closeM = new DelimiterMatcher(compiled.closeBytes);
    this.openScan =
      compiled.validate === undefined ? new DelimiterMatcher(compiled.openBytes) : undefined;
    // The default cap fits the initial scratch, so steady state never grows it.
    this.payload = new Uint8Array(Math.min(compiled.maxPayloadBytes, 64));
  }

  protected begin(chunk: Uint8Array): void {
    this.chunk = chunk;
    this.enter(chunk, chunk.length, false, 0, 0);
  }

  protected step(): boolean {
    if (!this.scan()) return false;
    this.park();
    this.chunk = EMPTY;
    this.src = EMPTY;
    return true;
  }

  protected beginFlush(): boolean {
    if (this.inToken) {
      this.emit(this.openBytes);
      if (this.payloadEnd > this.base) this.emit(this.payloadCopy());
      if (this.closeM.k > 0) this.emit(this.closeBytes.slice(0, this.closeM.k));
    } else if (this.openM.k > 0) {
      this.emit(this.openBytes.slice(0, this.openM.k));
    }
    this.endToken();
    return false;
  }

  protected verbatim(payload: Uint8Array, sink: (part: Uint8Array) => void): void {
    sink(this.openBytes);
    if (payload.length > 0) sink(payload);
    sink(this.closeBytes);
  }

  protected report(bytesOut: number): void {
    this.onDone?.({
      replaced: this.replaced,
      rejected: this.rejected,
      aborted: this.aborted,
      bytesIn: this.bytesIn,
      bytesOut,
    });
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

        if (!this.inToken) {
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

        this.feed(src[this.i]);
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
    // In-token bytes are never part of a pending span.
    if (this.inToken) return;
    const k = this.openM.k;
    if (k > 0) {
      this.flushSpan(this.srcEnd - (k - this.carry));
      this.carry = k;
    } else {
      this.flushSpan(this.srcEnd);
    }
  }

  /** Per-byte automaton. The semantic model. The loops above are its fast paths. */
  private feed(byte: number): void {
    if (!this.inToken) {
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
    this.inToken = true;
    this.clearPayload();
    this.closeM.reset();
  }

  /** A complete token. The resolver runs now, its result may settle later. */
  private finishToken(): void {
    this.flushStart = this.i + 1;
    const copy = this.borrows ? undefined : this.payloadCopy();
    const arg = copy ?? this.payloadScratch();
    let value: unknown;
    let promise: Promise<unknown> | undefined;
    let failed = false;
    let error: unknown;
    try {
      value = this.resolve(arg, this.context);
      if (!(value instanceof Uint8Array)) promise = toPromise(value);
    } catch (thrown) {
      failed = true;
      error = thrown;
    }
    // Fast path: bytes go out, or behind the queue, with no further checks.
    if (value instanceof Uint8Array && !this.stopped) {
      // A borrowed scratch view must not go out by reference.
      this.emit(value === arg && copy === undefined ? value.slice() : value);
      this.replaced++;
    } else if (copy !== undefined) this.accept(value, promise, failed, error, copy);
    else this.accept(value, promise, failed, error, this.payloadCopy(), arg);
    this.endToken();
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
    this.aborted++;
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
    this.openSearched = true;
    const opens = this.opens;
    for (;;) {
      this.emit(open);
      this.aborted++;
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

  private flushSpan(end: number): void {
    if (end > this.flushStart) {
      // Common case inline: a caller chunk with nothing queued.
      if (!this.srcOwned && this.slots.length === 0) {
        this.out.emitRange(this.src, this.flushStart, end);
      } else this.emitSpan(this.src, this.flushStart, end, this.srcOwned);
      this.flushStart = end;
    }
  }

  private clearPayload(): void {
    this.base = 0;
    this.payloadEnd = 0;
    if (!this.openSearched) return;
    this.openSearched = false;
    this.scanned = 0;
    this.opens.length = 0;
    this.openHead = 0;
    this.openScan?.reset();
  }

  private endToken(): void {
    this.inToken = false;
    this.clearPayload();
    this.closeM.reset();
    this.openM.reset();
    this.carry = 0;
  }

  protected clear(): void {
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
  }
}

/** Body for `new TransformStream(...)`. Single use. */
export function createTokenTransformer(options: TokenTransformOptions): TokenTransformer {
  return lookaheadBody(options?.signal, (onClose) => new Substituter(options, onClose));
}

/** Backpressured pair for `pipeThrough`. Single use. */
export function createTokenStream(
  options: TokenTransformOptions,
): ReadableWritablePair<Uint8Array, Uint8Array> {
  return flowStream(createTokenTransformer(options), options?.signal);
}
