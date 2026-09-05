import { copyBytes, EMPTY, requeue } from "./bytes.ts";
import { flowStream } from "./flow.ts";
import { COMPLETE, DelimiterMatcher, REJECTED } from "./matcher.ts";
import { Emitter } from "./output.ts";
import {
  type AsyncTokenResolver,
  compileOptions,
  type PayloadValidator,
  type ResolveErrorHandler,
  type TokenStats,
  type TokenTransformer,
  type TokenTransformOptions,
} from "./types.ts";

const OUTSIDE = 0;
const IN_TOKEN = 1;

/** Payload views up to this length are cached per length. A validator is called
 *  once per committed byte, and without the cache every call allocates a fresh
 *  subarray over the same scratch. */
const VIEW_CACHE_MAX = 128;

type Controller = TransformStreamDefaultController<Uint8Array>;

/**
 * Byte scanner behind createTokenTransformStream.
 *
 * Carried state across chunks is integers plus the payload scratch: partial
 * delimiter matches live in the matchers' `k`, never as indices into a chunk.
 *
 * `carry` is the number of held open-candidate bytes that are NOT backed by the
 * current chunk's pending span (they arrived in an earlier chunk, or came off the
 * re-scan queue). Released bytes are emitted only up to `carry`; the rest are
 * still covered by the pending span, which keeps non-token output maximal.
 *
 * All scan state lives in fields, which is what makes the scanner resumable: the
 * async subclass parks at a completed token and re-enters `pump()` unchanged.
 * Not part of the public API.
 */
export class Substituter {
  paused = false;
  private readonly openBytes: Uint8Array;
  private readonly closeBytes: Uint8Array;
  private readonly openFirst: number;
  private readonly closeFirst: number;
  protected readonly resolve: AsyncTokenResolver;
  private readonly validate: PayloadValidator | undefined;
  protected readonly onResolveError: ResolveErrorHandler | undefined;
  private readonly onDone: ((stats: TokenStats) => void) | undefined;
  private readonly maxPayloadBytes: number;
  private readonly openM: DelimiterMatcher;
  private readonly closeM: DelimiterMatcher;

  /** Present only when `onDone` is, so the default path stays untouched. */
  private readonly stats: TokenStats | undefined;

  private state: typeof OUTSIDE | typeof IN_TOKEN = OUTSIDE;
  private carry = 0;
  protected payload: Uint8Array;
  protected payloadLen = 0;
  // Cached payload.subarray(0, len) by len, dropped when the scratch grows.
  private payloadViews: (Uint8Array | undefined)[] = [];

  // Chunks may be backed by any ArrayBufferLike, including SharedArrayBuffer.
  private chunk: Uint8Array<ArrayBufferLike> = EMPTY;

  // The buffer being scanned: the chunk, or the re-scan queue. `srcOwned` marks
  // one the scanner reuses, whose spans must be copied to be emitted.
  private src: Uint8Array<ArrayBufferLike> = EMPTY;
  private srcEnd = 0;
  private srcOwned = false;
  private i = 0;
  private flushStart = 0;

  /** Set by an async resolver that returned a thenable. The scan loops unwind
   *  without advancing, leaving every field exactly where `resume()` needs it. */
  protected suspended = false;

  protected readonly out: Emitter;

  // Re-scan queue. A reused scratch: live bytes are queue[0..queueLen). The
  // payload it replays spans earlier chunks, so it cannot be re-read in place.
  private inQueue = false;
  private queue = EMPTY;
  private queueLen = 0;
  // The chunk cursor, parked while the queue is scanned. Fields, not locals: an
  // async resolver can suspend mid-queue and unwind the stack.
  private chunkI = 0;
  private chunkFlushStart = 0;
  /** abortToken() re-entered the queue, so the cursor must not advance. */
  private restarted = false;

  constructor(options: TokenTransformOptions) {
    const compiled = compileOptions(options);
    this.openBytes = compiled.openBytes;
    this.closeBytes = compiled.closeBytes;
    this.openFirst = compiled.openBytes[0];
    this.closeFirst = compiled.closeBytes[0];
    this.resolve = compiled.resolve;
    this.validate = compiled.validate;
    this.onResolveError = compiled.onResolveError;
    this.onDone = compiled.onDone;
    this.maxPayloadBytes = compiled.maxPayloadBytes;
    this.out = new Emitter(compiled.flushBytes);
    this.openM = new DelimiterMatcher(compiled.openBytes);
    this.closeM = new DelimiterMatcher(compiled.closeBytes);
    this.stats =
      compiled.onDone === undefined
        ? undefined
        : { resolved: 0, rejected: 0, aborted: 0, bytesIn: 0, bytesOut: 0 };
    // The default cap fits the initial scratch, so steady state never grows it.
    this.payload = new Uint8Array(Math.min(compiled.maxPayloadBytes, 64));
  }

  transform(chunk: Uint8Array, ctrl: Controller): void {
    try {
      this.begin(chunk, ctrl);
      this.pump();
    } catch (error) {
      this.reset();
      throw error;
    }
  }

  /** Accept a chunk and position the scanner at its first byte. */
  protected begin(chunk: Uint8Array, ctrl: Controller): void {
    if (!(chunk instanceof Uint8Array)) throw new TypeError("chunk must be a Uint8Array");

    this.chunk = chunk;
    this.out.ctrl = ctrl;
    this.enter(chunk, chunk.length, false, 0, 0);
    if (this.stats !== undefined) this.stats.bytesIn += chunk.length;
  }

  /** Scan the current chunk to its end, or until an async resolver suspends.
   *  Re-entrant: every exit point leaves the fields ready for the next call. */
  protected pump(): void {
    this.paused = false;
    this.scan();
    if (this.suspended || this.paused) return;
    this.park();
    // Must precede dropping the chunk: buffered spans are views into it.
    this.out.flush();
    this.out.ctrl = undefined;
    this.chunk = EMPTY;
    this.src = EMPTY;
  }

  resumeOutput(ctrl: Controller): void {
    if (!this.paused) return;
    this.out.ctrl = ctrl;
    this.pump();
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

  /** Scan the current buffer, descending into the re-scan queue whenever an
   *  abort fills it and coming back out where it left off. */
  private scan(): void {
    for (;;) {
      // Re-read per iteration: an abort inside the queue replaces the buffer.
      while (this.i < this.srcEnd) {
        const src = this.src;
        const end = this.srcEnd;

        if (this.state === OUTSIDE) {
          // k === 0 means no held state, so a delimiter contained in this
          // buffer can be settled by indexOf plus a direct compare. Only one
          // straddling the end needs the matcher.
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
              if (this.pauseOutput()) return;
              continue;
            }
          }
        } else if (this.closeM.k === 0 && this.validate === undefined) {
          // No validator: bulk-copy up to the next possible closePat start.
          const j = this.findClose(src, this.i, end);
          if (j > this.i) {
            this.ensureScratch(this.payloadLen + (j - this.i));
            copyBytes(this.payload, this.payloadLen, src, this.i, j);
            this.payloadLen += j - this.i;
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
              if (this.suspended) return;
              this.i++;
              if (this.pauseOutput()) return;
              continue;
            }
          }
        }

        this.step(src[this.i]);
        if (this.suspended) return;
        if (this.restarted) {
          this.restarted = false;
          if (this.pauseOutput()) return;
          continue;
        }
        this.i++;
        if (this.pauseOutput()) return;
      }

      if (!this.inQueue) return;
      this.leaveQueue();
    }
  }

  private pauseOutput(): boolean {
    this.paused = this.out.blocked;
    return this.paused;
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
    // The byte that filled the queue is consumed; carry on past it.
    this.enter(this.chunk, this.chunk.length, false, this.chunkI + 1, this.chunkFlushStart);
  }

  /** End of a buffer: emit the settled span. Candidacy bytes still inside it
   *  become virtual. */
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

  flush(ctrl: Controller): void {
    this.out.ctrl = ctrl;
    try {
      if (this.state === IN_TOKEN) {
        this.out.emit(this.openBytes);
        if (this.payloadLen > 0) this.out.emit(this.payload.slice(0, this.payloadLen));
        if (this.closeM.k > 0) this.out.emit(this.closeBytes.slice(0, this.closeM.k));
      } else if (this.openM.k > 0) {
        this.out.emit(this.openBytes.slice(0, this.openM.k));
      }
      this.out.flush();
    } finally {
      this.reset();
    }
    if (this.stats !== undefined) {
      this.stats.bytesOut = this.out.bytesOut;
      (this.onDone as (s: TokenStats) => void)(this.stats);
    }
  }

  /** Per-byte automaton. The semantic model; the loops above are its fast paths. */
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
    this.payloadLen = 0;
    this.closeM.reset();
  }

  /** A complete token. The resolver call is the only suspend point: when it
   *  parks, every field still describes the token, and `resume()` finishes it. */
  private finishToken(): void {
    const value = this.invokeResolve();
    if (this.suspended) return;
    this.completeToken(value);
  }

  /** Sync resolution. Overridden by the async subclass. */
  protected invokeResolve(): Uint8Array | null {
    try {
      return (this.resolve as (p: Uint8Array) => Uint8Array | null)(
        this.payloadView(this.payloadLen),
      );
    } catch (error) {
      if (this.onResolveError === undefined) throw error;
      return this.onResolveError(error, this.payloadCopy());
    }
  }

  /** Emit a resolved token and leave the scanner outside it. */
  protected completeToken(value: Uint8Array | null): void {
    this.flushStart = this.i + 1;

    if (value === null) {
      // Null is atomic: verbatim, and the span is not re-scanned.
      this.out.emit(this.openBytes);
      if (this.payloadLen > 0) this.out.emit(this.payload.slice(0, this.payloadLen));
      this.out.emit(this.closeBytes);
      if (this.stats !== undefined) this.stats.rejected++;
    } else {
      if (value.length > 0) this.out.emit(value);
      if (this.stats !== undefined) this.stats.resolved++;
    }
    this.endToken();
  }

  /** Finish a token whose resolver parked, then carry on scanning. The byte that
   *  completed the token has not been consumed yet, which is why this advances
   *  the cursor the suspended loop was about to advance. */
  protected resume(value: Uint8Array | null): void {
    this.suspended = false;
    this.completeToken(value);
    this.i++;
    this.pump();
  }

  /** A retainable copy of the committed payload. */
  protected payloadCopy(): Uint8Array {
    return this.payload.slice(0, this.payloadLen);
  }

  /**
   * Abort: emit `open` verbatim, then re-scan the committed payload plus
   * every byte that followed it. `tail` is closeBytes[from..released), then the
   * still-held closeBytes[0..heldK), then `trailing` if >= 0.
   *
   * The queue scratch is reused: the unconsumed old suffix moves first
   * (copyWithin is a memmove), then the head is overwritten.
   */
  private abortToken(from: number, released: number, heldK: number, trailing: number): void {
    this.flushStart = this.i + 1;
    this.out.emit(this.openBytes);
    if (this.stats !== undefined) this.stats.aborted++;

    const rest = this.inQueue ? this.queueLen - this.i - 1 : 0;
    const restStart = this.i + 1;
    const tailLen = released - from + heldK + (trailing >= 0 ? 1 : 0);
    const head = this.payloadLen + tailLen;
    const q = (this.queue = requeue(this.queue, head, rest, restStart, 0));
    let w = copyBytes(q, 0, this.payload, 0, this.payloadLen);
    for (let pos = from; pos < released; pos++) q[w++] = this.closeBytes[pos];
    for (let pos = 0; pos < heldK; pos++) q[w++] = this.closeBytes[pos];
    if (trailing >= 0) q[w++] = trailing;

    this.endToken();
    this.queueLen = head + rest;
    // Already inside the queue means the buffer was just replaced: restart on it.
    if (this.inQueue) this.enter(this.queue, this.queueLen, true, 0, 0);
    else this.enterQueue();
    this.restarted = true;
  }

  /** First closeFirst in src[from..end), clamped to the cap: bytes past `room`
   *  cannot be committed, so whether closeFirst occurs among them is irrelevant,
   *  and an unbounded indexOf would rescan the rest of src on every cap abort.
   *  Returns the clamp position when absent; the caller commits src[from..j)
   *  whole, so a cap overflow still aborts on the exact same byte. */
  private findClose(src: Uint8Array<ArrayBufferLike>, from: number, end: number): number {
    const room = this.maxPayloadBytes - this.payloadLen;
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
    if (this.validate !== undefined && !this.validate(this.payloadView(this.payloadLen), byte))
      return false;
    if (this.payloadLen + 1 > this.maxPayloadBytes) return false;
    this.ensureScratch(this.payloadLen + 1);
    this.payload[this.payloadLen++] = byte;
    return true;
  }

  /** Scratch view handed to validate/resolve. Valid only during the call, which
   *  is what makes reusing one view object per length safe. */
  private payloadView(len: number): Uint8Array {
    if (len >= VIEW_CACHE_MAX) return this.payload.subarray(0, len);
    const cached = this.payloadViews[len];
    if (cached !== undefined) return cached;
    const view = this.payload.subarray(0, len);
    this.payloadViews[len] = view;
    return view;
  }

  private ensureScratch(need: number): void {
    if (need <= this.payload.length) return;
    let size = this.payload.length === 0 ? 16 : this.payload.length * 2;
    if (size < need) size = need;
    if (size > this.maxPayloadBytes) size = this.maxPayloadBytes;
    const next = new Uint8Array(size);
    next.set(this.payload.subarray(0, this.payloadLen));
    this.payload = next;
    this.payloadViews.length = 0;
  }

  /** Emit held open bytes that no longer belong to a candidacy, oldest first.
   *  Only the virtual ones: the rest are still inside the pending span. */
  private releaseOpen(released: number): void {
    const count = released < this.carry ? released : this.carry;
    if (count === 0) return;
    this.out.emit(this.openBytes.subarray(0, count));
    this.carry -= count;
  }

  /** A buffer the scanner reuses is copied out of; a caller's chunk is enqueued
   *  by reference. */
  private flushSpan(end: number): void {
    if (end > this.flushStart) {
      const src = this.src;
      if (this.srcOwned) this.out.emit(src.slice(this.flushStart, end));
      else this.out.emitRange(src, this.flushStart, end);
      this.flushStart = end;
    }
  }

  private endToken(): void {
    this.state = OUTSIDE;
    this.payloadLen = 0;
    this.closeM.reset();
    this.openM.reset();
    this.carry = 0;
  }

  cancel(): void {
    this.reset();
  }

  protected reset(): void {
    this.paused = false;
    this.endToken();
    this.suspended = false;
    this.payload = EMPTY;
    this.payloadViews.length = 0;
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

/** Single-use transformer body, for runtimes where `TransformStream` is not a
 *  global: `new TransformStream(createTokenTransformer(options))`. Construct one
 *  per stream. Touches no global other than `TextEncoder`, and only then if a
 *  delimiter is a string. */
export function createTokenTransformer(options: TokenTransformOptions): TokenTransformer {
  let s: Substituter | undefined = new Substituter(options);
  const body = {
    get paused() {
      return s?.paused ?? false;
    },
    resume: (ctrl: Controller) => {
      try {
        s?.resumeOutput(ctrl);
      } catch (error) {
        s?.cancel();
        s = undefined;
        throw error;
      }
    },
    transform: (chunk: Uint8Array, ctrl: Controller) => {
      if (s === undefined) throw new TypeError("transformer is no longer active");
      try {
        s.transform(chunk, ctrl);
      } catch (error) {
        s = undefined;
        throw error;
      }
    },
    flush: (ctrl: Controller) => {
      const active = s;
      s = undefined;
      active?.flush(ctrl);
    },
    cancel: () => {
      s?.cancel();
      s = undefined;
    },
  };
  return body;
}

/** Single-use native transform. */
export function createTokenTransformStream(
  options: TokenTransformOptions,
): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>(createTokenTransformer(options));
}

/** Single-use pair with intra-chunk backpressure. */
export function createTokenStreamPair(
  options: TokenTransformOptions,
): ReadableWritablePair<Uint8Array, Uint8Array> {
  return flowStream(createTokenTransformer(options));
}
