import { copyBytes, EMPTY, grow, requeue } from "./bytes.ts";
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
  private i = 0;
  private flushStart = 0;

  /** Set by an async resolver that returned a thenable. The scan loops unwind
   *  without advancing, leaving every field exactly where `resume()` needs it. */
  protected suspended = false;

  private readonly out: Emitter;

  // Re-scan queue. A reused scratch: live bytes are queue[0..queueLen).
  // abortToken() rewrites it in place and bumps queueGen, which is how drain()
  // tells a replaced queue from a consumed byte.
  private draining = false;
  private queue = EMPTY;
  private queueLen = 0;
  private qi = 0;
  private queueGen = 0;

  // Drained content bytes have no backing chunk, so they accumulate here.
  private drainBuf = EMPTY;
  private drainLen = 0;

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
    this.begin(chunk, ctrl);
    this.pump();
  }

  /** Accept a chunk and position the scanner at its first byte. */
  protected begin(chunk: Uint8Array, ctrl: Controller): void {
    if (!(chunk instanceof Uint8Array)) throw new TypeError("chunk must be a Uint8Array");

    this.chunk = chunk;
    this.out.ctrl = ctrl;
    this.i = 0;
    this.flushStart = 0;
    if (this.stats !== undefined) this.stats.bytesIn += chunk.length;
  }

  /** Scan the current chunk to its end, or until an async resolver suspends.
   *  Re-entrant: every exit point leaves the fields ready for the next call. */
  protected pump(): void {
    const chunk = this.chunk;
    const count = chunk.length;

    while (this.i < count) {
      if (this.draining) {
        // A drain interrupted by a suspended token: pick it back up.
        this.drain();
        if (this.suspended) return;
        this.i++;
        continue;
      }

      if (this.state === OUTSIDE) {
        // k === 0 means no held state, so a delimiter contained in this chunk
        // can be settled by indexOf (a SIMD memchr) plus a direct compare. Only
        // one straddling the chunk end needs the matcher.
        if (this.openM.k === 0) {
          const open = this.openBytes;
          const len = open.length;
          let matched = false;
          for (;;) {
            const idx = chunk.indexOf(this.openFirst, this.i);
            if (idx < 0) {
              this.i = count;
              break;
            }
            if (idx + len > count) {
              this.i = idx;
              break;
            }
            let m = 1;
            while (m < len && chunk[idx + m] === open[m]) m++;
            if (m === len) {
              this.i = idx + len - 1;
              this.startToken();
              matched = true;
              break;
            }
            this.i = idx + 1;
          }
          if (this.i >= count) break;
          if (matched) {
            this.i++;
            continue;
          }
        }
      } else if (this.closeM.k === 0 && this.validate === undefined) {
        // No validator: bulk-copy up to the next possible closePat start.
        const j = this.findClose(chunk, this.i, count);
        if (j > this.i) {
          this.ensureScratch(this.payloadLen + (j - this.i));
          copyBytes(this.payload, this.payloadLen, chunk, this.i, j);
          this.payloadLen += j - this.i;
          this.i = j;
          continue;
        }
      }

      this.step(chunk[this.i]);
      if (this.suspended) return;
      if (this.queueLen > 0) {
        this.drain();
        if (this.suspended) return;
      }
      this.i++;
    }

    if (this.state === OUTSIDE) {
      const k = this.openM.k;
      if (k > 0) {
        // Candidacy bytes still in this chunk become virtual.
        this.flushSpan(count - (k - this.carry));
        this.carry = k;
      } else {
        this.flushSpan(count);
      }
    }
    // IN_TOKEN: in-token bytes are never part of a pending span.

    // Must precede dropping the chunk: buffered spans are views into it.
    this.out.flush();
    this.chunk = EMPTY;
  }

  flush(ctrl: Controller): void {
    this.out.ctrl = ctrl;
    if (this.state === IN_TOKEN) {
      this.out.emit(this.openBytes);
      if (this.payloadLen > 0) this.out.emit(this.payload.slice(0, this.payloadLen));
      if (this.closeM.k > 0) this.out.emit(this.closeBytes.slice(0, this.closeM.k));
    } else if (this.openM.k > 0) {
      this.out.emit(this.openBytes.slice(0, this.openM.k));
    }
    this.out.flush();
    this.reset();
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
      this.releaseOpen(this.openM.released);
      if (res === REJECTED) this.contentByte(byte);
      else if (this.draining) this.carry++;
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
    if (this.draining) {
      this.flushDrain();
    } else {
      this.flushSpan(this.i + 1 - (this.openBytes.length - this.carry));
      this.flushStart = this.i + 1;
    }
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
    if (this.draining) this.flushDrain();
    else this.flushStart = this.i + 1;

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
    const inDrain = this.draining;
    this.completeToken(value);
    if (inDrain) this.qi++;
    else this.i++;
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
   * (copyWithin is a memmove), then the head is overwritten. queueGen tells
   * drain() to restart its cursor.
   */
  private abortToken(from: number, released: number, heldK: number, trailing: number): void {
    if (this.draining) this.flushDrain();
    else this.flushStart = this.i + 1;
    this.out.emit(this.openBytes);
    if (this.stats !== undefined) this.stats.aborted++;

    const rest = this.draining ? this.queueLen - this.qi - 1 : 0;
    const restStart = this.qi + 1;
    const tailLen = released - from + heldK + (trailing >= 0 ? 1 : 0);
    const head = this.payloadLen + tailLen;
    const q = (this.queue = requeue(this.queue, head, rest, restStart, 0));
    let w = copyBytes(q, 0, this.payload, 0, this.payloadLen);
    for (let pos = from; pos < released; pos++) q[w++] = this.closeBytes[pos];
    for (let pos = 0; pos < heldK; pos++) q[w++] = this.closeBytes[pos];
    if (trailing >= 0) q[w++] = trailing;

    this.endToken();
    this.queueLen = head + rest;
    this.qi = 0;
    this.queueGen++;
  }

  /** Drive queued bytes through the scanner before consuming more input.
   *  Mirrors the pump() fast paths, with content going to drainBuf instead
   *  of a pending span. Bulk paths only bypass step() for bytes that provably
   *  cannot advance a matcher, so carried state stays identical. */
  private drain(): void {
    this.draining = true;
    const openPat = this.openBytes;
    const openLen = openPat.length;
    while (this.qi < this.queueLen) {
      const q = this.queue;
      const len = this.queueLen;
      if (this.state === OUTSIDE) {
        if (this.openM.k === 0) {
          const start = this.qi;
          let scan = start;
          let matched = false;
          let stop = len;
          for (;;) {
            const idx = q.indexOf(this.openFirst, scan);
            if (idx < 0 || idx >= len) break;
            if (idx + openLen > len) {
              stop = idx;
              break;
            }
            let m = 1;
            while (m < openLen && q[idx + m] === openPat[m]) m++;
            if (m === openLen) {
              stop = idx;
              matched = true;
              break;
            }
            scan = idx + 1;
          }
          this.drainRange(q, start, stop);
          if (matched) {
            this.qi = stop + openLen;
            this.startToken();
            continue;
          }
          this.qi = stop;
          if (stop >= len) continue;
          // A candidate straddles the queue end: resolve it per byte.
        }
      } else if (this.closeM.k === 0 && this.validate === undefined) {
        const j = this.findClose(q, this.qi, len);
        if (j > this.qi) {
          this.ensureScratch(this.payloadLen + (j - this.qi));
          copyBytes(this.payload, this.payloadLen, q, this.qi, j);
          this.payloadLen += j - this.qi;
          this.qi = j;
          continue;
        }
      }

      const gen = this.queueGen;
      this.step(q[this.qi]);
      // Suspended: qi stays put, and resume() advances it. finishToken never
      // rewrites the queue, so the generation cannot have moved either.
      if (this.suspended) return;
      if (this.queueGen === gen) this.qi++;
    }
    this.flushDrain();
    this.draining = false;
    this.queueLen = 0;
    this.qi = 0;
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
    const j = src.indexOf(this.closeFirst, from);
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

  /** A byte that is ordinary content. In a chunk it stays covered by the pending
   *  span; off the queue it has no backing chunk, so it is buffered. */
  private contentByte(byte: number): void {
    if (this.draining) {
      this.ensureDrain(this.drainLen + 1);
      this.drainBuf[this.drainLen++] = byte;
    }
  }

  private drainRange(src: Uint8Array, start: number, end: number): void {
    if (end <= start) return;
    this.ensureDrain(this.drainLen + (end - start));
    this.drainLen = copyBytes(this.drainBuf, this.drainLen, src, start, end);
  }

  private ensureDrain(need: number): void {
    this.drainBuf = grow(this.drainBuf, this.drainLen, need, 32);
  }

  /** Emit held open bytes that no longer belong to a candidacy, oldest first.
   *  Only the virtual ones: the rest are still inside the pending span. */
  private releaseOpen(released: number): void {
    const count = released < this.carry ? released : this.carry;
    if (count === 0) return;
    if (this.draining) {
      this.drainRange(this.openBytes, 0, count);
    } else {
      this.out.emit(this.openBytes.subarray(0, count));
    }
    this.carry -= count;
  }

  private flushSpan(end: number): void {
    if (end > this.flushStart) {
      this.out.emit(this.chunk.subarray(this.flushStart, end));
      this.flushStart = end;
    }
  }

  /** The copy is required: drainBuf is scratch and may be rewritten before the
   *  accumulator flushes, and with flushBytes 0 the part is enqueued as-is. */
  private flushDrain(): void {
    if (this.drainLen === 0) return;
    this.out.emit(this.drainBuf.slice(0, this.drainLen));
    this.drainLen = 0;
  }

  private endToken(): void {
    this.state = OUTSIDE;
    this.payloadLen = 0;
    this.closeM.reset();
    this.openM.reset();
    this.carry = 0;
  }

  private reset(): void {
    this.endToken();
    this.chunk = EMPTY;
    this.queue = EMPTY;
    this.queueLen = 0;
    this.qi = 0;
    this.drainBuf = EMPTY;
    this.drainLen = 0;
    this.draining = false;
    this.out.reset();
  }
}

/** Single-use transformer body, for runtimes where `TransformStream` is not a
 *  global: `new TransformStream(createTokenTransformer(options))`. Construct one
 *  per stream. Touches no global other than `TextEncoder`, and only then if a
 *  delimiter is a string. */
export function createTokenTransformer(options: TokenTransformOptions): TokenTransformer {
  const s = new Substituter(options);
  return {
    transform: (chunk, ctrl) => s.transform(chunk, ctrl),
    flush: (ctrl) => s.flush(ctrl),
  };
}

/** Single-use TransformStream. Construct one per stream. */
export function createTokenTransformStream(
  options: TokenTransformOptions,
): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>(createTokenTransformer(options));
}
