import { AhoCorasick } from "./aho-corasick.ts";
import { copyBytes, EMPTY, encodeText } from "./bytes.ts";
import { flowStream } from "./flow.ts";
import { buildFailureTable } from "./matcher.ts";
import { Emitter } from "./output.ts";

export { DEFAULT_MAX_TABLE_BYTES } from "./aho-corasick.ts";

type Controller = TransformStreamDefaultController<Uint8Array>;

/** Resolve a matched needle to substitution bytes. `index` is its position in
 *  the compiled set. Bytes are enqueued by reference, so do not mutate them
 *  after. null emits the needle verbatim. `needle` is scratch: a view valid
 *  only during the call, so copy it to keep it. */
export type NeedleResolver = (needle: Uint8Array, index: number) => Uint8Array | null;

/** Counts for one stream, delivered once from `flush`. */
export interface NeedleStats {
  /** Matches whose resolver returned bytes (including empty). */
  substituted: number;
  /** Matches whose resolver returned null, emitted verbatim. */
  rejected: number;
  bytesIn: number;
  bytesOut: number;
}

/** Literals to replace. A record or Map supplies the replacements; an array
 *  supplies only the needles and requires `resolve`. Strings are UTF-8 encoded
 *  once. Needles must be non-empty. */
export type NeedleSource =
  | Record<string, string | Uint8Array>
  | Map<string | Uint8Array, string | Uint8Array>
  | readonly (string | Uint8Array)[];

export interface CompileNeedleOptions {
  /** Ceiling on the automaton's transition table, in bytes. Defaults to
   *  `DEFAULT_MAX_TABLE_BYTES`. Raise it for a large set on a runtime with room
   *  for it, lower it to fail earlier on a constrained one. */
  maxTableBytes?: number;
}

export interface NeedleTransformOptions extends CompileNeedleOptions {
  /** A `NeedleSource`, or the result of `compileNeedles`. `maxTableBytes` is
   *  ignored for an already-compiled set. */
  needles: NeedleSource | CompiledNeedles;
  /** Overrides the table, and is required when `needles` is an array. */
  resolve?: NeedleResolver;
  /** Output accumulator high-water mark. See `TokenTransformOptions`. Default 16384. */
  flushBytes?: number;
  onDone?: (stats: NeedleStats) => void;
}

export interface NeedleTransformer {
  transform(chunk: Uint8Array, controller: Controller): void;
  flush(controller: Controller): void;
  cancel?(reason?: unknown): void;
}

interface CompiledNeedleOptions {
  set: NeedleSet;
  resolve: NeedleResolver;
  flushBytes: number;
  onDone: ((stats: NeedleStats) => void) | undefined;
}

/** The automaton plus the bytes it matches. Opaque and immutable. */
class NeedleSet {
  readonly needles: Uint8Array[];
  readonly values: Uint8Array[] | undefined;
  readonly ac: AhoCorasick;

  constructor(needles: Uint8Array[], values: Uint8Array[] | undefined, maxTableBytes?: number) {
    this.needles = needles;
    this.values = values;
    this.ac = new AhoCorasick(needles, maxTableBytes);
  }
}

export type CompiledNeedles = NeedleSet;

/** Rolling match index for overlap-heavy streams. */
class IndexedNeedles {
  private readonly needles: readonly Uint8Array[];
  private readonly ac: AhoCorasick;
  private readonly resolve: NeedleResolver;
  private readonly out: Emitter;
  private readonly ring: Uint8Array;
  private readonly matches: Uint32Array;
  private readonly states: Uint32Array;
  private readonly failures: (Uint8Array | Uint32Array)[];
  private read = 0;
  private at = 0;
  private plain = 0;
  private node = 0;

  constructor(
    needles: readonly Uint8Array[],
    ac: AhoCorasick,
    resolve: NeedleResolver,
    out: Emitter,
  ) {
    this.needles = needles;
    this.ac = ac;
    this.resolve = resolve;
    this.out = out;
    this.ring = new Uint8Array(ac.maxLength + 1);
    this.matches = new Uint32Array(this.ring.length);
    this.states = new Uint32Array(needles.length);
    this.failures = needles.map(buildFailureTable);
  }

  scan(src: Uint8Array, from: number, end: number): number {
    this.settle(false);
    for (let i = from; i < end; i++) {
      if (this.read - this.plain === this.ring.length) this.flushPlain();
      if (this.out.blocked) return i;
      const byte = src[i];
      const slot = this.read % this.ring.length;
      this.ring[slot] = byte;
      this.matches[slot] = 0;
      for (let p = 0; p < this.needles.length; p++) {
        const needle = this.needles[p];
        const fail = this.failures[p];
        let k = this.states[p];
        while (k > 0 && needle[k] !== byte) k = fail[k - 1];
        if (needle[k] === byte) k++;
        if (k === needle.length) {
          const start = this.read + 1 - k;
          if (start >= this.at) {
            const pos = start % this.ring.length;
            const prev = this.matches[pos];
            if (prev === 0 || this.needles[prev - 1].length < k) this.matches[pos] = p + 1;
          }
          k = fail[k - 1];
        }
        this.states[p] = k;
      }
      this.node = this.ac.delta[this.node * this.ac.width + this.ac.classOf[byte]];
      this.read++;
      this.settle(false);
      if (this.out.blocked) return i + 1;
    }
    this.flushPlain();
    return end;
  }

  finish(): boolean {
    this.settle(true);
    if (this.at !== this.read) return false;
    this.flushPlain();
    return true;
  }

  private settle(final: boolean): void {
    while (this.at < this.read) {
      while (this.ac.depth[this.node] > this.read - this.at) this.node = this.ac.fail[this.node];
      if (!final && this.at >= this.read - this.ac.depth[this.node]) return;
      const match = this.matches[this.at % this.ring.length];
      if (match === 0) {
        this.at++;
        continue;
      }
      this.flushPlain();
      if (this.out.blocked) return;
      const length = this.needles[match - 1].length;
      const payload = this.copy(this.at, length);
      const value = this.resolve(payload, match - 1);
      this.at += length;
      this.plain = this.at;
      this.out.emit(value === null ? payload : value);
      if (this.out.blocked) return;
    }
  }

  private flushPlain(): void {
    if (this.plain === this.at) return;
    const bytes = this.copy(this.plain, this.at - this.plain);
    this.plain = this.at;
    this.out.emit(bytes);
  }

  private copy(from: number, length: number): Uint8Array {
    const start = from % this.ring.length;
    const first = Math.min(length, this.ring.length - start);
    const out = new Uint8Array(length);
    copyBytes(out, 0, this.ring, start, start + first);
    if (first < length) copyBytes(out, first, this.ring, 0, length - first);
    return out;
  }
}

/** Build the automaton once, outside the request path. A transformer is
 *  constructed per stream and the trie is identical every time, so for a fixed
 *  set do this at module scope and pass the result as `needles`.
 *
 *  Matches bytes, not characters: no Unicode normalization, so NFC and NFD
 *  spellings of the same text do not match each other. Duplicates keep the
 *  first entry. Throws on an empty set, an empty needle, or a set whose
 *  transition table would be too large. */
export function compileNeedles(
  source: NeedleSource,
  options?: CompileNeedleOptions,
): CompiledNeedles {
  const needles: Uint8Array[] = [];
  let values: Uint8Array[] | undefined;

  if (Array.isArray(source)) {
    for (const needle of source) needles.push(encodeText(needle, "needle"));
  } else {
    const entries =
      source instanceof Map
        ? [...source]
        : (Object.entries(source as Record<string, string | Uint8Array>) as [
            string | Uint8Array,
            string | Uint8Array,
          ][]);
    values = [];
    for (const [needle, value] of entries) {
      needles.push(encodeText(needle, "needle"));
      values.push(encodeText(value, "replacement"));
    }
  }

  if (needles.length === 0) throw new TypeError("needles must not be empty");
  for (const needle of needles) {
    if (needle.length === 0) throw new TypeError("needles must be non-empty");
  }
  const max = options?.maxTableBytes;
  if (max !== undefined && (!Number.isSafeInteger(max) || max <= 0)) {
    throw new RangeError("maxTableBytes must be a positive safe integer");
  }
  return new NeedleSet(needles, values, max);
}

export function compileNeedleOptions(options: NeedleTransformOptions): CompiledNeedleOptions {
  if (options == null || typeof options !== "object") {
    throw new TypeError("options must be an object");
  }
  const source = options.needles;
  const set = source instanceof NeedleSet ? source : compileNeedles(source, options);

  let resolve = options.resolve;
  if (resolve === undefined) {
    if (set.values === undefined) {
      throw new TypeError("resolve is required when needles is an array");
    }
    const table = set.values;
    resolve = (_needle, index) => table[index];
  } else if (typeof resolve !== "function") {
    throw new TypeError("resolve must be a function");
  }

  const flushBytes = options.flushBytes ?? 16384;
  if (!Number.isSafeInteger(flushBytes) || flushBytes < 0) {
    throw new RangeError("flushBytes must be a non-negative safe integer");
  }
  if (options.onDone !== undefined && typeof options.onDone !== "function") {
    throw new TypeError("onDone must be a function");
  }

  return { set, resolve, flushBytes, onDone: options.onDone };
}

/**
 * Literal multi-pattern substitution, leftmost-longest. Substituted bytes are
 * never re-scanned, so a replacement containing a needle cannot cascade.
 *
 * Only bytes that could begin a needle are held; the rest leave as views into
 * the chunk they arrived in. The held window is bounded by the longest needle,
 * so carried state is independent of body size.
 *
 * `scan` is the whole scanner: a window surviving a chunk boundary is bridged
 * with the next chunk's leading bytes and scanned as one buffer, and a
 * substitution re-scans in place rather than through a pushback queue. Not part
 * of the public API.
 */
export class NeedleSubstituter {
  done = false;
  paused = false;
  private chunk: Uint8Array = EMPTY;
  private bridgeTake = 0;
  private flushing = false;
  private flushTail = false;
  private index: IndexedNeedles | undefined;
  private replayed = 0;
  private pendingCommit = false;
  private readonly resolve: NeedleResolver;
  private readonly onDone: ((stats: NeedleStats) => void) | undefined;
  private readonly out: Emitter;
  // Automaton tables, cached to skip the double indirection per byte.
  private readonly delta: Uint16Array | Int32Array;
  private readonly classOf: Uint16Array;
  private readonly width: number;
  private readonly outLen: Uint16Array | Int32Array;
  private readonly outIdx: Int32Array;
  private readonly depth: Uint16Array | Int32Array;
  private readonly firstByteMask: Uint8Array;
  private readonly soleFirstByte: number;
  private readonly maxLength: number;
  private readonly needles: readonly Uint8Array[];
  private readonly ac: AhoCorasick;

  private bytesIn = 0;
  private substituted = 0;
  private rejected = 0;

  /** The tail that may still match, grown only when a chunk strands a prefix.
   *  At most two windows: held bytes followed by the next chunk's bridge. */
  private hold: Uint8Array<ArrayBuffer> = EMPTY;
  private holdLen = 0;

  // The buffer being scanned. `srcOwned` marks one the scanner reuses, whose
  // spans must be copied to be emitted; a chunk goes out by reference.
  private src: Uint8Array<ArrayBufferLike> = EMPTY;
  private srcEnd = 0;
  private srcOwned = false;

  private i = 0;
  private flushStart = 0;
  private node = 0;
  /** Index in `src` of the pending match, or -1. */
  private candStart = -1;
  private candLen = 0;
  private candIdx = -1;

  constructor(options: NeedleTransformOptions) {
    const compiled = compileNeedleOptions(options);
    this.needles = compiled.set.needles;
    this.ac = compiled.set.ac;
    this.resolve = compiled.resolve;
    this.onDone = compiled.onDone;
    this.out = new Emitter(compiled.flushBytes);
    const ac = compiled.set.ac;
    this.delta = ac.delta;
    this.classOf = ac.classOf;
    this.width = ac.width;
    this.outLen = ac.outLen;
    this.outIdx = ac.outIdx;
    this.depth = ac.depth;
    this.firstByteMask = ac.firstBytes;
    this.soleFirstByte = ac.soleFirstByte;
    this.maxLength = ac.maxLength;
  }

  transform(chunk: Uint8Array, ctrl: Controller): void {
    try {
      if (!(chunk instanceof Uint8Array)) throw new TypeError("chunk must be a Uint8Array");
      this.out.ctrl = ctrl;
      this.bytesIn += chunk.length;
      this.chunk = chunk;
      if (chunk.length > 0) this.consume(chunk, chunk.length);
      if (!this.paused) this.finishChunk();
    } catch (error) {
      this.reset();
      throw error;
    }
  }

  private finishChunk(): void {
    if (this.index !== undefined) {
      this.hold = EMPTY;
      this.holdLen = 0;
    }
    this.out.flush();
    this.out.ctrl = undefined;
    this.src = this.chunk = EMPTY;
  }

  resumeOutput(ctrl: Controller): void {
    if (!this.paused) return;
    this.paused = false;
    if (this.flushing) {
      this.flush(ctrl);
      return;
    }
    this.out.ctrl = ctrl;
    this.continueConsume();
    if (!this.paused) this.finishChunk();
  }

  flush(ctrl: Controller): void {
    this.out.ctrl = ctrl;
    this.flushing = true;
    try {
      this.flushHeld();
      if (this.paused) return;
      this.out.flush();
    } catch (error) {
      this.reset();
      throw error;
    }
    this.reset();
    this.done = true;
    if (this.onDone !== undefined) {
      this.onDone({
        substituted: this.substituted,
        rejected: this.rejected,
        bytesIn: this.bytesIn,
        bytesOut: this.out.bytesOut,
      });
    }
  }

  private flushHeld(): void {
    if (this.index !== undefined) {
      this.scan();
      if (!this.paused) this.paused = !this.index.finish();
      return;
    }
    if (this.holdLen > 0 && !this.flushTail) {
      // Nothing follows, so the window is decided. Copied, so its spans can go
      // out by reference; entered past the end, since it is already scanned.
      const tail = this.hold.slice(0, this.holdLen);
      this.holdLen = 0;
      this.enter(tail, tail.length, false, tail.length, 0);
      this.flushTail = true;
      if (tail.length > 256) {
        this.indexedScan(0);
        if (this.paused) return;
        this.paused = !(this.index as unknown as IndexedNeedles).finish();
        return;
      }
    }
    if (this.flushTail) {
      this.scan();
      if (this.paused) return;
      // Looped, not an `if`: re-scanning the bytes behind a decided match can
      // leave a fresh candidate with nothing after it to force the decision.
      while (this.candStart >= 0) {
        this.commit();
        if (this.out.blocked) {
          this.paused = true;
          return;
        }
        this.scan();
        if (this.paused) return;
      }
      this.emitSpan(this.srcEnd);
    }
  }

  /** Scan one chunk, bridging a window the previous one stranded. */
  private consume(chunk: Uint8Array, count: number): void {
    if (this.holdLen > 0) {
      const held = this.holdLen;
      const take = this.maxLength < count ? this.maxLength : count;
      this.ensureHold(held + take);
      copyBytes(this.hold, held, chunk, 0, take);
      // The window sits at offset 0, so a pending match's index carries over.
      this.enter(this.hold, held + take, true, held, 0);
      this.bridgeTake = take;
    } else {
      this.enter(chunk, count, false, 0, 0);
    }
    this.continueConsume();
  }

  private continueConsume(): void {
    this.scan();
    if (this.paused) return;
    const take = this.bridgeTake;
    if (take > 0) {
      this.bridgeTake = 0;
      const chunk = this.chunk;
      const count = chunk.length;
      if (take === count) {
        this.park();
        return;
      }
      // `take` was maxLength, so any surviving window is at most that long and
      // lies wholly in the bridged bytes: rebase it instead of copying it.
      const held = this.srcEnd - take;
      const ws = this.srcEnd - this.windowLen();
      this.emitSpan(ws);
      this.holdLen = 0;
      if (this.candStart >= 0) this.candStart -= held;
      this.enter(chunk, count, false, take, ws - held);
      this.scan();
      if (this.paused) return;
    }
    this.park();
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

  /** Run the automaton over src[i..srcEnd). State lives in fields, so it
   *  resumes across buffers and chunks. */
  private scan(): void {
    if (this.pendingCommit) {
      this.commit();
      if (this.paused) return;
    }
    if (this.index !== undefined) {
      this.i = this.index.scan(this.src, this.i, this.srcEnd);
      this.flushStart = this.i;
      this.node = 0;
      this.candStart = -1;
      this.candLen = 0;
      this.paused = this.out.blocked;
      return;
    }
    const delta = this.delta;
    const classOf = this.classOf;
    const width = this.width;
    const outLen = this.outLen;
    const outIdx = this.outIdx;
    const depth = this.depth;
    const mask = this.firstByteMask;
    const sole = this.soleFirstByte;
    const src = this.src;
    const end = this.srcEnd;
    let i = this.i;
    let node = this.node;
    let candStart = this.candStart;
    let candLen = this.candLen;
    let candIdx = this.candIdx;

    while (i < end) {
      if (node === 0) {
        // Only a needle's first byte leaves the root.
        if (sole >= 0) {
          // Bounded by `end`, not by the buffer: `hold` outlives its span.
          const at = src.indexOf(sole, i);
          if (at < 0 || at >= end) {
            i = end;
            break;
          }
          i = at;
        } else {
          while (i < end && mask[src[i]] === 0) i++;
          if (i >= end) break;
        }
      }
      node = delta[node * width + classOf[src[i]]];
      i++;
      const len = outLen[node];
      if (len !== 0) {
        const start = i - len;
        if (candStart < 0 || start < candStart) {
          candStart = start;
          candLen = len;
          candIdx = outIdx[node];
        } else if (start === candStart && len > candLen) {
          candLen = len;
          candIdx = outIdx[node];
        }
      }
      // A maximum-length match cannot extend or lose to an earlier match.
      // Otherwise wait until no live prefix reaches back to its start.
      if (candStart >= 0 && (candLen === this.maxLength || i - depth[node] > candStart)) {
        this.i = i;
        this.node = node;
        this.candStart = candStart;
        this.candLen = candLen;
        this.candIdx = candIdx;
        this.commit();
        if (this.paused) return;
        this.replayed += i - this.i;
        if (this.replayed > Math.max(1024, this.bytesIn)) {
          this.indexedScan(this.i);
          return;
        }
        // Bytes consumed past the match re-scan from the root, in place.
        i = this.i;
        node = 0;
        candStart = -1;
        candLen = 0;
        if (this.out.blocked) {
          this.paused = true;
          break;
        }
      }
    }

    this.i = i;
    this.node = node;
    this.candStart = candStart;
    this.candLen = candLen;
    this.candIdx = candIdx;
  }

  /** Switch once, retaining the index across chunks. */
  private indexedScan(from: number): void {
    this.index = new IndexedNeedles(
      this.needles,
      this.ac,
      (payload, index) => {
        const value = this.resolve(payload, index);
        if (value === null) this.rejected++;
        else this.substituted++;
        return value;
      },
      this.out,
    );
    this.i = from;
    this.scan();
  }

  /** Emit the content before the pending match and then the match's
   *  replacement, leaving the cursor just past it with a reset automaton. */
  private commit(): void {
    const start = this.candStart;
    const end = start + this.candLen;
    this.emitSpan(start);
    if (this.out.blocked) {
      this.pendingCommit = true;
      this.paused = true;
      return;
    }
    this.pendingCommit = false;
    const value = this.resolve(this.src.subarray(start, end), this.candIdx);
    if (value === null) {
      // Atomic: verbatim, and not re-scanned.
      this.emitRange(start, end);
      this.rejected++;
    } else {
      if (value.length > 0) this.out.emit(value);
      this.substituted++;
    }
    this.i = end;
    this.flushStart = end;
    this.node = 0;
    this.candStart = -1;
    this.candLen = 0;
  }

  /** Bytes at the end of the buffer that must survive it: the live prefix, and
   *  a pending match if it starts before the prefix does. */
  private windowLen(): number {
    const d = this.depth[this.node];
    if (this.candStart < 0) return d;
    const span = this.srcEnd - this.candStart;
    return span > d ? span : d;
  }

  /** End of buffer: emit everything already decided and copy the live window
   *  into `hold`, where the next chunk picks it up. */
  private park(): void {
    const end = this.srcEnd;
    const ws = end - this.windowLen();
    this.emitSpan(ws);
    if (ws < end) {
      this.ensureHold(end - ws);
      // May move within `hold` itself, but only leftwards.
      if (ws !== 0 || this.src !== this.hold) copyBytes(this.hold, 0, this.src, ws, end);
      this.holdLen = end - ws;
      if (this.candStart >= 0) this.candStart -= ws;
    } else {
      this.holdLen = 0;
    }
  }

  private emitSpan(to: number): void {
    if (to > this.flushStart) {
      this.emitRange(this.flushStart, to);
      this.flushStart = to;
    }
  }

  private ensureHold(need: number): void {
    if (need <= this.hold.length) return;
    const size = Math.min(this.maxLength * 2, Math.max(need, this.hold.length * 2, 64));
    const next = new Uint8Array(size);
    next.set(this.hold.subarray(0, this.holdLen));
    this.hold = next;
  }

  private emitRange(from: number, to: number): void {
    const src = this.src;
    if (this.srcOwned) this.out.emit(src.slice(from, to));
    else this.out.emitRange(src, from, to);
  }

  cancel(): void {
    this.reset();
  }

  private reset(): void {
    this.paused = false;
    this.chunk = EMPTY;
    this.bridgeTake = 0;
    this.flushing = this.flushTail = false;
    this.index = undefined;
    this.replayed = 0;
    this.pendingCommit = false;
    this.node = 0;
    this.holdLen = 0;
    this.hold = EMPTY;
    this.candStart = -1;
    this.candLen = 0;
    this.src = EMPTY;
    this.srcEnd = 0;
    this.i = 0;
    this.flushStart = 0;
    this.out.reset();
  }
}

/** Single-use transformer body, for runtimes where `TransformStream` is not a
 *  global. Construct one per stream. */
export function createNeedleTransformer(options: NeedleTransformOptions): NeedleTransformer {
  let s: NeedleSubstituter | undefined = new NeedleSubstituter(options);
  const body = {
    get paused() {
      return s?.paused ?? false;
    },
    resume: (ctrl: Controller) => {
      try {
        s?.resumeOutput(ctrl);
        if (s?.done) s = undefined;
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
      try {
        active?.flush(ctrl);
      } finally {
        if (!active?.paused) s = undefined;
      }
    },
    cancel: () => {
      s?.cancel();
      s = undefined;
    },
  };
  return body;
}

/** Single-use native transform. */
export function createNeedleTransformStream(
  options: NeedleTransformOptions,
): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>(createNeedleTransformer(options));
}

/** Single-use pair with intra-chunk backpressure. */
export function createNeedleStreamPair(
  options: NeedleTransformOptions,
): ReadableWritablePair<Uint8Array, Uint8Array> {
  return flowStream(createNeedleTransformer(options));
}
