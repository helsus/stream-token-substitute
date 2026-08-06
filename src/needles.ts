import { AhoCorasick } from "./aho-corasick.ts";
import { copyBytes, EMPTY, encodeText, grow, requeue } from "./bytes.ts";
import { Emitter } from "./output.ts";

type Controller = TransformStreamDefaultController<Uint8Array>;

/** Resolve a matched needle to substitution bytes. `index` is its position in
 *  the compiled set. Bytes are enqueued by reference, so do not mutate them
 *  after. null emits the needle verbatim. `needle` is scratch. */
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

export interface NeedleTransformOptions {
  /** A `NeedleSource`, or the result of `compileNeedles`. */
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

  constructor(needles: Uint8Array[], values: Uint8Array[] | undefined) {
    this.needles = needles;
    this.values = values;
    this.ac = new AhoCorasick(needles);
  }
}

export type CompiledNeedles = NeedleSet;

/** Build the automaton once, outside the request path. A transformer is
 *  constructed per stream and the trie is identical every time, so for a fixed
 *  set do this at module scope and pass the result as `needles`. */
export function compileNeedles(source: NeedleSource): CompiledNeedles {
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
  return new NeedleSet(needles, values);
}

export function compileNeedleOptions(options: NeedleTransformOptions): CompiledNeedleOptions {
  if (options == null || typeof options !== "object") {
    throw new TypeError("options must be an object");
  }
  const source = options.needles;
  const set = source instanceof NeedleSet ? source : compileNeedles(source);

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
 * so carried state is independent of body size. Not part of the public API.
 */
export class NeedleSubstituter {
  private readonly resolve: NeedleResolver;
  private readonly onDone: ((stats: NeedleStats) => void) | undefined;
  private readonly out: Emitter;
  // Automaton tables, cached to skip the double indirection per byte.
  private readonly delta: Uint16Array | Int32Array;
  private readonly classOf: Uint16Array;
  private readonly width: number;
  private readonly outLen: Int32Array;
  private readonly outIdx: Int32Array;
  private readonly depth: Int32Array;
  private readonly firstByteMask: Uint8Array;
  private readonly soleFirstByte: number;

  private node = 0;
  private bytesIn = 0;
  private substituted = 0;
  private rejected = 0;

  /** The tail of the input that may still begin or continue a match. */
  private hold: Uint8Array<ArrayBuffer>;
  private holdLen = 0;
  /** Index in `hold` of the pending match, or -1. */
  private candStart = -1;
  private candLen = 0;
  private candIdx = -1;

  // Bytes pushed back after a substitution, re-scanned from a reset automaton.
  private queue = EMPTY;
  private queueLen = 0;
  private qi = 0;
  private queueGen = 0;
  private draining = false;

  private chunk: Uint8Array<ArrayBufferLike> = EMPTY;
  private i = 0;
  private flushStart = 0;

  constructor(options: NeedleTransformOptions) {
    const compiled = compileNeedleOptions(options);
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
    this.hold = new Uint8Array(ac.maxLength);
  }

  transform(chunk: Uint8Array, ctrl: Controller): void {
    if (!(chunk instanceof Uint8Array)) throw new TypeError("chunk must be a Uint8Array");
    this.out.ctrl = ctrl;
    this.chunk = chunk;
    this.i = 0;
    this.flushStart = 0;
    this.bytesIn += chunk.length;

    const count = chunk.length;
    // A window carried across the chunk boundary goes through the per-byte
    // path until it dies; everything after runs in chunk coordinates.
    while (this.i < count && this.holdLen > 0) {
      this.step(chunk[this.i], false);
      this.i++;
      if (this.queueLen > 0) this.drain();
    }
    if (this.i < count) this.fastScan(chunk, count);

    this.flushSpan(count);
    // Must precede dropping the chunk: buffered spans are views into it.
    this.out.flush();
    this.chunk = EMPTY;
  }

  flush(ctrl: Controller): void {
    this.out.ctrl = ctrl;
    // Looped, not an `if`: re-scanning the bytes held behind a decided match
    // can leave a fresh candidate with nothing after it to force the decision.
    while (this.candStart >= 0) {
      this.substitute();
      if (this.queueLen > 0) this.drain();
    }
    this.releaseHold(this.holdLen);
    this.out.flush();
    this.reset();
    if (this.onDone !== undefined) {
      this.onDone({
        substituted: this.substituted,
        rejected: this.rejected,
        bytesIn: this.bytesIn,
        bytesOut: this.out.bytesOut,
      });
    }
  }

  /** Scan [this.i, count) in chunk coordinates: no hold copies, no queue, and
   *  content, matches, and near-misses leave as views into the chunk. Entered
   *  with an empty window; one still alive at the end is copied into `hold`. */
  private fastScan(chunk: Uint8Array<ArrayBufferLike>, count: number): void {
    const delta = this.delta;
    const classOf = this.classOf;
    const width = this.width;
    const outLen = this.outLen;
    const outIdx = this.outIdx;
    const depth = this.depth;
    const mask = this.firstByteMask;
    const sole = this.soleFirstByte;
    let i = this.i;
    let flushStart = this.flushStart;
    let node = 0;
    let candStart = -1;
    let candLen = 0;
    let candIdx = -1;

    while (i < count) {
      if (node === 0) {
        // Only a needle's first byte leaves the root.
        if (sole >= 0) {
          i = chunk.indexOf(sole, i);
          if (i < 0) {
            i = count;
            break;
          }
        } else {
          while (i < count && mask[chunk[i]] === 0) i++;
          if (i >= count) break;
        }
      }
      node = delta[node * width + classOf[chunk[i]]];
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
      // Decided once no live prefix reaches back to the candidate's start.
      if (candStart >= 0 && i - depth[node] > candStart) {
        if (candStart > flushStart) this.out.emit(chunk.subarray(flushStart, candStart));
        const end = candStart + candLen;
        const value = this.resolve(chunk.subarray(candStart, end), candIdx);
        if (value === null) {
          // Atomic: verbatim, and not re-scanned.
          this.out.emit(chunk.subarray(candStart, end));
          this.rejected++;
        } else {
          if (value.length > 0) this.out.emit(value);
          this.substituted++;
        }
        // Bytes consumed past the match re-scan from the root, in place.
        i = end;
        flushStart = end;
        node = 0;
        candStart = -1;
        candLen = 0;
      }
    }

    // An undecided candidate lies inside the live prefix, so the prefix is
    // the whole window.
    const ws = count - depth[node];
    if (ws < count) {
      if (ws > flushStart) this.out.emit(chunk.subarray(flushStart, ws));
      copyBytes(this.hold, 0, chunk, ws, count);
      this.holdLen = count - ws;
      this.node = node;
      this.candStart = candStart >= 0 ? candStart - ws : -1;
      this.candLen = candLen;
      this.candIdx = candIdx;
      this.flushStart = count;
    } else {
      this.flushStart = flushStart;
    }
    this.i = count;
  }

  /** Per-byte automaton. A byte reaching here leaves the pending span and is
   *  held instead, so content is emitted exactly once. */
  private step(byte: number, buffered: boolean): void {
    if (!buffered) {
      this.flushSpan(this.i);
      this.flushStart = this.i + 1;
    }

    const node = this.delta[this.node * this.width + this.classOf[byte]];
    this.node = node;
    this.holdPush(byte);

    const len = this.outLen[node];
    if (len > 0) {
      const start = this.holdLen - len;
      if (this.candStart < 0 || start < this.candStart) {
        this.candStart = start;
        this.candLen = len;
        this.candIdx = this.outIdx[node];
      } else if (start === this.candStart && len > this.candLen) {
        this.candLen = len;
        this.candIdx = this.outIdx[node];
      }
    }

    // The window keeps the live prefix and the pending match; the rest is content.
    const d = this.depth[node];
    const span = this.candStart >= 0 ? this.holdLen - this.candStart : 0;
    const need = d > span ? d : span;
    if (this.holdLen > need) this.releaseHold(this.holdLen - need);

    // Decided once no live prefix reaches back to the candidate's start.
    if (this.candStart >= 0 && d < this.holdLen - this.candStart) this.substitute();
  }

  /** Emit the pending match and push back everything held after it. */
  private substitute(): void {
    // Live-prefix bytes before the match are dead now: plain content.
    if (this.candStart > 0) this.releaseHold(this.candStart);
    const len = this.candLen;
    const value = this.resolve(this.hold.subarray(0, len), this.candIdx);
    if (value === null) {
      // Atomic: verbatim, and not re-scanned.
      this.out.emit(this.hold.slice(0, len));
      this.rejected++;
    } else {
      if (value.length > 0) this.out.emit(value);
      this.substituted++;
    }

    // Trailing held bytes precede whatever is left of the queue being drained.
    const trail = this.holdLen - len;
    const rest = this.draining ? this.queueLen - this.qi : 0;
    const restStart = this.qi;
    const q = (this.queue = requeue(this.queue, trail, rest, restStart, 16));
    copyBytes(q, 0, this.hold, len, this.holdLen);

    this.queueLen = trail + rest;
    this.qi = 0;
    this.queueGen++;
    this.node = 0;
    this.holdLen = 0;
    this.candStart = -1;
    this.candLen = 0;
  }

  /** Re-scan pushed-back bytes before consuming more input. */
  private drain(): void {
    this.draining = true;
    while (this.qi < this.queueLen) {
      const byte = this.queue[this.qi];
      const gen = this.queueGen;
      this.qi++;
      this.step(byte, true);
      // substitute() rewrote the queue and reset the cursor; carry on from it.
      if (this.queueGen !== gen) continue;
    }
    this.queueLen = 0;
    this.qi = 0;
    this.draining = false;
  }

  private holdPush(byte: number): void {
    if (this.holdLen === this.hold.length) {
      this.hold = grow(this.hold, this.holdLen, this.holdLen + 1, 16);
    }
    this.hold[this.holdLen++] = byte;
  }

  /** No backing span, so releasing copies. Only near-misses get here. */
  private releaseHold(count: number): void {
    if (count <= 0) return;
    this.out.emit(this.hold.slice(0, count));
    this.holdLen -= count;
    if (this.holdLen > 0) this.hold.copyWithin(0, count, count + this.holdLen);
    if (this.candStart > 0) this.candStart -= count;
  }

  private flushSpan(end: number): void {
    if (end > this.flushStart) {
      this.out.emit(this.chunk.subarray(this.flushStart, end));
      this.flushStart = end;
    }
  }

  private reset(): void {
    this.node = 0;
    this.holdLen = 0;
    this.candStart = -1;
    this.chunk = EMPTY;
    this.queue = EMPTY;
    this.queueLen = 0;
    this.qi = 0;
    this.draining = false;
    this.out.reset();
  }
}

/** Single-use transformer body, for runtimes where `TransformStream` is not a
 *  global. Construct one per stream. */
export function createNeedleTransformer(options: NeedleTransformOptions): NeedleTransformer {
  const s = new NeedleSubstituter(options);
  return {
    transform: (chunk, ctrl) => s.transform(chunk, ctrl),
    flush: (ctrl) => s.flush(ctrl),
  };
}

/** Single-use TransformStream. Construct one per stream. */
export function createNeedleTransformStream(
  options: NeedleTransformOptions,
): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>(createNeedleTransformer(options));
}
