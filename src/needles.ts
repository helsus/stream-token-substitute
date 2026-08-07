import { AhoCorasick } from "./aho-corasick.ts";
import { copyBytes, EMPTY, encodeText } from "./bytes.ts";
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

  private bytesIn = 0;
  private substituted = 0;
  private rejected = 0;

  /** The tail of the input that may still begin or continue a match, at offset
   *  0. Sized for two windows: the bridge bytes go in the upper half. */
  private readonly hold: Uint8Array<ArrayBuffer>;
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
    this.hold = new Uint8Array(ac.maxLength * 2);
  }

  transform(chunk: Uint8Array, ctrl: Controller): void {
    if (!(chunk instanceof Uint8Array)) throw new TypeError("chunk must be a Uint8Array");
    this.out.ctrl = ctrl;
    this.bytesIn += chunk.length;
    if (chunk.length > 0) this.consume(chunk, chunk.length);
    // Must precede dropping the chunk: buffered spans are views into it.
    this.out.flush();
    this.src = EMPTY;
  }

  flush(ctrl: Controller): void {
    this.out.ctrl = ctrl;
    if (this.holdLen > 0) {
      // Nothing follows, so the window is decided. Copied, so its spans can go
      // out by reference; entered past the end, since it is already scanned.
      const tail = this.hold.slice(0, this.holdLen);
      this.holdLen = 0;
      this.enter(tail, tail.length, false, tail.length, 0);
      // Looped, not an `if`: re-scanning the bytes behind a decided match can
      // leave a fresh candidate with nothing after it to force the decision.
      while (this.candStart >= 0) {
        this.commit();
        this.scan();
      }
      this.emitSpan(tail.length);
    }
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

  /** Scan one chunk, bridging a window the previous one stranded. */
  private consume(chunk: Uint8Array, count: number): void {
    if (this.holdLen > 0) {
      const held = this.holdLen;
      const take = this.maxLength < count ? this.maxLength : count;
      copyBytes(this.hold, held, chunk, 0, take);
      // The window sits at offset 0, so a pending match's index carries over.
      this.enter(this.hold, held + take, true, held, 0);
      this.scan();
      if (take === count) {
        this.park();
        return;
      }
      // `take` was maxLength, so any surviving window is at most that long and
      // lies wholly in the bridged bytes: rebase it instead of copying it.
      const ws = this.srcEnd - this.windowLen();
      this.emitSpan(ws);
      this.holdLen = 0;
      if (this.candStart >= 0) this.candStart -= held;
      this.enter(chunk, count, false, take, ws - held);
    } else {
      this.enter(chunk, count, false, 0, 0);
    }
    this.scan();
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
      // Decided once no live prefix reaches back to the candidate's start.
      if (candStart >= 0 && i - depth[node] > candStart) {
        this.i = i;
        this.node = node;
        this.candStart = candStart;
        this.candLen = candLen;
        this.candIdx = candIdx;
        this.commit();
        // Bytes consumed past the match re-scan from the root, in place.
        i = this.i;
        node = 0;
        candStart = -1;
        candLen = 0;
      }
    }

    this.i = i;
    this.node = node;
    this.candStart = candStart;
    this.candLen = candLen;
    this.candIdx = candIdx;
  }

  /** Emit the content before the pending match and then the match's
   *  replacement, leaving the cursor just past it with a reset automaton. */
  private commit(): void {
    const start = this.candStart;
    const end = start + this.candLen;
    this.emitSpan(start);
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
      // May move within `hold` itself, but only leftwards.
      copyBytes(this.hold, 0, this.src, ws, end);
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

  private emitRange(from: number, to: number): void {
    const src = this.src;
    this.out.emit(this.srcOwned ? src.slice(from, to) : src.subarray(from, to));
  }

  private reset(): void {
    this.node = 0;
    this.holdLen = 0;
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
