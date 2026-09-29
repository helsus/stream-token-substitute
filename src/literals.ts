import { AhoCorasick, DEFAULT_MAX_MEMORY_BYTES } from "./aho-corasick.ts";
import { copyBytes, EMPTY, encodeText } from "./bytes.ts";
import { checkSignal, type FlowBody, flowStream } from "./flow.ts";
import { Emitter } from "./output.ts";

export { DEFAULT_MAX_MEMORY_BYTES } from "./aho-corasick.ts";

type Controller = TransformStreamDefaultController<Uint8Array>;

/** Replacement for a matched literal. `literal` is a view valid only during the
 *  call, returning it is safe. Bytes are enqueued by reference. null emits it verbatim. */
export type LiteralResolver = (literal: Uint8Array, index: number) => Uint8Array | string | null;

type ByteResolver = (literal: Uint8Array, index: number) => Uint8Array | null;

/** Counts for one stream, delivered once from `flush`. */
export interface LiteralStats {
  substituted: number;
  rejected: number;
  bytesIn: number;
  bytesOut: number;
}

/** A record or Map supplies replacements. An array needs `resolve`. */
export type LiteralSource =
  | Record<string, string | Uint8Array>
  | Map<string | Uint8Array, string | Uint8Array>
  | readonly (string | Uint8Array)[];

export interface CompileLiteralOptions {
  /** Estimated compile memory ceiling. Default `DEFAULT_MAX_MEMORY_BYTES`. */
  maxMemoryBytes?: number;
}

export interface LiteralTransformOptions extends CompileLiteralOptions {
  /** Ignores `maxMemoryBytes` when already compiled. */
  literals: LiteralSource | CompiledLiterals;
  /** Required when `literals` is an array. */
  resolve?: LiteralResolver;
  /** Output merge threshold in bytes. Default 16384. */
  flushBytes?: number;
  /** Stops the stream with the abort reason. */
  signal?: AbortSignal;
  onDone?: (stats: LiteralStats) => void;
}

export interface LiteralTransformer {
  transform(chunk: Uint8Array, controller: Controller): void;
  flush(controller: Controller): void;
  cancel?(reason?: unknown): void;
}

interface CompiledLiteralOptions {
  set: LiteralSet;
  resolve: ByteResolver;
  flushBytes: number;
  onDone: ((stats: LiteralStats) => void) | undefined;
}

/** The automaton plus the bytes it matches. Opaque and immutable. */
class LiteralSet {
  readonly literals: Uint8Array[];
  readonly values: Uint8Array[] | undefined;
  readonly ac: AhoCorasick;

  constructor(
    literals: Uint8Array[],
    values: Uint8Array[] | undefined,
    maxMemoryBytes: number,
    reserved: number,
  ) {
    this.literals = literals;
    this.values = values;
    this.ac = new AhoCorasick(literals, maxMemoryBytes, reserved);
  }
}

export type CompiledLiterals = LiteralSet;

/** Rolling match index for overlap-heavy streams. */
class IndexedLiterals {
  private readonly literals: readonly Uint8Array[];
  private readonly ac: AhoCorasick;
  private readonly resolve: ByteResolver;
  private readonly out: Emitter;
  private readonly ring: Uint8Array;
  /** Per start position in the ring: longest literal index + 1, 0 for none. */
  private readonly matches: Uint32Array;
  private read = 0;
  private at = 0;
  private plain = 0;
  private node = 0;

  constructor(
    literals: readonly Uint8Array[],
    ac: AhoCorasick,
    resolve: ByteResolver,
    out: Emitter,
  ) {
    this.literals = literals;
    this.ac = ac;
    this.resolve = resolve;
    this.out = out;
    this.ring = new Uint8Array(ac.maxLength + 1);
    this.matches = new Uint32Array(this.ring.length);
  }

  scan(src: Uint8Array, from: number, end: number): number {
    const { delta, width, classOf, own, dict } = this.ac;
    const size = this.ring.length;
    this.settle(false);
    for (let i = from; i < end; i++) {
      if (this.read - this.plain === this.ring.length) this.flushPlain();
      if (this.out.blocked) return i;
      const byte = src[i];
      const slot = this.read % size;
      this.ring[slot] = byte;
      this.matches[slot] = 0;
      const node = delta[this.node * width + classOf[byte]];
      this.node = node;
      this.read++;
      // Every literal ending here, longest (earliest start) first.
      for (let s = own[node] >= 0 ? node : dict[node]; s !== 0; s = dict[s]) {
        const p = own[s];
        const length = this.literals[p].length;
        const start = this.read - length;
        if (start < this.at) continue;
        const pos = start % size;
        const prev = this.matches[pos];
        if (prev === 0 || this.literals[prev - 1].length < length) this.matches[pos] = p + 1;
      }
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
      const length = this.literals[match - 1].length;
      // A fresh copy, so returning it is safe.
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

/** Compile once at module scope and reuse. Byte matching, duplicates keep the first. */
export function compileLiterals(
  source: LiteralSource,
  options?: CompileLiteralOptions,
): CompiledLiterals {
  const max = options?.maxMemoryBytes ?? DEFAULT_MAX_MEMORY_BYTES;
  if (!Number.isSafeInteger(max) || max <= 0) {
    throw new RangeError("maxMemoryBytes must be a positive safe integer");
  }
  const literals: Uint8Array[] = [];
  let values: Uint8Array[] | undefined;
  let reserved = 0;
  const add = (list: Uint8Array[], bytes: Uint8Array) => {
    reserved += bytes.length;
    if (reserved > max) {
      throw new RangeError(
        `literal set too large: literals and values take ${reserved} bytes, ` +
          `over the ${max} byte maxMemoryBytes limit`,
      );
    }
    list.push(bytes);
  };

  if (Array.isArray(source)) {
    for (const literal of source) add(literals, encodeText(literal, "literal"));
  } else {
    const entries =
      source instanceof Map
        ? [...source]
        : (Object.entries(source as Record<string, string | Uint8Array>) as [
            string | Uint8Array,
            string | Uint8Array,
          ][]);
    values = [];
    for (const [literal, value] of entries) {
      add(literals, encodeText(literal, "literal"));
      add(values, encodeText(value, "replacement"));
    }
  }

  if (literals.length === 0) throw new TypeError("literals must not be empty");
  for (const literal of literals) {
    if (literal.length === 0) throw new TypeError("literals must be non-empty");
  }
  return new LiteralSet(literals, values, max, reserved);
}

function kindOf(value: unknown): string {
  if (typeof value !== "object") return typeof value;
  if (Array.isArray(value)) return "array";
  return (value as object).constructor?.name ?? "object";
}

export function compileLiteralOptions(options: LiteralTransformOptions): CompiledLiteralOptions {
  if (options == null || typeof options !== "object") {
    throw new TypeError("options must be an object");
  }
  const source = options.literals;
  const set = source instanceof LiteralSet ? source : compileLiterals(source, options);

  const user = options.resolve;
  let resolve: ByteResolver;
  if (user === undefined) {
    if (set.values === undefined) {
      throw new TypeError("resolve is required when literals is an array");
    }
    const table = set.values;
    resolve = (_literal, index) => table[index];
  } else if (typeof user !== "function") {
    throw new TypeError("resolve must be a function");
  } else {
    resolve = (literal, index) => {
      const value: unknown = user(literal, index);
      if (value === null || value instanceof Uint8Array) return value;
      if (typeof value === "string") return encodeText(value, "replacement");
      throw new TypeError(`resolve must return Uint8Array, string, or null; got ${kindOf(value)}`);
    };
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

/** Leftmost-longest literal scanner. Held bytes are bounded by the longest literal. */
export class LiteralSubstituter {
  done = false;
  paused = false;
  private chunk: Uint8Array = EMPTY;
  private bridgeTake = 0;
  private flushing = false;
  private flushTail = false;
  private index: IndexedLiterals | undefined;
  private replayed = 0;
  private pendingCommit = false;
  private readonly resolve: ByteResolver;
  private readonly onDone: ((stats: LiteralStats) => void) | undefined;
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
  private readonly literals: readonly Uint8Array[];
  private readonly ac: AhoCorasick;

  private bytesIn = 0;
  private substituted = 0;
  private rejected = 0;

  /** Tail that may still match: held bytes, then the next chunk bridge. */
  private hold: Uint8Array<ArrayBuffer> = EMPTY;
  private holdLen = 0;

  // `srcOwned`: src is a reused scratch, so its spans are copied out.
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

  constructor(options: LiteralTransformOptions) {
    const compiled = compileLiteralOptions(options);
    this.literals = compiled.set.literals;
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
      // Nothing follows, so the window is decided. Scanned already, so enter past its end.
      const tail = this.hold.slice(0, this.holdLen);
      this.holdLen = 0;
      this.enter(tail, tail.length, false, tail.length, 0);
      this.flushTail = true;
      if (tail.length > 256) {
        const index = this.indexedScan(0);
        if (this.paused) return;
        this.paused = !index.finish();
        return;
      }
    }
    if (this.flushTail) {
      this.scan();
      if (this.paused) return;
      // A re-scan behind a decided match can leave a fresh candidate.
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
      // The surviving window lies wholly in the bridged bytes, so rebase it.
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

  /** Run the automaton over src[i..srcEnd). */
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
        // Only a literal's first byte leaves the root.
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
      // A maximum-length match cannot extend or lose to an earlier one.
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
  private indexedScan(from: number): IndexedLiterals {
    const index = new IndexedLiterals(
      this.literals,
      this.ac,
      (payload, at) => {
        const value = this.resolve(payload, at);
        if (value === null) this.rejected++;
        else this.substituted++;
        return value;
      },
      this.out,
    );
    this.index = index;
    this.i = from;
    this.scan();
    return index;
  }

  /** Emit up to the pending match, then its replacement. */
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
      // A view of a buffer the scanner reuses must be copied to outlive it.
      if (value.length > 0) {
        this.out.emit(this.srcOwned && value.buffer === this.src.buffer ? value.slice() : value);
      }
      this.substituted++;
    }
    this.i = end;
    this.flushStart = end;
    this.node = 0;
    this.candStart = -1;
    this.candLen = 0;
  }

  /** Start of the bytes that must outlive the buffer. */
  private windowLen(): number {
    const d = this.depth[this.node];
    if (this.candStart < 0) return d;
    const span = this.srcEnd - this.candStart;
    return span > d ? span : d;
  }

  /** End of buffer: emit what is decided, keep the live window in `hold`. */
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

/** Body for `new TransformStream(...)`. Single use. */
export function createLiteralTransformer(options: LiteralTransformOptions): LiteralTransformer {
  let signal = options?.signal;
  checkSignal(signal);
  let s: LiteralSubstituter | undefined = new LiteralSubstituter(options);
  let failure: { reason: unknown } | undefined;
  const stop = (reason?: unknown) => {
    s?.cancel();
    s = undefined;
    failure ??= { reason };
    signal?.removeEventListener("abort", onAbort);
    signal = undefined;
  };
  const onAbort = () => stop(signal?.reason);
  const inactive = () => failure?.reason ?? new TypeError("transformer is no longer active");
  if (signal?.aborted) onAbort();
  else signal?.addEventListener("abort", onAbort, { once: true });
  const body: LiteralTransformer & FlowBody = {
    get paused() {
      return s?.paused ?? false;
    },
    resume: (ctrl: Controller) => {
      if (s === undefined) throw inactive();
      try {
        s.resumeOutput(ctrl);
        if (s.done) stop();
      } catch (error) {
        stop(error);
        throw error;
      }
    },
    transform: (chunk: Uint8Array, ctrl: Controller) => {
      if (s === undefined) throw inactive();
      try {
        s.transform(chunk, ctrl);
      } catch (error) {
        stop(error);
        throw error;
      }
    },
    flush: (ctrl: Controller) => {
      if (s === undefined) throw inactive();
      try {
        s.flush(ctrl);
      } catch (error) {
        stop(error);
        throw error;
      }
      if (!s.paused) stop();
    },
    cancel: stop,
  };
  return body;
}

/** Backpressured pair for `pipeThrough`. Single use. */
export function createLiteralStream(
  options: LiteralTransformOptions,
): ReadableWritablePair<Uint8Array, Uint8Array> {
  return flowStream(createLiteralTransformer(options), options?.signal);
}
