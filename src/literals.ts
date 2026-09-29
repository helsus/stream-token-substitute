import { AhoCorasick } from "./aho-corasick.ts";
import { copyBytes, EMPTY, encodeText } from "./bytes.ts";
import { flowStream } from "./flow.ts";
import { Lookahead, lookaheadBody, toPromise } from "./lookahead.ts";
import {
  compileShared,
  optionalFunction,
  type Replacement,
  type ResolveContext,
  type ResolveErrorHandler,
  type TokenTransformer,
} from "./types.ts";

/** Compile memory ceiling, sized for a 128 MiB Workers isolate. */
export const DEFAULT_MAX_MEMORY_BYTES = 16 * 1024 * 1024;

/** Replacement for a matched literal, same contract as `TokenResolver`. `literal` is a fresh copy unless `borrow` is set. */
export type LiteralResolver = (
  literal: Uint8Array,
  index: number,
  context: ResolveContext,
) => Replacement | PromiseLike<Replacement>;

/** Counts for one stream, delivered once from `flush`. */
export interface LiteralStats {
  replaced: number;
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
  /** Output merge threshold in bytes. `0` disables merging. Default 16384. */
  mergeBytes?: number;
  /** Pending lookups (thenables and unread streams) scanned ahead of output. Default 4. */
  concurrency?: number;
  /** The resolver never keeps its argument past its synchronous return, so it
   *  gets a view instead of a copy. Do not store it, return a view of it, or read it after an await. */
  borrow?: boolean;
  /** Called when `resolve` throws or rejects. Absent: the error errors the stream. */
  onResolveError?: ResolveErrorHandler;
  /** Stops the stream with the abort reason. */
  signal?: AbortSignal;
  onDone?: (stats: LiteralStats) => void;
}

export type LiteralTransformer = TokenTransformer;

/** @internal */
export interface CompiledLiteralOptions {
  set: LiteralSet;
  /** The replacement table, or the user resolver. */
  resolve: Uint8Array[] | LiteralResolver;
  mergeBytes: number;
  concurrency: number;
  borrow: boolean;
  onResolveError: ResolveErrorHandler | undefined;
  onDone: ((stats: LiteralStats) => void) | undefined;
}

/** @internal The automaton plus the bytes it matches. */
export interface LiteralSet {
  readonly literals: Uint8Array[];
  readonly values: Uint8Array[] | undefined;
  readonly ac: AhoCorasick;
}

/** A compiled literal set from `compileLiterals`. Opaque and immutable. */
export class CompiledLiterals {
  // biome-ignore lint/correctness/noUnusedPrivateClassMembers: nominal brand
  #brand = true;
}

const compiledSets = new WeakMap<CompiledLiterals, LiteralSet>();

/** Rolling match index for overlap-heavy streams. */
class IndexedLiterals {
  private readonly literals: readonly Uint8Array[];
  private readonly ac: AhoCorasick;
  private readonly owner: LiteralSubstituter;
  private readonly ring: Uint8Array;
  /** Per start position in the ring: longest literal index + 1, 0 for none. */
  private readonly matches: Uint32Array;
  private read = 0;
  private at = 0;
  private plain = 0;
  private node = 0;

  constructor(literals: readonly Uint8Array[], ac: AhoCorasick, owner: LiteralSubstituter) {
    this.literals = literals;
    this.ac = ac;
    this.owner = owner;
    this.ring = new Uint8Array(ac.maxLength + 1);
    this.matches = new Uint32Array(this.ring.length);
  }

  scan(src: Uint8Array, from: number, end: number): number {
    const { delta, width, classOf, own, dict } = this.ac;
    const size = this.ring.length;
    this.settle(false);
    for (let i = from; i < end; i++) {
      if (this.read - this.plain === this.ring.length) this.flushPlain();
      if (this.owner.blocked()) return i;
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
      if (this.owner.blocked()) return i + 1;
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
      if (this.owner.blocked()) return;
      const length = this.literals[match - 1].length;
      const table = this.owner.table;
      if (table !== undefined) this.owner.replace(table[match - 1]);
      else this.owner.resolveMatch(match - 1, this.copy(this.at, length), false);
      this.at += length;
      this.plain = this.at;
      if (this.owner.blocked()) return;
    }
  }

  private flushPlain(): void {
    if (this.plain === this.at) return;
    const bytes = this.copy(this.plain, this.at - this.plain);
    this.plain = this.at;
    this.owner.send(bytes);
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
    const entries: Iterable<[string | Uint8Array, string | Uint8Array]> =
      source instanceof Map ? source : Object.entries(source);
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
  const compiled = new CompiledLiterals();
  compiledSets.set(compiled, { literals, values, ac: new AhoCorasick(literals, max, reserved) });
  return compiled;
}

/** @internal */
export function compileLiteralOptions(options: LiteralTransformOptions): CompiledLiteralOptions {
  if (options == null || typeof options !== "object") {
    throw new TypeError("options must be an object");
  }
  const source = options.literals;
  const set = compiledSets.get(
    source instanceof CompiledLiterals ? source : compileLiterals(source, options),
  );
  if (set === undefined) throw new TypeError("literals must come from compileLiterals");

  const resolve = optionalFunction(options.resolve, "resolve") ?? set.values;
  if (resolve === undefined) throw new TypeError("resolve is required when literals is an array");
  return {
    set,
    resolve,
    ...compileShared(options),
    onResolveError: optionalFunction(options.onResolveError, "onResolveError"),
    onDone: optionalFunction(options.onDone, "onDone"),
  };
}

/** Stands in for the user resolver when a table answers every match. */
const unreachable: LiteralResolver = () => null;

/** @internal Leftmost-longest literal scanner. Held bytes are bounded by the longest literal. */
export class LiteralSubstituter extends Lookahead {
  /** Replacements by index when they come from the literal set. */
  readonly table: Uint8Array[] | undefined;
  private readonly user: LiteralResolver;
  private readonly borrow: boolean;
  private readonly onDone: ((stats: LiteralStats) => void) | undefined;
  private readonly literals: readonly Uint8Array[];
  private readonly ac: AhoCorasick;
  private chunk: Uint8Array = EMPTY;
  private bridgeTake = 0;
  private flushTail = false;
  private index: IndexedLiterals | undefined;
  private replayed = 0;
  private pendingCommit = false;
  /** The current step stopped on a halt. */
  private stalled = false;

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

  constructor(options: LiteralTransformOptions, onClose: (failure?: { reason: unknown }) => void) {
    const compiled = compileLiteralOptions(options);
    super(compiled, onClose);
    this.literals = compiled.set.literals;
    this.ac = compiled.set.ac;
    const resolve = compiled.resolve;
    this.table = Array.isArray(resolve) ? resolve : undefined;
    this.user = Array.isArray(resolve) ? unreachable : resolve;
    this.borrow = compiled.borrow;
    this.onDone = compiled.onDone;
  }

  blocked(): boolean {
    return this.halted();
  }

  send(part: Uint8Array): void {
    this.emit(part);
  }

  /** A table replacement, never pending. */
  replace(value: Uint8Array): void {
    this.emit(value);
    this.replaced++;
  }

  /** Resolve a match through the user resolver. `bytes` is a copy, or a borrowed view if `view`. */
  resolveMatch(index: number, bytes: Uint8Array, view: boolean): void {
    let value: unknown;
    let promise: Promise<unknown> | undefined;
    let failed = false;
    let error: unknown;
    try {
      value = this.user(bytes, index, this.context);
      if (!(value instanceof Uint8Array)) promise = toPromise(value);
    } catch (thrown) {
      failed = true;
      error = thrown;
    }
    if (value instanceof Uint8Array && !this.stopped) {
      this.replace(view && value === bytes ? value.slice() : value);
    } else if (view) this.accept(value, promise, failed, error, bytes.slice(), bytes);
    else this.accept(value, promise, failed, error, bytes);
  }

  protected begin(chunk: Uint8Array): void {
    this.chunk = chunk;
    if (chunk.length > 0) this.enterChunk(chunk, chunk.length);
  }

  protected beginFlush(): boolean {
    return true;
  }

  protected step(): boolean {
    this.stalled = false;
    if (this.flushing) {
      this.flushHeld();
      return !this.stalled;
    }
    if (this.chunk.length > 0) {
      this.continueConsume();
      if (this.stalled) return false;
    }
    if (this.index !== undefined) {
      this.hold = EMPTY;
      this.holdLen = 0;
    }
    this.src = this.chunk = EMPTY;
    return true;
  }

  protected verbatim(payload: Uint8Array, sink: (part: Uint8Array) => void): void {
    sink(payload);
  }

  protected report(bytesOut: number): void {
    this.onDone?.({
      replaced: this.replaced,
      rejected: this.rejected,
      bytesIn: this.bytesIn,
      bytesOut,
    });
  }

  private flushHeld(): void {
    if (this.index !== undefined) {
      this.scan();
      if (!this.stalled) this.stalled = !this.index.finish();
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
        if (this.stalled) return;
        this.stalled = !index.finish();
        return;
      }
    }
    if (this.flushTail) {
      this.scan();
      if (this.stalled) return;
      // A re-scan behind a decided match can leave a fresh candidate.
      while (this.candStart >= 0) {
        this.commit();
        if (this.halted()) {
          this.stalled = true;
          return;
        }
        this.scan();
        if (this.stalled) return;
      }
      this.emitUpTo(this.srcEnd);
    }
  }

  /** Enter a chunk, bridging a window the previous one stranded. */
  private enterChunk(chunk: Uint8Array, count: number): void {
    if (this.holdLen > 0) {
      const held = this.holdLen;
      const take = this.ac.maxLength < count ? this.ac.maxLength : count;
      this.ensureHold(held + take);
      copyBytes(this.hold, held, chunk, 0, take);
      // The window sits at offset 0, so a pending match's index carries over.
      this.enter(this.hold, held + take, true, held, 0);
      this.bridgeTake = take;
    } else {
      this.enter(chunk, count, false, 0, 0);
    }
  }

  private continueConsume(): void {
    this.scan();
    if (this.stalled) return;
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
      this.emitUpTo(ws);
      this.holdLen = 0;
      if (this.candStart >= 0) this.candStart -= held;
      this.enter(chunk, count, false, take, ws - held);
      this.scan();
      if (this.stalled) return;
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
      if (this.stalled) return;
    }
    if (this.index !== undefined) {
      this.i = this.index.scan(this.src, this.i, this.srcEnd);
      this.flushStart = this.i;
      this.node = 0;
      this.candStart = -1;
      this.candLen = 0;
      this.stalled = this.halted();
      return;
    }
    const {
      delta,
      classOf,
      width,
      outLen,
      outIdx,
      depth,
      firstBytes: mask,
      soleFirstByte: sole,
      maxLength,
    } = this.ac;
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
      if (candStart >= 0 && (candLen === maxLength || i - depth[node] > candStart)) {
        this.i = i;
        this.node = node;
        this.candStart = candStart;
        this.candLen = candLen;
        this.candIdx = candIdx;
        this.commit();
        if (this.stalled) return;
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
        if (this.halted()) {
          this.stalled = true;
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
    const index = new IndexedLiterals(this.literals, this.ac, this);
    this.index = index;
    this.i = from;
    this.scan();
    return index;
  }

  /** Emit up to the pending match, then its replacement. */
  private commit(): void {
    const start = this.candStart;
    const end = start + this.candLen;
    this.emitUpTo(start);
    if (this.halted()) {
      this.pendingCommit = true;
      this.stalled = true;
      return;
    }
    this.pendingCommit = false;
    const table = this.table;
    if (table !== undefined) this.replace(table[this.candIdx]);
    else {
      const src = this.src;
      const view = this.borrow;
      this.resolveMatch(
        this.candIdx,
        view ? src.subarray(start, end) : src.slice(start, end),
        view,
      );
    }
    this.i = end;
    this.flushStart = end;
    this.node = 0;
    this.candStart = -1;
    this.candLen = 0;
  }

  /** Start of the bytes that must outlive the buffer. */
  private windowLen(): number {
    const d = this.ac.depth[this.node];
    if (this.candStart < 0) return d;
    const span = this.srcEnd - this.candStart;
    return span > d ? span : d;
  }

  /** End of buffer: emit what is decided, keep the live window in `hold`. */
  private park(): void {
    const end = this.srcEnd;
    const ws = end - this.windowLen();
    this.emitUpTo(ws);
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

  private emitUpTo(to: number): void {
    if (to > this.flushStart) {
      this.emitSpan(this.src, this.flushStart, to, this.srcOwned);
      this.flushStart = to;
    }
  }

  private ensureHold(need: number): void {
    if (need <= this.hold.length) return;
    const size = Math.min(this.ac.maxLength * 2, Math.max(need, this.hold.length * 2, 64));
    const next = new Uint8Array(size);
    next.set(this.hold.subarray(0, this.holdLen));
    this.hold = next;
  }

  protected clear(): void {
    this.chunk = EMPTY;
    this.bridgeTake = 0;
    this.flushTail = false;
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
  }
}

/** Body for `new TransformStream(...)`. Single use. */
export function createLiteralTransformer(options: LiteralTransformOptions): LiteralTransformer {
  return lookaheadBody(options?.signal, (onClose) => new LiteralSubstituter(options, onClose));
}

/** Backpressured pair for `pipeThrough`. Single use. */
export function createLiteralStream(
  options: LiteralTransformOptions,
): ReadableWritablePair<Uint8Array, Uint8Array> {
  return flowStream(createLiteralTransformer(options), options?.signal);
}
