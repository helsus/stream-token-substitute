import { encodeText } from "./bytes.ts";

/** Substitution for one token.
 *  Bytes: enqueued by reference, do not mutate afterwards.
 *  String: UTF-8 encoded.
 *  null: rejected, raw open+payload+close is emitted verbatim.
 *  Stream or async iterable: read in order once earlier output is drained. */
export type Replacement =
  | Uint8Array
  | string
  | null
  | ReadableStream<Uint8Array>
  | AsyncIterable<Uint8Array>;

/** Resolve a token payload. `payload` is a fresh copy, safe to retain.
 *  A thenable or stream result does not stall scanning, see `concurrency`. */
export type TokenResolver = (payload: Uint8Array) => Replacement | PromiseLike<Replacement>;

/** Incremental validator, called once per byte committed to the payload.
 *  Return false to abort the token.
 *  Bytes matching a prefix of `close` are withheld until the match falls through. */
export type PayloadValidator = (payload: Uint8Array, next: number) => boolean;

/** Called when `resolve` throws or rejects. Returns a replacement, null emits the
 *  token verbatim. To rethrow, throw from the handler. `payload` is a copy. */
export type ResolveErrorHandler = (
  error: unknown,
  payload: Uint8Array,
) => Replacement | PromiseLike<Replacement>;

/** Counts for one stream, delivered once from `flush`. */
export interface TokenStats {
  /** Complete tokens whose resolver returned bytes (including empty). */
  resolved: number;
  /** Complete tokens whose resolver returned null, emitted verbatim. */
  rejected: number;
  /** Tokens abandoned by the validator or the payload cap. */
  aborted: number;
  /** Bytes written in. */
  bytesIn: number;
  /** Bytes enqueued out. */
  bytesOut: number;
}

/** Structurally a DOM `Transformer<Uint8Array, Uint8Array>`. */
export interface TokenTransformer {
  transform(
    chunk: Uint8Array,
    controller: TransformStreamDefaultController<Uint8Array>,
  ): void | Promise<void>;
  flush(controller: TransformStreamDefaultController<Uint8Array>): void | Promise<void>;
  /** Stop pending resolution and release scanner state. Does not cancel external I/O. */
  cancel?(reason?: unknown): void;
}

export interface TokenTransformOptions {
  resolve: TokenResolver;
  /** Opening delimiter. Strings are UTF-8 encoded once. Must be non-empty. */
  open: string | Uint8Array;
  /** Closing delimiter. Defaults to `open`. Must be non-empty. */
  close?: string | Uint8Array;
  validate?: PayloadValidator;
  /** Cap on committed payload length. Exceeding it aborts the token. Default 64.  */
  maxPayloadBytes?: number;
  /** Output merge threshold in bytes. `0` disables merging. Default 16384. */
  flushBytes?: number;
  /** Pending lookups (thenables and unread streams) scanned ahead of output. Default 4. */
  concurrency?: number;
  /** Called when `resolve` throws or rejects. Absent: the error propagates and
   *  errors the stream. A throwing `validate` always propagates. */
  onResolveError?: ResolveErrorHandler;
  /** Cancels pending resolution, including in runtimes without Transformer.cancel. */
  signal?: AbortSignal;
  /** Called once from `flush` with the stream's counts. */
  onDone?: (stats: TokenStats) => void;
}

/** Normalized options, shared by reference and transformer. */
export interface CompiledOptions {
  openBytes: Uint8Array;
  closeBytes: Uint8Array;
  resolve: TokenResolver;
  /** resolve does not retain its argument, so it may see the scratch. */
  borrows: boolean;
  concurrency: number;
  validate: PayloadValidator | undefined;
  maxPayloadBytes: number;
  flushBytes: number;
  onResolveError: ResolveErrorHandler | undefined;
  onDone: ((stats: TokenStats) => void) | undefined;
}

const DEFAULT_MAX_PAYLOAD_BYTES = 64;
const DEFAULT_FLUSH_BYTES = 16384;
const DEFAULT_CONCURRENCY = 4;

/** Internal: resolvers that never retain their argument. */
export const BORROWING = new WeakSet<object>();

function encodeDelimiter(value: string | Uint8Array, name: string): Uint8Array {
  const bytes = encodeText(value, name);
  if (bytes.length === 0) throw new TypeError(`${name} must be non-empty`);
  return bytes;
}

export const isByteCount = (value: number): boolean => Number.isSafeInteger(value) && value >= 0;

export function optionalFunction<T>(value: T | undefined, name: string): T | undefined {
  if (value !== undefined && typeof value !== "function") {
    throw new TypeError(`${name} must be a function`);
  }
  return value;
}

export function compileOptions(options: TokenTransformOptions): CompiledOptions {
  if (options == null || typeof options !== "object") {
    throw new TypeError("options must be an object");
  }
  const openBytes = encodeDelimiter(options.open, "open");
  const closeBytes =
    options.close === undefined ? openBytes : encodeDelimiter(options.close, "close");

  if (typeof options.resolve !== "function") throw new TypeError("resolve must be a function");
  const validate = optionalFunction(options.validate, "validate");
  const onResolveError = optionalFunction(options.onResolveError, "onResolveError");
  const onDone = optionalFunction(options.onDone, "onDone");

  const maxPayloadBytes = options.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;
  if (!isByteCount(maxPayloadBytes)) {
    throw new RangeError("maxPayloadBytes must be a non-negative safe integer");
  }
  const flushBytes = options.flushBytes ?? DEFAULT_FLUSH_BYTES;
  if (!isByteCount(flushBytes)) {
    throw new RangeError("flushBytes must be a non-negative safe integer");
  }
  const concurrency = options.concurrency ?? DEFAULT_CONCURRENCY;
  if (!Number.isSafeInteger(concurrency) || concurrency < 1) {
    throw new RangeError("concurrency must be a positive safe integer");
  }

  return {
    openBytes,
    closeBytes,
    resolve: options.resolve,
    borrows: BORROWING.has(options.resolve),
    concurrency,
    validate,
    maxPayloadBytes,
    flushBytes,
    onResolveError,
    onDone,
  };
}
