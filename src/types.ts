import { encodeText } from "./bytes.ts";

/** Resolve a token payload to substitution bytes.
 *  Bytes: enqueued by reference, do not mutate afterwards.
 *  Empty: token recognized, substitutes to nothing.
 *  null: rejected, raw open+payload+close is emitted verbatim.
 *  `payload` is a scratch view, valid only during the call. */
export type TokenResolver = (payload: Uint8Array) => Uint8Array | null;

/** Async form. May return a value directly; only a thenable suspends the scanner.
 *  `payload` is a fresh copy, so it stays valid across the await. */
export type AsyncTokenResolver = (
  payload: Uint8Array,
) => Uint8Array | null | PromiseLike<Uint8Array | null>;

/** Incremental validator, called once per byte committed to the payload.
 *  Return false to abort the token.
 *  Bytes matching a prefix of `close` are withheld until the match falls through. */
export type PayloadValidator = (payload: Uint8Array, next: number) => boolean;

/** Called when `resolve` throws. Return bytes to substitute, or null to emit the
 *  raw open+payload+close verbatim. To rethrow, throw from the handler.
 *  `payload` is a copy and is safe to retain. */
export type ResolveErrorHandler = (error: unknown, payload: Uint8Array) => Uint8Array | null;

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

/** Structurally a DOM `Transformer<Uint8Array, Uint8Array>`, declared here
 *  because @types/node exposes `TransformStream` globally but not `Transformer`,
 *  so naming the DOM type would break consumers without the DOM lib. */
export interface TokenTransformer {
  transform(chunk: Uint8Array, controller: TransformStreamDefaultController<Uint8Array>): void;
  flush(controller: TransformStreamDefaultController<Uint8Array>): void;
  /** Release a single-use transformer without flushing. */
  cancel?(reason?: unknown): void;
}

/** Async counterpart. `transform` settles once the chunk is fully consumed. */
export interface AsyncTokenTransformer {
  transform(
    chunk: Uint8Array,
    controller: TransformStreamDefaultController<Uint8Array>,
  ): Promise<void>;
  flush(controller: TransformStreamDefaultController<Uint8Array>): void;
  /** Stop pending resolution and release scanner state. Does not cancel external I/O. */
  cancel?(reason?: unknown): void;
}

/** Options common to the sync and async transformers. */
export interface TokenTransformOptionsBase {
  /** Opening delimiter. Strings are UTF-8 encoded once. Must be non-empty. */
  open: string | Uint8Array;
  /** Closing delimiter. Defaults to `open`. Must be non-empty. */
  close?: string | Uint8Array;
  validate?: PayloadValidator;
  /** Cap on committed payload length. Exceeding it aborts the token. Default 64.  */
  maxPayloadBytes?: number;
  /** Output accumulator high-water mark. Small pieces are merged once they
   *  reach this many bytes, trading zero-copy spans for far fewer parts. A piece
   *  of 1 kB or more is never merged. `0` disables buffering. Default 16384. */
  flushBytes?: number;
  /** Called when `resolve` throws. Absent: the error propagates and errors the
   *  stream. A throwing `validate` always propagates. */
  onResolveError?: ResolveErrorHandler;
  /** Called once from `flush` with the stream's counts. */
  onDone?: (stats: TokenStats) => void;
}

export interface TokenTransformOptions extends TokenTransformOptionsBase {
  resolve: TokenResolver;
}

export interface AsyncTokenTransformOptions extends TokenTransformOptionsBase {
  resolve: AsyncTokenResolver;
  /** Cancels pending resolution, including in runtimes without Transformer.cancel. */
  signal?: AbortSignal;
}

/** Normalized options, shared by reference and transformer. */
export interface CompiledOptions {
  openBytes: Uint8Array;
  closeBytes: Uint8Array;
  resolve: AsyncTokenResolver;
  validate: PayloadValidator | undefined;
  maxPayloadBytes: number;
  flushBytes: number;
  onResolveError: ResolveErrorHandler | undefined;
  onDone: ((stats: TokenStats) => void) | undefined;
}

const DEFAULT_MAX_PAYLOAD_BYTES = 64;
const DEFAULT_FLUSH_BYTES = 16384;

function encodeDelimiter(value: string | Uint8Array, name: string): Uint8Array {
  const bytes = encodeText(value, name);
  if (bytes.length === 0) throw new TypeError(`${name} must be non-empty`);
  return bytes;
}

/** isSafeInteger is already false for non-numbers, so no typeof guard is needed. */
const isByteCount = (value: number): boolean => Number.isSafeInteger(value) && value >= 0;

function optionalFunction<T>(value: T | undefined, name: string): T | undefined {
  if (value !== undefined && typeof value !== "function") {
    throw new TypeError(`${name} must be a function`);
  }
  return value;
}

export function compileOptions(
  options: TokenTransformOptions | AsyncTokenTransformOptions,
): CompiledOptions {
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

  return {
    openBytes,
    closeBytes,
    resolve: options.resolve,
    validate,
    maxPayloadBytes,
    flushBytes,
    onResolveError,
    onDone,
  };
}
