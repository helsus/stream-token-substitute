import { compileOptions, type TokenResolver, type TokenTransformOptions } from "../src/types.ts";

function matchesAt(input: Uint8Array, at: number, pat: Uint8Array): boolean {
  if (at + pat.length > input.length) return false;
  for (let i = 0; i < pat.length; i++) {
    if (input[at + i] !== pat[i]) return false;
  }
  return true;
}

/** input[at..end] is a non-empty proper prefix of pat: what the streaming close
 *  matcher would still hold when the stream ends. */
function isTruncatedPrefix(input: Uint8Array, at: number, pat: Uint8Array): boolean {
  const rest = input.length - at;
  if (rest <= 0 || rest >= pat.length) return false;
  for (let i = 0; i < rest; i++) {
    if (input[at + i] !== pat[i]) return false;
  }
  return true;
}

/** Non-streaming equivalent and normative reference for the transformer.
 *  Deliberately naive. Do not optimize. */
export function substituteBytes(input: Uint8Array, options: TokenTransformOptions): Uint8Array {
  const { openBytes, closeBytes, validate, maxPayloadBytes } = compileOptions(options);
  // compileOptions widens `resolve` to the async signature for the shared path.
  // The reference is sync by construction and is only ever given a sync one.
  const resolve = options.resolve as TokenResolver;

  if (!(input instanceof Uint8Array)) throw new TypeError("input must be a Uint8Array");

  const out: Uint8Array[] = [];
  const payload: number[] = [];

  let i = 0;
  let contentStart = 0;

  const flushContent = (upto: number): void => {
    if (upto > contentStart) out.push(input.subarray(contentStart, upto));
    contentStart = upto;
  };

  while (i < input.length) {
    if (!matchesAt(input, i, openBytes)) {
      i++;
      continue;
    }

    const tokenStart = i;
    let j = i + openBytes.length;
    payload.length = 0;

    for (;;) {
      if (j >= input.length) {
        // Unterminated: passes through verbatim.
        flushContent(input.length);
        i = input.length;
        break;
      }
      if (matchesAt(input, j, closeBytes)) {
        const end = j + closeBytes.length;
        flushContent(tokenStart);
        const value = resolve(Uint8Array.from(payload));
        if (value === null) {
          // Null is atomic: verbatim, resume after close, no re-scan.
          out.push(input.subarray(tokenStart, end));
        } else if (value.length > 0) {
          out.push(value);
        }
        i = end;
        contentStart = end;
        break;
      }
      if (isTruncatedPrefix(input, j, closeBytes)) {
        // Held as a close candidate at EOF: never committed.
        flushContent(input.length);
        i = input.length;
        break;
      }

      // Commit: validate, count against the cap, append.
      const byte = input[j];
      const rejected =
        (validate !== undefined && !validate(Uint8Array.from(payload), byte)) ||
        payload.length + 1 > maxPayloadBytes;
      if (rejected) {
        // Emit open verbatim, re-scan from the first payload byte.
        flushContent(tokenStart);
        out.push(openBytes);
        i = tokenStart + openBytes.length;
        contentStart = i;
        break;
      }
      payload.push(byte);
      j++;
    }
  }

  flushContent(input.length);

  let total = 0;
  for (const part of out) total += part.length;
  const result = new Uint8Array(total);
  let w = 0;
  for (const part of out) {
    result.set(part, w);
    w += part.length;
  }
  return result;
}
