import { compileLiteralOptions, type LiteralTransformOptions } from "../src/literals.ts";

function matchesAt(input: Uint8Array, at: number, pat: Uint8Array): boolean {
  if (at + pat.length > input.length) return false;
  for (let i = 0; i < pat.length; i++) {
    if (input[at + i] !== pat[i]) return false;
  }
  return true;
}

/** Non-streaming equivalent and normative reference for the literal transformer.
 *  Leftmost-longest, no re-scan of substituted bytes.
 *  Deliberately naive. Do not optimize. */
export function substituteLiterals(
  input: Uint8Array,
  options: LiteralTransformOptions,
): Uint8Array {
  const { set, resolve } = compileLiteralOptions(options);
  const literals = set.literals;
  const out: Uint8Array[] = [];
  let contentStart = 0;
  let i = 0;

  while (i < input.length) {
    let best = -1;
    let bestLen = 0;
    for (let n = 0; n < literals.length; n++) {
      const literal = literals[n];
      if (literal.length > bestLen && matchesAt(input, i, literal)) {
        best = n;
        bestLen = literal.length;
      }
    }
    if (best < 0) {
      i++;
      continue;
    }
    if (i > contentStart) out.push(input.subarray(contentStart, i));
    const value = resolve(input.subarray(i, i + bestLen), best);
    // Null is atomic: verbatim, and the span is not re-scanned.
    out.push(value === null ? input.subarray(i, i + bestLen) : value);
    i += bestLen;
    contentStart = i;
  }
  if (input.length > contentStart) out.push(input.subarray(contentStart));

  let total = 0;
  for (const part of out) total += part.length;
  const merged = new Uint8Array(total);
  let w = 0;
  for (const part of out) {
    merged.set(part, w);
    w += part.length;
  }
  return merged;
}
