import { compileNeedleOptions, type NeedleTransformOptions } from "../src/needles.ts";

function matchesAt(input: Uint8Array, at: number, pat: Uint8Array): boolean {
  if (at + pat.length > input.length) return false;
  for (let i = 0; i < pat.length; i++) {
    if (input[at + i] !== pat[i]) return false;
  }
  return true;
}

/** Non-streaming equivalent and normative reference for the needle transformer.
 *  Leftmost-longest, no re-scan of substituted bytes.
 *  Deliberately naive. Do not optimize. */
export function substituteNeedles(input: Uint8Array, options: NeedleTransformOptions): Uint8Array {
  const { set, resolve } = compileNeedleOptions(options);
  const needles = set.needles;
  const out: Uint8Array[] = [];
  let contentStart = 0;
  let i = 0;

  while (i < input.length) {
    let best = -1;
    let bestLen = 0;
    for (let n = 0; n < needles.length; n++) {
      const needle = needles[n];
      if (needle.length > bestLen && matchesAt(input, i, needle)) {
        best = n;
        bestLen = needle.length;
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
