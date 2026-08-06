/** KMP delimiter matcher. Whole state is `k`, the matched prefix length, so a
 *  partial match survives chunk boundaries. Held bytes are always
 *  `delim.subarray(0, k)`, never a view into an input chunk. */

export const ADVANCED = 0;
export const COMPLETE = 1;
export const REJECTED = 2;

export type MatchResult = typeof ADVANCED | typeof COMPLETE | typeof REJECTED;

/** fail[i] = longest proper prefix of pat[0..i] that is also a suffix of it. */
export function buildFailureTable(pat: Uint8Array): Uint8Array | Uint32Array {
  const n = pat.length;
  const fail = n < 256 ? new Uint8Array(n) : new Uint32Array(n);
  let k = 0;
  for (let i = 1; i < n; i++) {
    const byte = pat[i];
    while (k > 0 && pat[k] !== byte) k = fail[k - 1];
    if (pat[k] === byte) k++;
    fail[i] = k;
  }
  return fail;
}

export class DelimiterMatcher {
  readonly pat: Uint8Array;
  private readonly fail: Uint8Array | Uint32Array;
  private readonly firstByte: number;
  k = 0;
  /** Held bytes released by the last feed(). */
  released = 0;

  constructor(pat: Uint8Array) {
    if (pat.length === 0) throw new TypeError("delimiter must be non-empty");
    this.pat = pat;
    this.fail = buildFailureTable(pat);
    this.firstByte = pat[0];
  }

  reset(): void {
    this.k = 0;
    this.released = 0;
  }

  /** COMPLETE: full match, k reset to 0.
   *  ADVANCED: match extended, possibly after fallback; `released` bytes dropped.
   *  REJECTED: no match; all held bytes released and `byte` is the caller's to handle. */
  feed(byte: number): MatchResult {
    const pat = this.pat;
    if (pat.length === 1) {
      this.released = 0;
      return byte === this.firstByte ? COMPLETE : REJECTED;
    }

    const prevK = this.k;
    let k = prevK;
    const fail = this.fail;
    while (k > 0 && pat[k] !== byte) k = fail[k - 1];

    if (pat[k] === byte) {
      k++;
      if (k === pat.length) {
        this.k = 0;
        this.released = 0;
        return COMPLETE;
      }
      this.k = k;
      this.released = prevK + 1 - k;
      return ADVANCED;
    }

    this.k = 0;
    this.released = prevK;
    return REJECTED;
  }
}
