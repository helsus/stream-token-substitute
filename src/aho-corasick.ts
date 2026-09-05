/** Default ceiling on the transition table. It is states * width cells, so a
 *  large enough set would allocate hundreds of megabytes and take the isolate
 *  with it. Refusing to build it beats dying at request time. Sized for the
 *  smallest runtime that matters: a Workers isolate gets 128 MiB total. */
export const DEFAULT_MAX_TABLE_BYTES = 16 * 1024 * 1024;

/** Aho-Corasick compiled to a full DFA over byte classes: a transition is two
 *  loads and a multiply, O(1) per byte, no failure walks. The whole scan state
 *  is one int, so a partial match survives a chunk boundary. Immutable after
 *  construction. Needles must be non-empty (validated in compileNeedles). */
export class AhoCorasick {
  /** byte -> DFA column. 0 for bytes no needle contains; that column is all
   *  zeros, so unknown bytes land on the root for free. */
  readonly classOf: Uint16Array = new Uint16Array(256);
  /** Columns per state: distinct needle bytes + 1. */
  readonly width: number;
  /** delta[node * width + classOf[byte]] is the next node. */
  readonly delta: Uint16Array | Int32Array;
  /** Longest suffix that can still extend to a needle. */
  readonly depth: Uint16Array | Int32Array;
  /** Longest needle ending at a node, 0 when none. It starts earliest, which
   *  makes leftmost-longest decidable per byte. */
  readonly outLen: Uint16Array | Int32Array;
  /** Index of that needle in the constructor's array, -1 when none. */
  readonly outIdx: Int32Array;
  /** Distinct first bytes, for the outside-a-match skip. */
  readonly firstBytes: Uint8Array;
  /** The shared first byte when there is exactly one, else -1. Lets the scanner
   *  skip with one indexOf instead of a mask test per byte. */
  readonly soleFirstByte: number;
  readonly maxLength: number;
  readonly fail: Uint32Array;

  constructor(needles: readonly Uint8Array[], maxTableBytes: number = DEFAULT_MAX_TABLE_BYTES) {
    // Five cells per node: child, sibling, byte, depth, match index + 1.
    let trie = new Uint32Array(5 * 16);
    let states = 1;
    const classOf = this.classOf;
    const firstBytes = new Uint8Array(256);
    let distinctFirst = 0;
    let soleFirstByte = -1;
    let maxLength = 0;
    let width = 1;

    for (let p = 0; p < needles.length; p++) {
      const needle = needles[p];
      if (needle.length > maxLength) maxLength = needle.length;
      if (firstBytes[needle[0]] === 0) {
        firstBytes[needle[0]] = 1;
        distinctFirst++;
        soleFirstByte = needle[0];
      }
      let node = 0;
      for (let i = 0; i < needle.length; i++) {
        const byte = needle[i];
        if (classOf[byte] === 0) classOf[byte] = width++;
        let child = trie[node * 5];
        while (child !== 0 && trie[child * 5 + 2] !== byte) child = trie[child * 5 + 1];
        if (child === 0) {
          child = states;
          const count = states + 1;
          const tableBytes = count * width * (count <= 65536 ? 2 : 4);
          if (tableBytes > maxTableBytes) {
            throw new RangeError(
              `needle set too large: ${count} states x ${width} byte classes needs ` +
                `${tableBytes} bytes of transition table, over the ` +
                `${maxTableBytes} byte maxTableBytes limit`,
            );
          }
          if (count * 5 > trie.length) {
            const grown = new Uint32Array(trie.length * 2);
            grown.set(trie);
            trie = grown;
          }
          states = count;
          trie[child * 5 + 1] = trie[node * 5];
          trie[child * 5 + 2] = byte;
          trie[child * 5 + 3] = trie[node * 5 + 3] + 1;
          trie[node * 5] = child;
        }
        node = child;
      }
      // A duplicate needle keeps the first entry.
      if (trie[node * 5 + 4] === 0) trie[node * 5 + 4] = p + 1;
    }

    // BFS builds failure links and DFA rows together.
    const fail = new Uint32Array(states);
    this.fail = fail;
    const queue = new Uint32Array(states - 1);
    const depth = maxLength < 65536 ? new Uint16Array(states) : new Int32Array(states);
    const outLen = maxLength < 65536 ? new Uint16Array(states) : new Int32Array(states);
    const outIdx = new Int32Array(states).fill(-1);
    const cells = states * width;
    const cellBytes = states <= 65536 ? 2 : 4;
    const delta = cellBytes === 2 ? new Uint16Array(cells) : new Int32Array(cells);
    let tail = 0;
    for (let child = trie[0]; child !== 0; child = trie[child * 5 + 1]) {
      delta[classOf[trie[child * 5 + 2]]] = child;
      queue[tail++] = child;
    }
    for (let head = 0; head < tail; head++) {
      const s = queue[head];
      const row = s * width;
      const failRow = fail[s] * width;
      delta.copyWithin(row, failRow, failRow + width);
      const match = trie[s * 5 + 4];
      outLen[s] = match === 0 ? outLen[fail[s]] : trie[s * 5 + 3];
      outIdx[s] = match === 0 ? outIdx[fail[s]] : match - 1;
      depth[s] = trie[s * 5] === 0 ? depth[fail[s]] : trie[s * 5 + 3];
      for (let child = trie[s * 5]; child !== 0; child = trie[child * 5 + 1]) {
        const c = classOf[trie[child * 5 + 2]];
        fail[child] = delta[failRow + c];
        delta[row + c] = child;
        queue[tail++] = child;
      }
    }

    this.width = width;
    this.delta = delta;
    this.depth = depth;
    this.outLen = outLen;
    this.outIdx = outIdx;
    this.firstBytes = firstBytes;
    this.soleFirstByte = distinctFirst === 1 ? soleFirstByte : -1;
    this.maxLength = maxLength;
  }
}
