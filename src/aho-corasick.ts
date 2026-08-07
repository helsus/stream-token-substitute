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
  /** Held-byte count per node. Half-width where it fits: the scan loop reads
   *  this and outLen on every byte. */
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

  constructor(needles: readonly Uint8Array[], maxTableBytes: number = DEFAULT_MAX_TABLE_BYTES) {
    // Trie. The per-node Maps are build-time only; the DFA replaces them.
    const next: Map<number, number>[] = [new Map()];
    const depth = [0];
    const outLen = [0];
    const outIdx = [-1];
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
        let child = next[node].get(byte);
        if (child === undefined) {
          child = next.length;
          next.push(new Map());
          depth.push(depth[node] + 1);
          outLen.push(0);
          outIdx.push(-1);
          next[node].set(byte, child);
        }
        node = child;
      }
      // A duplicate needle keeps the first entry.
      if (outIdx[node] === -1) {
        outLen[node] = needle.length;
        outIdx[node] = p;
      }
    }

    // Failure links, BFS. Children inherit the longest suffix needle, which
    // collapses the output-link walk.
    const states = next.length;
    const fail = new Int32Array(states);
    const queue: number[] = [];
    for (const child of next[0].values()) queue.push(child);
    for (let head = 0; head < queue.length; head++) {
      const node = queue[head];
      for (const [byte, child] of next[node]) {
        let f = fail[node];
        for (;;) {
          const t = next[f].get(byte);
          if (t !== undefined) {
            fail[child] = t;
            break;
          }
          if (f === 0) break;
          f = fail[f];
        }
        if (outIdx[child] === -1) {
          const link = fail[child];
          outLen[child] = outLen[link];
          outIdx[child] = outIdx[link];
        }
        queue.push(child);
      }
    }

    // DFA: resolve every (state, class) once. BFS order guarantees the fail
    // row is complete before any row that reads it.
    const byteOfClass = new Uint8Array(width);
    for (let b = 0; b < 256; b++) {
      if (classOf[b] !== 0) byteOfClass[classOf[b]] = b;
    }
    const cells = states * width;
    const cellBytes = states <= 65536 ? 2 : 4;
    if (cells * cellBytes > maxTableBytes) {
      throw new RangeError(
        `needle set too large: ${states} states x ${width} byte classes needs ` +
          `${cells * cellBytes} bytes of transition table, over the ` +
          `${maxTableBytes} byte maxTableBytes limit`,
      );
    }
    const delta = cellBytes === 2 ? new Uint16Array(cells) : new Int32Array(cells);
    for (let c = 1; c < width; c++) {
      delta[c] = next[0].get(byteOfClass[c]) ?? 0;
    }
    for (let head = 0; head < queue.length; head++) {
      const s = queue[head];
      const row = s * width;
      const failRow = fail[s] * width;
      for (let c = 1; c < width; c++) {
        delta[row + c] = next[s].get(byteOfClass[c]) ?? delta[failRow + c];
      }
    }

    this.width = width;
    this.delta = delta;
    this.depth = maxLength < 65536 ? Uint16Array.from(depth) : Int32Array.from(depth);
    this.outLen = maxLength < 65536 ? Uint16Array.from(outLen) : Int32Array.from(outLen);
    this.outIdx = Int32Array.from(outIdx);
    this.firstBytes = firstBytes;
    this.soleFirstByte = distinctFirst === 1 ? soleFirstByte : -1;
    this.maxLength = maxLength;
  }
}
