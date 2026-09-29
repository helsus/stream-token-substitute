/** Estimated build bytes: copies, trie, per-state arrays and the DFA table. */
function estimate(
  reserved: number,
  trieCells: number,
  states: number,
  width: number,
  maxLength: number,
): number {
  // fail, queue, outIdx, own, dict: 4 bytes each. depth, outLen: 2 or 4.
  const perState = 20 + (maxLength < 65536 ? 4 : 8);
  return reserved + trieCells * 4 + states * perState + states * width * (states <= 65536 ? 2 : 4);
}

function tooLarge(bytes: number, limit: number, states: number, width: number): RangeError {
  return new RangeError(
    `literal set too large: ${states} states x ${width} byte classes needs about ` +
      `${bytes} bytes to compile, over the ${limit} byte maxMemoryBytes limit`,
  );
}

/** Aho-Corasick as a full DFA over byte classes. Scan state is one int. */
export class AhoCorasick {
  /** byte -> DFA column. Column 0 is all zeros, for bytes no literal contains. */
  readonly classOf: Uint16Array = new Uint16Array(256);
  /** Columns per state: distinct literal bytes + 1. */
  readonly width: number;
  /** delta[node * width + classOf[byte]] is the next node. */
  readonly delta: Uint16Array | Int32Array;
  /** Longest suffix that can still extend to a literal. */
  readonly depth: Uint16Array | Int32Array;
  /** Longest literal ending at a node, 0 when none. */
  readonly outLen: Uint16Array | Int32Array;
  /** Index of that literal in the constructor's array, -1 when none. */
  readonly outIdx: Int32Array;
  /** Distinct first bytes, for the outside-a-match skip. */
  readonly firstBytes: Uint8Array;
  /** The only first byte, or -1 when there are several. */
  readonly soleFirstByte: number;
  readonly maxLength: number;
  readonly fail: Uint32Array;
  /** Index of the literal ending exactly at a node, -1 when none. */
  readonly own: Int32Array;
  /** Nearest proper suffix node with its own literal, 0 when none. */
  readonly dict: Int32Array;

  /** `reserved` bytes already held by the caller count against the limit. */
  constructor(literals: readonly Uint8Array[], maxMemoryBytes: number, reserved = 0) {
    // Five cells per node: child, sibling, byte, depth, match index + 1.
    let trie = new Uint32Array(5 * 16);
    let states = 1;
    const classOf = this.classOf;
    const firstBytes = new Uint8Array(256);
    let distinctFirst = 0;
    let soleFirstByte = -1;
    let maxLength = 0;
    let width = 1;

    for (let p = 0; p < literals.length; p++) {
      const literal = literals[p];
      if (literal.length > maxLength) maxLength = literal.length;
      if (firstBytes[literal[0]] === 0) {
        firstBytes[literal[0]] = 1;
        distinctFirst++;
        soleFirstByte = literal[0];
      }
      let node = 0;
      for (let i = 0; i < literal.length; i++) {
        const byte = literal[i];
        if (classOf[byte] === 0) classOf[byte] = width++;
        let child = trie[node * 5];
        while (child !== 0 && trie[child * 5 + 2] !== byte) child = trie[child * 5 + 1];
        if (child === 0) {
          child = states;
          const count = states + 1;
          const cells = count * 5 > trie.length ? trie.length * 3 : trie.length;
          const bytes = estimate(reserved, cells, count, width, maxLength);
          if (bytes > maxMemoryBytes) throw tooLarge(bytes, maxMemoryBytes, count, width);
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
      // A duplicate literal keeps the first entry.
      if (trie[node * 5 + 4] === 0) trie[node * 5 + 4] = p + 1;
    }

    const bytes = estimate(reserved, trie.length, states, width, maxLength);
    if (bytes > maxMemoryBytes) throw tooLarge(bytes, maxMemoryBytes, states, width);

    // BFS builds failure links, dictionary links and DFA rows together.
    const fail = new Uint32Array(states);
    this.fail = fail;
    const queue = new Uint32Array(states - 1);
    const depth = maxLength < 65536 ? new Uint16Array(states) : new Int32Array(states);
    const outLen = maxLength < 65536 ? new Uint16Array(states) : new Int32Array(states);
    const outIdx = new Int32Array(states).fill(-1);
    const own = new Int32Array(states).fill(-1);
    const dict = new Int32Array(states);
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
      own[s] = match - 1;
      const f = fail[s];
      dict[s] = own[f] >= 0 ? f : dict[f];
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
    this.own = own;
    this.dict = dict;
    this.depth = depth;
    this.outLen = outLen;
    this.outIdx = outIdx;
    this.firstBytes = firstBytes;
    this.soleFirstByte = distinctFirst === 1 ? soleFirstByte : -1;
    this.maxLength = maxLength;
  }
}
