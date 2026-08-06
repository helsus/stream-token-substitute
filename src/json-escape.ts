/** Escape bytes for a JSON string literal embedded in HTML.
 *  Byte-level: only the listed bytes and the two 3-byte sequences are rewritten.
 *  Invalid UTF-8 passes through untouched. Not a validator, not a sanitizer.
 *  If nothing needs escaping, `src` is returned as-is: treat both as immutable. */

const HEX = "0123456789abcdef";

const BACKSLASH = 0x5c;
const QUOTE = 0x22;
const LT = 0x3c;
const GT = 0x3e;
const AMP = 0x26;
const LOWER_U = 0x75;

function needsEscape(byte: number): boolean {
  return (
    byte < 0x20 ||
    byte === QUOTE ||
    byte === BACKSLASH ||
    byte === LT ||
    byte === GT ||
    byte === AMP
  );
}

/** LOWER_U+2028 / LOWER_U+2029: legal in JSON, illegal in pre-ES2019 JS string literals. */
function isLineSep(src: Uint8Array, i: number): boolean {
  return src[i] === 0xe2 && src[i + 1] === 0x80 && (src[i + 2] === 0xa8 || src[i + 2] === 0xa9);
}

function shortEscape(byte: number): number {
  switch (byte) {
    case 0x08:
      return 0x62; // b
    case 0x09:
      return 0x74; // t
    case 0x0a:
      return 0x6e; // n
    case 0x0c:
      return 0x66; // f
    case 0x0d:
      return 0x72; // r
    default:
      return 0;
  }
}

export function jsonEscapeBytes(src: Uint8Array): Uint8Array {
  if (!(src instanceof Uint8Array)) throw new TypeError("src must be a Uint8Array");

  // First pass: the first byte needing work, and the exact output length. Sizing
  // the buffer up front costs one extra scan of a short value and saves the 6x
  // worst-case allocation, which is what an edge memory budget actually notices.
  const len = src.length;
  let start = -1;
  let total = len;
  for (let i = 0; i < len; i++) {
    const byte = src[i];
    if (byte === 0xe2) {
      if (!isLineSep(src, i)) continue;
      if (start < 0) start = i;
      total += 3; // 3 bytes to
      i += 2;
      continue;
    }
    if (!needsEscape(byte)) continue;
    if (start < 0) start = i;
    // \" \\ and the five short forms are 2 bytes; everything else is \u00XX.
    total += byte === QUOTE || byte === BACKSLASH || shortEscape(byte) !== 0 ? 1 : 5;
  }
  if (start === -1) return src;

  const out = new Uint8Array(total);
  out.set(src.subarray(0, start), 0);
  let w = start;

  for (let i = start; i < len; i++) {
    const byte = src[i];

    if (byte === 0xe2 && isLineSep(src, i)) {
      out[w++] = BACKSLASH;
      out[w++] = LOWER_U;
      out[w++] = 0x32; // 2
      out[w++] = 0x30; // 0
      out[w++] = 0x32; // 2
      out[w++] = src[i + 2] === 0xa8 ? 0x38 : 0x39; // 8 or 9
      i += 2;
      continue;
    }

    if (!needsEscape(byte)) {
      out[w++] = byte;
      continue;
    }

    if (byte === QUOTE || byte === BACKSLASH) {
      out[w++] = BACKSLASH;
      out[w++] = byte;
      continue;
    }

    const short = shortEscape(byte);
    if (short !== 0) {
      out[w++] = BACKSLASH;
      out[w++] = short;
      continue;
    }

    // \u00XX: remaining controls, plus < > & as defense in depth.
    out[w++] = BACKSLASH;
    out[w++] = LOWER_U;
    out[w++] = 0x30;
    out[w++] = 0x30;
    out[w++] = HEX.charCodeAt(byte >> 4);
    out[w++] = HEX.charCodeAt(byte & 0x0f);
  }

  return out;
}
