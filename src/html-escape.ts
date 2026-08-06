/** Escape bytes for an HTML text node or attribute value.
 *  Byte-level: only the listed bytes are rewritten, everything else passes
 *  through, invalid UTF-8 included. Not a validator, not a sanitizer, and no
 *  substitute for a context-aware encoder in script or style contexts.
 *  If nothing needs escaping, `src` is returned as-is: treat both as immutable. */

/** No TextEncoder: these tables are built at import time, and Akamai
 *  EdgeWorkers has no global one. Replacements are ASCII by construction. */
function ascii(text: string): Uint8Array {
  const out = new Uint8Array(text.length);
  for (let i = 0; i < text.length; i++) out[i] = text.charCodeAt(i);
  return out;
}

type EscapeTable = (Uint8Array | undefined)[];

function table(pairs: [string, string][]): EscapeTable {
  const t: EscapeTable = new Array(256).fill(undefined);
  for (const [char, replacement] of pairs) t[char.charCodeAt(0)] = ascii(replacement);
  return t;
}

/** Safe in a text node and inside a quoted attribute value. */
const TEXT_PAIRS: [string, string][] = [
  ["&", "&amp;"],
  ["<", "&lt;"],
  [">", "&gt;"],
  ['"', "&quot;"],
  ["'", "&#x27;"],
];

const TEXT_TABLE = table(TEXT_PAIRS);

/** Adds the bytes that can terminate an unquoted attribute value or start a new
 *  one, so a value is inert even where the template forgot the quotes. */
const ATTR_TABLE = table([
  ...TEXT_PAIRS,
  [" ", "&#x20;"],
  ["\t", "&#x9;"],
  ["\n", "&#xA;"],
  ["\r", "&#xD;"],
  ["\f", "&#xC;"],
  ["/", "&#x2F;"],
  ["=", "&#x3D;"],
  ["`", "&#x60;"],
]);

/** Two passes: the first finds the first byte needing work and the exact output
 *  length, so the second writes into a right-sized buffer. */
function escapeWith(src: Uint8Array, escapes: EscapeTable, name: string): Uint8Array {
  if (!(src instanceof Uint8Array)) throw new TypeError(`${name} must be a Uint8Array`);

  const len = src.length;
  let start = -1;
  let total = len;
  for (let i = 0; i < len; i++) {
    const replacement = escapes[src[i]];
    if (replacement === undefined) continue;
    if (start < 0) start = i;
    total += replacement.length - 1;
  }
  if (start < 0) return src;

  const out = new Uint8Array(total);
  out.set(src.subarray(0, start), 0);
  let w = start;
  for (let i = start; i < len; i++) {
    const byte = src[i];
    const replacement = escapes[byte];
    if (replacement === undefined) {
      out[w++] = byte;
      continue;
    }
    for (let p = 0; p < replacement.length; p++) out[w++] = replacement[p];
  }
  return out;
}

/** Escape `& < > " '` for an HTML text node or a quoted attribute value. */
export function htmlEscapeBytes(src: Uint8Array): Uint8Array {
  return escapeWith(src, TEXT_TABLE, "src");
}

/** Escape for an attribute value that may be unquoted: everything
 *  `htmlEscapeBytes` handles, plus whitespace and `/ = \``. */
export function attrEscapeBytes(src: Uint8Array): Uint8Array {
  return escapeWith(src, ATTR_TABLE, "src");
}
