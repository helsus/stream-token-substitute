// Escaping helpers and the response/resolver conveniences.

import { describe, expect, it } from "vitest";
import { resolveFrom, substituteResponse } from "../src/helpers.ts";
import { escapeAttr, escapeHtml } from "../src/html-escape.ts";
import { escapeJson } from "../src/json-escape.ts";
import { bytes, decoder, encoder } from "./helpers.ts";

const esc = (fn: (src: Uint8Array) => Uint8Array, text: string): string =>
  decoder.decode(fn(bytes(text)));

describe("escapeHtml", () => {
  it("escapes the five text-context bytes", () => {
    expect(esc(escapeHtml, `<a href="x">&'`)).toBe("&lt;a href=&quot;x&quot;&gt;&amp;&#x27;");
  });

  it("returns src itself when nothing needs escaping", () => {
    const src = bytes("plain text 123");
    expect(escapeHtml(src)).toBe(src);
  });

  it("leaves multi-byte utf-8 and invalid bytes untouched", () => {
    const src = new Uint8Array([...bytes("h\u00e9llo\u4e2d\u6587"), 0xff, 0xc0]);
    expect(escapeHtml(src)).toBe(src);
  });

  it("neutralizes a script-closing payload", () => {
    expect(esc(escapeHtml, "</script><img onerror=alert(1)>")).not.toContain("<");
  });

  it("allocates exactly the bytes it writes", () => {
    const out = escapeHtml(bytes("<>&"));
    expect(out.byteLength).toBe(out.length);
    expect(decoder.decode(out)).toBe("&lt;&gt;&amp;");
  });

  it("rejects a non-Uint8Array", () => {
    expect(() => escapeHtml("x" as unknown as Uint8Array)).toThrow(TypeError);
  });
});

describe("escapeAttr", () => {
  it("escapes everything escapeHtml does", () => {
    expect(esc(escapeAttr, "<>&\"'")).toBe("&lt;&gt;&amp;&quot;&#x27;");
  });

  it("escapes the bytes that break out of an unquoted attribute", () => {
    // <div class=VALUE> with VALUE ending an unquoted attribute must not be
    // able to start a new one.
    expect(esc(escapeAttr, "x onerror=alert(1)")).toBe("x&#x20;onerror&#x3D;alert(1)");
    expect(esc(escapeAttr, "a\tb\nc\rd/e`f")).toBe("a&#x9;b&#xA;c&#xD;d&#x2F;e&#x60;f");
  });

  it("returns src itself for an already-safe value", () => {
    const src = bytes("safe-value_123");
    expect(escapeAttr(src)).toBe(src);
  });
});

describe("escapeJson sizing", () => {
  it("returns a buffer with no slack", () => {
    const out = escapeJson(bytes('a"b\\c\nde'));
    expect(out.byteLength).toBe(out.length);
    expect(out.buffer.byteLength).toBe(out.length);
    expect(decoder.decode(out)).toBe('a\\"b\\\\c\\nd\\u0001e');
  });

  it("sizes U+2028 and U+2029 exactly", () => {
    const out = escapeJson(bytes("a\u2028b\u2029c"));
    expect(out.buffer.byteLength).toBe(out.length);
    expect(decoder.decode(out)).toBe("a\\u2028b\\u2029c");
  });

  it("does not over-allocate for a long mostly-clean value", () => {
    const src = bytes(`${"x".repeat(4096)}<`);
    const out = escapeJson(src);
    expect(out.buffer.byteLength).toBe(4096 + 6);
  });
});

const ctx = { signal: new AbortController().signal };

describe("resolveFrom", () => {
  it("matches names as bytes from a record", () => {
    const resolve = resolveFrom({ name: "Ada", city: "London" });
    expect(decoder.decode(resolve(bytes("name"), ctx) as Uint8Array)).toBe("Ada");
    expect(decoder.decode(resolve(bytes("city"), ctx) as Uint8Array)).toBe("London");
  });

  it("returns null for an unknown name, which emits it verbatim", () => {
    const resolve = resolveFrom({ name: "Ada" });
    expect(resolve(bytes("nope"), ctx)).toBeNull();
    expect(resolve(bytes(""), ctx)).toBeNull();
  });

  it("does not confuse names that share a length or a prefix", () => {
    const resolve = resolveFrom({ ab: "1", ac: "2", a: "3", abc: "4" });
    expect(decoder.decode(resolve(bytes("ab"), ctx) as Uint8Array)).toBe("1");
    expect(decoder.decode(resolve(bytes("ac"), ctx) as Uint8Array)).toBe("2");
    expect(decoder.decode(resolve(bytes("a"), ctx) as Uint8Array)).toBe("3");
    expect(decoder.decode(resolve(bytes("abc"), ctx) as Uint8Array)).toBe("4");
  });

  it("resolves hash collisions", () => {
    const resolve = resolveFrom({
      uwxqzevt: "first",
      rlttrteo: "second",
      aaaaaaaa: "a",
      bbbbbbbb: "b",
      cccccccc: "c",
      dddddddd: "d",
      eeeeeeee: "e",
      ffffffff: "f",
      gggggggg: "g",
    });
    expect(decoder.decode(resolve(bytes("uwxqzevt"), ctx) as Uint8Array)).toBe("first");
    expect(decoder.decode(resolve(bytes("rlttrteo"), ctx) as Uint8Array)).toBe("second");
    expect(resolve(bytes("zzzzzzzz"), ctx)).toBeNull();
  });

  it("accepts a Map and Uint8Array values", () => {
    const resolve = resolveFrom(new Map([["k", bytes("V")]]));
    expect(decoder.decode(resolve(bytes("k"), ctx) as Uint8Array)).toBe("V");
  });

  it("copies Uint8Array values so later mutation cannot leak", () => {
    const value = bytes("AAA");
    const resolve = resolveFrom(new Map([["k", value]]));
    value[0] = 0x5a;
    expect(decoder.decode(resolve(bytes("k"), ctx) as Uint8Array)).toBe("AAA");
  });

  it("handles multi-byte names", () => {
    const resolve = resolveFrom({ "\u540d\u524d": "Ada" });
    expect(decoder.decode(resolve(bytes("\u540d\u524d"), ctx) as Uint8Array)).toBe("Ada");
  });
});

describe("substituteResponse", () => {
  const options = {
    open: "{{",
    close: "}}",
    resolve: resolveFrom({ name: "Ada" }),
  };

  it("substitutes the body", async () => {
    const res = substituteResponse(new Response("hi {{name}}"), options);
    expect(await res.text()).toBe("hi Ada");
  });

  it("drops the headers that no longer describe the body", async () => {
    const stale = {
      "content-length": "11",
      "content-encoding": "gzip",
      etag: '"abc"',
      digest: "sha-256=x",
      "content-digest": "sha-256=:x:",
      "repr-digest": "sha-256=:x:",
      "content-md5": "x",
      "accept-ranges": "bytes",
    };
    const upstream = new Response("hi {{name}}", {
      headers: { ...stale, "content-type": "text/html", "cache-control": "public" },
    });
    const res = substituteResponse(upstream, options);
    for (const name of Object.keys(stale)) expect(res.headers.get(name)).toBeNull();
    expect(res.headers.get("content-type")).toBe("text/html");
    expect(res.headers.get("cache-control")).toBe("public");
  });

  it("preserves status and statusText", () => {
    const res = substituteResponse(
      new Response("{{name}}", { status: 203, statusText: "Transformed" }),
      options,
    );
    expect(res.status).toBe(203);
    expect(res.statusText).toBe("Transformed");
  });

  it("rejects partial content", () => {
    const partial = new Response("{{name}}", { status: 206 });
    expect(() => substituteResponse(partial, options)).toThrow(/partial content/);
    const ranged = new Response("{{name}}", { headers: { "content-range": "bytes 0-7/100" } });
    expect(() => substituteResponse(ranged, options)).toThrow(TypeError);
  });

  it("applies backpressure inside a chunk by default", async () => {
    let calls = 0;
    const tokens = "{{x}}".repeat(100);
    const res = substituteResponse(new Response(tokens), {
      open: "{{",
      close: "}}",
      resolve: () => {
        calls++;
        return new Uint8Array(256 * 1024);
      },
    });
    const reader = res.body?.getReader();
    if (reader === undefined) throw new Error("no body");
    await reader.read();
    await reader.cancel();
    expect(calls).toBeLessThan(10);
  });

  it("returns a bodyless response untouched", () => {
    const upstream = new Response(null, { status: 204 });
    expect(substituteResponse(upstream, options)).toBe(upstream);
  });

  it("accepts a prebuilt TransformStream, which is how async resolvers get in", async () => {
    const { nativeTokenStream } = await import("./helpers.ts");
    const res = substituteResponse(
      new Response("hi {{name}}"),
      nativeTokenStream({
        open: "{{",
        close: "}}",
        resolve: async () => bytes("Async"),
      }),
    );
    expect(await res.text()).toBe("hi Async");
  });
});

const escJson = (s: string) => decoder.decode(escapeJson(encoder.encode(s)));

describe("escapeJson", () => {
  it("returns src itself when nothing needs escaping", () => {
    const src = encoder.encode("plain text 123");
    expect(escapeJson(src)).toBe(src);
  });

  it("escapes quotes and backslashes", () => {
    expect(escJson('a"b')).toBe('a\\"b');
    expect(escJson("a\\b")).toBe("a\\\\b");
  });

  it("uses named escapes for the JSON.stringify set", () => {
    expect(escJson("\b\t\n\f\r")).toBe("\\b\\t\\n\\f\\r");
  });

  it("uses lowercase \\u00XX for other controls", () => {
    expect(escJson("\x00\x01\x1f")).toBe("\\u0000\\u0001\\u001f");
  });

  it("escapes HTML-significant bytes", () => {
    expect(escJson("<script>&")).toBe("\\u003cscript\\u003e\\u0026");
  });

  it("escapes U+2028 and U+2029", () => {
    expect(escJson("a\u2028b\u2029c")).toBe("a\\u2028b\\u2029c");
  });

  it("does not escape DEL, matching JSON.stringify", () => {
    expect(escJson("\x7f")).toBe("\x7f");
  });

  it("matches JSON.stringify over the ASCII range", () => {
    for (let i = 0; i < 128; i++) {
      const ch = String.fromCharCode(i);
      const ours = escJson(ch);
      if (i === 0x3c || i === 0x3e || i === 0x26) continue; // extra escapes by design
      const theirs = JSON.stringify(ch).slice(1, -1);
      expect(ours, `byte ${i}`).toBe(theirs);
    }
  });

  it("round-trips through JSON.parse", () => {
    const samples = [
      "hello",
      'a"b\\c',
      "line\nbreak\ttab",
      "<script>alert(1)</script>",
      "unicode: \u00e9\u4e2d\u6587\u{1f600}",
      "\u2028\u2029",
      "\x00\x1f\x7f",
    ];
    for (const s of samples) {
      expect(JSON.parse(`"${escJson(s)}"`)).toBe(s);
    }
  });

  it("passes invalid UTF-8 through untouched", () => {
    const src = new Uint8Array([0xff, 0xfe, 0xc0, 0x80, 0xe2, 0x80, 0x41]);
    expect([...escapeJson(src)]).toEqual([...src]);
  });

  it("escapes only the listed bytes around invalid UTF-8", () => {
    const src = new Uint8Array([0xff, 0x22, 0xc0]);
    expect([...escapeJson(src)]).toEqual([0xff, 0x5c, 0x22, 0xc0]);
  });

  it("handles a truncated U+2028 prefix at the end", () => {
    const src = new Uint8Array([0x22, 0xe2, 0x80]);
    expect([...escapeJson(src)]).toEqual([0x5c, 0x22, 0xe2, 0x80]);
  });

  it("rejects non-Uint8Array input", () => {
    // biome-ignore lint/suspicious/noExplicitAny: testing runtime validation
    expect(() => escapeJson("x" as any)).toThrow(TypeError);
  });
});
