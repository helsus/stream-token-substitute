// Escaping helpers and the response/resolver conveniences.

import { describe, expect, it } from "vitest";
import { resolveFrom, substituteResponse } from "../src/helpers.ts";
import { attrEscapeBytes, htmlEscapeBytes } from "../src/html-escape.ts";
import { jsonEscapeBytes } from "../src/json-escape.ts";
import { bytes, decoder, encoder } from "./helpers.ts";

const esc = (fn: (src: Uint8Array) => Uint8Array, text: string): string =>
  decoder.decode(fn(bytes(text)));

describe("htmlEscapeBytes", () => {
  it("escapes the five text-context bytes", () => {
    expect(esc(htmlEscapeBytes, `<a href="x">&'`)).toBe("&lt;a href=&quot;x&quot;&gt;&amp;&#x27;");
  });

  it("returns src itself when nothing needs escaping", () => {
    const src = bytes("plain text 123");
    expect(htmlEscapeBytes(src)).toBe(src);
  });

  it("leaves multi-byte utf-8 and invalid bytes untouched", () => {
    const src = new Uint8Array([...bytes("héllo中文"), 0xff, 0xc0]);
    expect(htmlEscapeBytes(src)).toBe(src);
  });

  it("neutralizes a script-closing payload", () => {
    expect(esc(htmlEscapeBytes, "</script><img onerror=alert(1)>")).not.toContain("<");
  });

  it("allocates exactly the bytes it writes", () => {
    const out = htmlEscapeBytes(bytes("<>&"));
    expect(out.byteLength).toBe(out.length);
    expect(decoder.decode(out)).toBe("&lt;&gt;&amp;");
  });

  it("rejects a non-Uint8Array", () => {
    expect(() => htmlEscapeBytes("x" as unknown as Uint8Array)).toThrow(TypeError);
  });
});

describe("attrEscapeBytes", () => {
  it("escapes everything htmlEscapeBytes does", () => {
    expect(esc(attrEscapeBytes, "<>&\"'")).toBe("&lt;&gt;&amp;&quot;&#x27;");
  });

  it("escapes the bytes that break out of an unquoted attribute", () => {
    // <div class=VALUE> with VALUE ending an unquoted attribute must not be
    // able to start a new one.
    expect(esc(attrEscapeBytes, "x onerror=alert(1)")).toBe("x&#x20;onerror&#x3D;alert(1)");
    expect(esc(attrEscapeBytes, "a\tb\nc\rd/e`f")).toBe("a&#x9;b&#xA;c&#xD;d&#x2F;e&#x60;f");
  });

  it("returns src itself for an already-safe value", () => {
    const src = bytes("safe-value_123");
    expect(attrEscapeBytes(src)).toBe(src);
  });
});

describe("jsonEscapeBytes sizing", () => {
  it("returns a buffer with no slack", () => {
    const out = jsonEscapeBytes(bytes('a"b\\c\nde'));
    expect(out.byteLength).toBe(out.length);
    expect(out.buffer.byteLength).toBe(out.length);
    expect(decoder.decode(out)).toBe('a\\"b\\\\c\\nd\\u0001e');
  });

  it("sizes U+2028 and U+2029 exactly", () => {
    const out = jsonEscapeBytes(bytes("a b c"));
    expect(out.buffer.byteLength).toBe(out.length);
    expect(decoder.decode(out)).toBe("a\\u2028b\\u2029c");
  });

  it("does not over-allocate for a long mostly-clean value", () => {
    const src = bytes(`${"x".repeat(4096)}<`);
    const out = jsonEscapeBytes(src);
    expect(out.buffer.byteLength).toBe(4096 + 6);
  });
});

describe("resolveFrom", () => {
  it("matches names as bytes from a record", () => {
    const resolve = resolveFrom({ name: "Ada", city: "London" });
    expect(decoder.decode(resolve(bytes("name")) as Uint8Array)).toBe("Ada");
    expect(decoder.decode(resolve(bytes("city")) as Uint8Array)).toBe("London");
  });

  it("returns null for an unknown name, which emits it verbatim", () => {
    const resolve = resolveFrom({ name: "Ada" });
    expect(resolve(bytes("nope"))).toBeNull();
    expect(resolve(bytes(""))).toBeNull();
  });

  it("does not confuse names that share a length or a prefix", () => {
    const resolve = resolveFrom({ ab: "1", ac: "2", a: "3", abc: "4" });
    expect(decoder.decode(resolve(bytes("ab")) as Uint8Array)).toBe("1");
    expect(decoder.decode(resolve(bytes("ac")) as Uint8Array)).toBe("2");
    expect(decoder.decode(resolve(bytes("a")) as Uint8Array)).toBe("3");
    expect(decoder.decode(resolve(bytes("abc")) as Uint8Array)).toBe("4");
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
    expect(decoder.decode(resolve(bytes("uwxqzevt")) as Uint8Array)).toBe("first");
    expect(decoder.decode(resolve(bytes("rlttrteo")) as Uint8Array)).toBe("second");
    expect(resolve(bytes("zzzzzzzz"))).toBeNull();
  });

  it("accepts a Map and Uint8Array values", () => {
    const resolve = resolveFrom(new Map([["k", bytes("V")]]));
    expect(decoder.decode(resolve(bytes("k")) as Uint8Array)).toBe("V");
  });

  it("copies Uint8Array values so later mutation cannot leak", () => {
    const value = bytes("AAA");
    const resolve = resolveFrom(new Map([["k", value]]));
    value[0] = 0x5a;
    expect(decoder.decode(resolve(bytes("k")) as Uint8Array)).toBe("AAA");
  });

  it("handles multi-byte names", () => {
    const resolve = resolveFrom({ 名前: "Ada" });
    expect(decoder.decode(resolve(bytes("名前")) as Uint8Array)).toBe("Ada");
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
    const upstream = new Response("hi {{name}}", {
      headers: {
        "content-type": "text/html",
        "content-length": "11",
        etag: '"abc"',
        digest: "sha-256=x",
        "cache-control": "public",
      },
    });
    const res = substituteResponse(upstream, options);
    expect(res.headers.get("content-length")).toBeNull();
    expect(res.headers.get("etag")).toBeNull();
    expect(res.headers.get("digest")).toBeNull();
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

  it("returns a bodyless response untouched", () => {
    const upstream = new Response(null, { status: 204 });
    expect(substituteResponse(upstream, options)).toBe(upstream);
  });

  it("accepts a prebuilt TransformStream, which is how async resolvers get in", async () => {
    const { createAsyncTokenTransformStream } = await import("../src/async-transformer.ts");
    const res = substituteResponse(
      new Response("hi {{name}}"),
      createAsyncTokenTransformStream({
        open: "{{",
        close: "}}",
        resolve: async () => bytes("Async"),
      }),
    );
    expect(await res.text()).toBe("hi Async");
  });
});

const escJson = (s: string) => decoder.decode(jsonEscapeBytes(encoder.encode(s)));

describe("jsonEscapeBytes", () => {
  it("returns src itself when nothing needs escaping", () => {
    const src = encoder.encode("plain text 123");
    expect(jsonEscapeBytes(src)).toBe(src);
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
    expect([...jsonEscapeBytes(src)]).toEqual([...src]);
  });

  it("escapes only the listed bytes around invalid UTF-8", () => {
    const src = new Uint8Array([0xff, 0x22, 0xc0]);
    expect([...jsonEscapeBytes(src)]).toEqual([0xff, 0x5c, 0x22, 0xc0]);
  });

  it("handles a truncated U+2028 prefix at the end", () => {
    const src = new Uint8Array([0x22, 0xe2, 0x80]);
    expect([...jsonEscapeBytes(src)]).toEqual([0x5c, 0x22, 0xe2, 0x80]);
  });

  it("rejects non-Uint8Array input", () => {
    // biome-ignore lint/suspicious/noExplicitAny: testing runtime validation
    expect(() => jsonEscapeBytes("x" as any)).toThrow(TypeError);
  });
});
