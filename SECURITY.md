# Security policy

## Reporting a vulnerability

Report privately through GitHub's [private vulnerability
reporting](https://github.com/helsus/stream-token-substitute/security/advisories/new). Please do
not open a public issue for a vulnerability.

Include the input, the delimiters or needle set, and the chunking that reproduces it. A failing
`FUZZ_SEED` is ideal.

## Scope

This library substitutes bytes. It is not a sanitizer: escaping a resolver's value for the
context it lands in is the caller's job, and `htmlEscapeBytes`, `attrEscapeBytes` and
`jsonEscapeBytes` are byte-level helpers, not validators.

In scope: any input where the streamed output differs from the same substitution applied to the
whole input at once, super-linear time on an input with a bounded `maxPayloadBytes`, carried
state growing with body size, or a substituted value being re-scanned as template.

## Supported versions

The latest published minor.
