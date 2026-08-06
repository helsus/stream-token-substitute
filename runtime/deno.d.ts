// Minimal shim for the one Deno API the adapter uses, so this file typechecks
// without pulling in unofficial Deno type packages.
declare namespace Deno {
  function test(name: string, fn: () => void | Promise<void>): void;
}
