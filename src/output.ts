import { copyBytes } from "./bytes.ts";

type Controller = TransformStreamDefaultController<Uint8Array>;

/** Where the curves cross: below this the extra read and microtask cost more
 *  than the memcpy, above it the memcpy does. */
const PASS_THROUGH_BYTES = 1024;

/** Output accumulator. Small pieces are merged once they reach `flushBytes`,
 *  trading a memcpy for far fewer enqueued parts. */
export class Emitter {
  ctrl!: Controller;
  bytesOut = 0;
  private readonly parts: Uint8Array[] = [];
  private len = 0;
  private readonly flushBytes: number;
  private readonly passThrough: number;

  constructor(flushBytes: number) {
    this.flushBytes = flushBytes;
    this.passThrough = Math.min(PASS_THROUGH_BYTES, flushBytes);
  }

  emit(part: Uint8Array): void {
    if (part.length === 0) return;
    this.bytesOut += part.length;
    if (this.flushBytes === 0) {
      this.ctrl.enqueue(part);
      return;
    }
    // Merging exists to avoid many small parts, and this is not one.
    if (part.length >= this.passThrough) {
      this.flush();
      this.ctrl.enqueue(part);
      return;
    }
    this.parts.push(part);
    this.len += part.length;
    if (this.len >= this.flushBytes) this.flush();
  }

  /** A lone piece has nothing to merge with, so it goes out by reference. */
  flush(): void {
    const parts = this.parts;
    if (parts.length === 0) return;
    if (parts.length === 1) {
      this.ctrl.enqueue(parts[0]);
    } else {
      const merged = new Uint8Array(this.len);
      let w = 0;
      for (let p = 0; p < parts.length; p++) {
        const part = parts[p];
        w = copyBytes(merged, w, part, 0, part.length);
      }
      this.ctrl.enqueue(merged);
    }
    parts.length = 0;
    this.len = 0;
  }

  reset(): void {
    this.parts.length = 0;
    this.len = 0;
  }
}
