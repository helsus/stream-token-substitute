import { copyBytes } from "./bytes.ts";
import { BLOCKED, BUFFER_LIMIT, type OutputController } from "./flow.ts";

type Controller = TransformStreamDefaultController<Uint8Array>;

/** Where the curves cross: below this the extra read and microtask cost more
 *  than the memcpy, above it the memcpy does. */
const PASS_THROUGH_BYTES = 1024;

/** Output accumulator. Small pieces are merged once they reach `flushBytes`,
 *  trading a memcpy for far fewer enqueued parts. */
export class Emitter {
  private controller: OutputController | undefined;
  bytesOut = 0;
  private readonly parts: Uint8Array[] = [];
  private readonly ranges: number[] = [];
  private len = 0;
  private started = false;
  private readonly flushBytes: number;
  private passThrough: number;
  private limit: number;

  constructor(flushBytes: number) {
    this.flushBytes = flushBytes;
    this.limit = flushBytes;
    this.passThrough = Math.min(PASS_THROUGH_BYTES, flushBytes);
  }

  get ctrl(): Controller | undefined {
    return this.controller;
  }

  set ctrl(ctrl: Controller | undefined) {
    this.controller = ctrl;
    this.limit = Math.min(this.flushBytes, this.controller?.[BUFFER_LIMIT] ?? this.flushBytes);
    this.passThrough = Math.min(PASS_THROUGH_BYTES, this.limit);
  }

  get blocked(): boolean {
    return (this.ctrl as OutputController | undefined)?.[BLOCKED]?.() === true;
  }

  emit(part: Uint8Array): void {
    if (part.length === 0) return;
    this.bytesOut += part.length;
    if (part.length >= this.passThrough) {
      this.flush();
      this.started = true;
      (this.ctrl as Controller).enqueue(part);
      return;
    }
    this.parts.push(part);
    this.len += part.length;
    if (this.len >= this.limit) this.flush();
  }

  emitRange(part: Uint8Array, start: number, end: number): void {
    const length = end - start;
    if (length === 0) return;
    // Scanners attach a controller only while processing an active call.
    const ctrl = this.ctrl as Controller;
    this.bytesOut += length;
    if (this.limit === 0) {
      this.started = true;
      ctrl.enqueue(start === 0 && end === part.length ? part : part.subarray(start, end));
      return;
    }
    // Merging exists to avoid many small parts, and this is not one.
    if (length >= this.passThrough) {
      this.flush();
      this.started = true;
      ctrl.enqueue(start === 0 && end === part.length ? part : part.subarray(start, end));
      return;
    }
    this.ranges.push(this.parts.length, start, end);
    this.parts.push(part);
    this.len += length;
    if (this.len >= this.limit) this.flush();
  }

  /** A lone piece has nothing to merge with, so it goes out by reference. */
  flush(): void {
    const parts = this.parts;
    if (parts.length === 0) return;
    const ctrl = this.ctrl as Controller;
    this.started = true;
    if (parts.length === 1) {
      const part = parts[0];
      ctrl.enqueue(this.ranges.length === 0 ? part : part.subarray(this.ranges[1], this.ranges[2]));
    } else {
      const merged = new Uint8Array(this.len);
      let w = 0;
      if (this.ranges.length === 0) {
        for (let p = 0; p < parts.length; p++) {
          const part = parts[p];
          w = copyBytes(merged, w, part, 0, part.length);
        }
      } else {
        let r = 0;
        for (let p = 0; p < parts.length; p++) {
          const part = parts[p];
          if (this.ranges[r] === p) {
            w = copyBytes(merged, w, part, this.ranges[r + 1], this.ranges[r + 2]);
            r += 3;
          } else w = copyBytes(merged, w, part, 0, part.length);
        }
      }
      ctrl.enqueue(merged);
    }
    parts.length = 0;
    this.ranges.length = 0;
    this.len = 0;
  }

  reset(): void {
    this.ctrl = undefined;
    this.parts.length = 0;
    this.ranges.length = 0;
    this.len = 0;
  }

  /** Do not hold the first bytes behind a lookup. */
  flushInitial(): void {
    if (!this.started) this.flush();
  }
}
