import { copyBytes } from "./bytes.ts";
import type { OutputController } from "./flow.ts";

/** Below this a memcpy is cheaper than an extra read and microtask. */
const PASS_THROUGH_BYTES = 1024;

/** @internal Output accumulator that merges small pieces up to `mergeBytes`. */
export class Emitter {
  private controller: OutputController | undefined;
  bytesOut = 0;
  private readonly parts: Uint8Array[] = [];
  private readonly ranges: number[] = [];
  private len = 0;
  private started = false;
  private readonly mergeBytes: number;
  private limit = 0;
  private passThrough = 0;
  /** Only an enqueue can newly block, so a clear check stays valid until the next. */
  private recheck = true;

  constructor(mergeBytes: number) {
    this.mergeBytes = mergeBytes;
    this.ctrl = undefined;
  }

  get ctrl(): OutputController | undefined {
    return this.controller;
  }

  set ctrl(ctrl: OutputController | undefined) {
    this.controller = ctrl;
    this.recheck = true;
    this.limit = Math.min(this.mergeBytes, ctrl?.bufferLimit ?? this.mergeBytes);
    this.passThrough = Math.min(PASS_THROUGH_BYTES, this.limit);
  }

  get blocked(): boolean {
    if (!this.recheck) return false;
    const blocked = this.controller?.blocked?.() === true;
    this.recheck = blocked;
    return blocked;
  }

  private get out(): OutputController {
    if (this.controller === undefined) throw new TypeError("no output controller attached");
    return this.controller;
  }

  emit(part: Uint8Array): void {
    if (part.length === 0) return;
    this.bytesOut += part.length;
    if (part.length >= this.passThrough) {
      this.send(part, 0, part.length);
      return;
    }
    this.parts.push(part);
    this.len += part.length;
    if (this.len >= this.limit) this.flush();
  }

  emitRange(part: Uint8Array, start: number, end: number): void {
    const length = end - start;
    if (length === 0) return;
    this.bytesOut += length;
    if (length >= this.passThrough) {
      this.send(part, start, end);
      return;
    }
    this.ranges.push(this.parts.length, start, end);
    this.parts.push(part);
    this.len += length;
    if (this.len >= this.limit) this.flush();
  }

  /** Enqueue part[start..end) behind anything buffered, by reference. */
  private send(part: Uint8Array, start: number, end: number): void {
    this.flush();
    this.started = true;
    this.recheck = true;
    this.out.enqueue(start === 0 && end === part.length ? part : part.subarray(start, end));
  }

  flush(): void {
    const parts = this.parts;
    if (parts.length === 0) return;
    const ctrl = this.out;
    this.started = true;
    this.recheck = true;
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
