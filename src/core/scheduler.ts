import type { PersistenceAdapter } from "../adapters/interface";

export const DEFAULT_SCHEDULER_INTERVAL_MS = 5_000;

export interface SchedulerOptions {
  adapter: PersistenceAdapter;
  /** How often to sweep for expired leases. */
  interval?: number;
  /** Called after each sweep that reclaimed at least one job. */
  onRequeue?: (count: number) => void;
}

/**
 * Reclaims jobs whose worker died or stalled. Runs inside the worker process,
 * not as a separate service.
 */
export class Scheduler {
  private running = false;
  private loopPromise: Promise<void> | null = null;

  constructor(private readonly options: SchedulerOptions) {}

  /** Begin sweeping. Returns immediately; the loop runs in the background. */
  start(): void {
    if (this.running) return;
    this.running = true;
    this.loopPromise = this.loop();
  }

  /** Stop sweeping and wait for the in-flight sweep to finish. */
  async stop(): Promise<void> {
    this.running = false;
    await this.loopPromise;
    this.loopPromise = null;
  }

  private async loop(): Promise<void> {
    const interval = this.options.interval ?? DEFAULT_SCHEDULER_INTERVAL_MS;

    while (this.running) {
      // wait one interval before sweeping again
      await Bun.sleep(interval);
      await this.sweep();
    }
  }

  /** One pass. Never throws — a failed sweep must not kill the loop. */
  private async sweep(): Promise<void> {
    try {
      // ask the adapter to reclaim expired leases
      const count = await this.options.adapter.requeueTimedOut();
      // report it, but only when something was actually reclaimed
      if (count > 0) this.options.onRequeue?.(count);
    } catch {
      // storage is unavailable; the next sweep will try again
    }
  }
}
