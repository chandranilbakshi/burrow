import type { PersistenceAdapter } from "../adapters/interface";
import type { Job } from "./job";

export const DEFAULT_POLL_INTERVAL_MS = 1_000;

export interface WorkerOptions {
  adapter: PersistenceAdapter;
  topic: string;
  group: string;
  handler: (job: Job) => Promise<void>;
  /** How long to wait before polling again when the queue is empty. */
  pollInterval?: number;
  /** Called with whatever the handler threw, before the retry decision is made. */
  onError?: (job: Job, error: unknown) => void;
  /** Called once a job has exhausted its attempts and been parked in the DLQ. */
  onDeadLetter?: (job: Job) => void;
}

/** Pulls jobs off one topic/group and runs them, applying retry and DLQ policy. */
export class Worker {
  private running = false;
  private loopPromise: Promise<void> | null = null;

  constructor(private readonly options: WorkerOptions) {}

  /** Begin polling. Returns immediately; the loop runs in the background. */
  start(): void {
    if (this.running) return;
    this.running = true;
    this.loopPromise = this.loop();
  }

  /** Stop polling and wait for the in-flight job to finish. */
  async stop(): Promise<void> {
    this.running = false;
    await this.loopPromise;
    this.loopPromise = null;
  }

  private async loop(): Promise<void> {
    const pollInterval = this.options.pollInterval ?? DEFAULT_POLL_INTERVAL_MS;

    while (this.running) {
      // ask the adapter for a job on this topic/group
      const job = await this.options.adapter.dequeue(
        this.options.topic,
        this.options.group,
      );
      if (!job) {
        // nothing to do — wait before asking again, then continue
        await Bun.sleep(pollInterval);
        continue;
      }

      await this.runJob(job);
    }
  }

  /** Run one job and record the outcome. Never throws — the loop must survive. */
  private async runJob(job: Job): Promise<void> {
    try {
      // run the user's handler and wait for it
      await this.options.handler(job);
      // it resolved, so the job succeeded — tell the adapter
      await this.options.adapter.ack(job.id);
    } catch (error) {
      this.options.onError?.(job, error);
      await this.handleFailure(job);
    }
  }

  /** Decide whether a failed job gets another try or goes to the DLQ. */
  private async handleFailure(job: Job): Promise<void> {
    // `job.attempts` counts deliveries *before* this one, so this was attempt N+1.
    const attemptsUsed = job.attempts + 1;

    if (attemptsUsed >= job.maxAttempts) {
      // out of retries — park it
      await this.options.adapter.moveToDLQ(job.id);
      this.options.onDeadLetter?.(job);
      return;
    }

    // exponential backoff — 2^attempts seconds from now
    const retryAt = Date.now() + Math.pow(2, job.attempts) * 1000;
    // hand it back to the adapter with that timestamp
    await this.options.adapter.nack(job.id, retryAt);
  }
}
