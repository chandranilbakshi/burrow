import type { Job } from "../core/job";

/**
 * Storage backend for a Burrow queue: mechanism only, no policy.
 * Retry backoff, DLQ decisions, and tracing live in `src/core`.
 */
export interface PersistenceAdapter {
  /** Store a fully-built job and make it available to workers. */
  enqueue(job: Job): Promise<void>;

  /**
   * Atomically claim the next job whose `visibleAt` has passed, hiding it from
   * other workers until its timeout expires or it is acked. `null` if idle.
   */
  dequeue(topic: string, group: string): Promise<Job | null>;

  /** The job succeeded. Release the claim and delete the job permanently. */
  ack(jobId: string): Promise<void>;

  /**
   * Failed with attempts left: bump the attempt count and make it visible again
   * at `retryAt` (absolute Unix ms; the caller already applied the backoff).
   */
  nack(jobId: string, retryAt: number): Promise<void>;

  /**
   * Return jobs whose visibility timeout expired (crashed or stalled workers)
   * to the ready state. Returns the count; the scheduler calls this on a loop.
   */
  requeueTimedOut(): Promise<number>;

  /** Record `group` as a subscriber of `topic` so publishes fan out to it. */
  registerGroup(topic: string, group: string): Promise<void>;

  /** Every consumer group subscribed to `topic`. */
  getGroups(topic: string): Promise<string[]>;

  /** The job has exhausted its attempts. Park it in the dead letter queue. */
  moveToDLQ(jobId: string): Promise<void>;

  /** Every job currently parked in the DLQ for `topic` / `group`. */
  listDLQ(topic: string, group: string): Promise<Job[]>;

  /** Move a job out of the DLQ and back into the ready state, attempts reset. */
  replayFromDLQ(jobId: string): Promise<void>;

  /** Permanently delete a job from the DLQ. Not recoverable. */
  discardFromDLQ(jobId: string): Promise<void>;

  /**
   * Release underlying connections. Call in test teardown — an open Redis
   * socket keeps the process alive and hangs `bun test`.
   */
  close(): Promise<void>;
}
