export interface Job {
  id: string;
  status: JobStatus;
  /** Logical channel the job was published to; publishes fan out to its groups. */
  topic: string;
  /** The subscriber group this copy of the job belongs to. */
  consumerGroup: string;
  attempts: number;
  maxAttempts: number;
  createdAt: number;
  visibleAt: number;
  visibilityTimeout: number;
  payload: any;
  /** W3C traceparent captured at enqueue, linking the job to its producer's trace. */
  traceparent: string | null;
}

export const DEFAULT_MAX_ATTEMPTS = 3;
export const DEFAULT_VISIBILITY_TIMEOUT_MS = 30_000;

/**
 * Per-call options for `queue.add(topic, payload, options)`.
 */
export interface EnqueueOptions {
  /** How many times the job may run before it is moved to the DLQ. */
  maxAttempts?: number;
  /** Milliseconds to wait before the job first becomes visible. Omitted = available now. */
  delay?: number;
  /** How long a worker may hold this job before it is treated as crashed. */
  visibilityTimeout?: number;
}

export type JobStatus = "pending" | "invisible" | "failed" | "completed";
