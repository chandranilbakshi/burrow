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

/** Queue/bus-level fallbacks applied when a per-call option is omitted. */
export interface JobDefaults {
  defaultMaxAttempts?: number;
  defaultVisibilityTimeout?: number;
}

/** Build a complete job from partial user input. Shared by Queue.add and EventBus.publish. */
export function createJob(
  topic: string,
  consumerGroup: string,
  payload: unknown,
  options: EnqueueOptions = {},
  defaults: JobDefaults = {},
): Job {
  const now = Date.now();

  return {
    id: crypto.randomUUID(),
    topic,
    consumerGroup,
    payload,
    status: "pending",
    createdAt: now,
    attempts: 0,
    visibleAt: now + (options.delay ?? 0),
    maxAttempts:
      options.maxAttempts ??
      defaults.defaultMaxAttempts ??
      DEFAULT_MAX_ATTEMPTS,
    visibilityTimeout:
      options.visibilityTimeout ??
      defaults.defaultVisibilityTimeout ??
      DEFAULT_VISIBILITY_TIMEOUT_MS,
    // TODO: Phase 4 — capture the active W3C traceparent here
    traceparent: null,
  };
}
