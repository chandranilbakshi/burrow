import type { PersistenceAdapter } from "../adapters/interface";
import {
  DEFAULT_MAX_ATTEMPTS,
  DEFAULT_VISIBILITY_TIMEOUT_MS,
  type EnqueueOptions,
  type Job,
} from "./job";

export interface QueueOptions {
  adapter: PersistenceAdapter;
  defaultMaxAttempts?: number;
  defaultVisibilityTimeout?: number;
}

/** Producer-consumer queue: turns user input into complete jobs and hands them to storage. */
export class Queue {
  private readonly adapter: PersistenceAdapter;

  constructor(
    private readonly name: string,
    private readonly config: QueueOptions,
  ) {
    this.adapter = config.adapter;
  }

  /** Build a complete job from partial user input and enqueue it. */
  async add(
    topic: string,
    payload: unknown,
    options: EnqueueOptions = {},
  ): Promise<Job> {
    const now = Date.now();

    const job: Job = {
      id: crypto.randomUUID(),
      topic,
      consumerGroup: this.name,
      payload,
      status: "pending",
      createdAt: now,
      attempts: 0,
      visibleAt: now + (options.delay ?? 0),
      maxAttempts:
        options.maxAttempts ??
        this.config.defaultMaxAttempts ??
        DEFAULT_MAX_ATTEMPTS,
      visibilityTimeout:
        options.visibilityTimeout ??
        this.config.defaultVisibilityTimeout ??
        DEFAULT_VISIBILITY_TIMEOUT_MS,
      // TODO: Phase 4 — capture the active W3C traceparent here
      traceparent: null,
    };

    await this.adapter.enqueue(job);
    return job;
  }

  /** Start a worker loop pulling `topic` off this queue. */
  process(topic: string, handler: (job: Job) => Promise<void>): never {
    throw new Error("not implemented — needs worker.ts");
  }
}
