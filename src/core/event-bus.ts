import type { PersistenceAdapter } from "../adapters/interface";
import { createJob, type EnqueueOptions, type Job } from "./job";
import { Worker, type WorkerOptions } from "./worker";

export interface EventBusOptions {
  adapter: PersistenceAdapter;
  defaultMaxAttempts?: number;
  defaultVisibilityTimeout?: number;
}

export type SubscribeOptions = Omit<
  WorkerOptions,
  "adapter" | "topic" | "group" | "handler"
>;

/** Pub/sub: one published event becomes one job per registered subscriber group. */
export class EventBus {
  constructor(private readonly config: EventBusOptions) {}

  /**
   * Register `group` as a subscriber of `topic` and start consuming.
   * Only groups subscribed *before* a publish receive that event.
   */
  async subscribe(
    topic: string,
    group: string,
    handler: (job: Job) => Promise<void>,
    options: SubscribeOptions = {},
  ): Promise<Worker> {
    // record the group so future publishes fan out to it
    await this.config.adapter.registerGroup(topic, group);

    const worker = new Worker({
      adapter: this.config.adapter,
      topic,
      group,
      handler,
      ...options,
    });
    worker.start();

    return worker;
  }

  /**
   * Fan an event out to every subscribed group — one independent job each,
   * with its own attempts, backoff and DLQ. Returns the jobs created.
   */
  async publish(
    topic: string,
    payload: unknown,
    options: EnqueueOptions = {},
  ): Promise<Job[]> {
    const groups = await this.config.adapter.getGroups(topic);
    const jobs: Job[] = [];

    // sequential: a throw mid-loop leaves the event partially fanned out,
    // since the adapter gives us no cross-group atomicity
    for (const group of groups) {
      const job = createJob(topic, group, payload, options, this.config);
      await this.config.adapter.enqueue(job);
      jobs.push(job);
    }

    return jobs;
  }
}
