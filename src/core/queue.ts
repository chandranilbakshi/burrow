import type { PersistenceAdapter } from "../adapters/interface";
import { createJob, type EnqueueOptions, type Job } from "./job";
import { Worker } from "./worker";

export interface QueueOptions {
  adapter: PersistenceAdapter;
  defaultMaxAttempts?: number;
  defaultVisibilityTimeout?: number;
}

/** Producer-consumer queue: turns user input into complete jobs and hands them to storage. */
export class Queue {
  constructor(
    private readonly name: string,
    private readonly config: QueueOptions,
  ) {}

  /** Build a complete job from partial user input and enqueue it. */
  async add(
    topic: string,
    payload: unknown,
    options: EnqueueOptions = {},
  ): Promise<Job> {
    const job = createJob(topic, this.name, payload, options, this.config);

    await this.config.adapter.enqueue(job);
    return job;
  }

  /** Start a worker loop pulling `topic` off this queue. */

  // queue.process('send_sms', async (job) => {
  //  await twilio.send(job.payload.phone);
  // });
  process(
    topic: string,
    handler: (job: Job) => Promise<void>,
    options: {
      pollInterval?: number;
      onError?: (job: Job, error: unknown) => void;
      onDeadLetter?: (job: Job) => void;
    } = {},
  ): Worker {
    const worker = new Worker({
      adapter: this.config.adapter,
      topic,
      group: this.name,
      handler,
      ...options,
    });
    worker.start();
    return worker;
  }
}
