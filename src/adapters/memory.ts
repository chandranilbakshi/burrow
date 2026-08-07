import type { Job } from "../core/job";
import type { PersistenceAdapter } from "./interface";

export class MemoryAdapter implements PersistenceAdapter {
  private jobs = new Map<string, Job>();
  private groups = new Map<string, Set<string>>();

  async enqueue(job: Job): Promise<void> {
    this.jobs.set(job.id, job);
  }

  async dequeue(topic: string, group: string): Promise<Job | null> {}

  async ack(jobId: string): Promise<void> {
    this.jobs.delete(jobId);
  }

  async nack(jobId: string, retryAt: number): Promise<void> {
    const job = this.jobs.get(jobId);
    if (!job) return;

    job.attempts += 1;
    job.visibleAt = retryAt;
    job.status = "pending";
  }

  async requeueTimedOut(): Promise<number> {
    let c = 0;
    for (const job of this.jobs.values()) {
      if (job.status === "invisible" && job.visibleAt <= Date.now()) {
        c++;
        job.attempts++;
        job.visibleAt = Date.now();
        job.status = "pending";
      }
    }
    return c;
  }

  async registerGroup(topic: string, group: string): Promise<void> {}

  async moveToDLQ(jobId: string): Promise<void> {
    const job = this.jobs.get(jobId);
    if (!job) return;

    job.status = "failed";
  }

  async listDLQ(topic: string, group: string): Promise<Job[]> {
    const dlq: Job[] = [];
    for (const job of this.jobs.values()) {
      if (
        job.status === "failed" &&
        job.topic === topic &&
        job.consumerGroup === group
      ) {
        dlq.push(job);
      }
    }
    return dlq;
  }

  async replayFromDLQ(jobId: string): Promise<void> {
    const job = this.jobs.get(jobId);
    if (!job) return;

    if (job.status === "failed") {
      job.status = "pending";
      job.attempts = 0;
      job.visibleAt = Date.now();
    }
  }

  async discardFromDLQ(jobId: string): Promise<void> {
    const job = this.jobs.get(jobId);
    if (!job) return;

    if (job.status === "failed") {
      this.jobs.delete(jobId);
    }
  }

  async close(): Promise<void> {}
}
