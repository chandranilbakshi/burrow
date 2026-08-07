import type { Job } from "../core/job";
import type { PersistenceAdapter } from "./interface";

export class MemoryAdapter implements PersistenceAdapter {
  private jobs = new Map<string, Job>(); // jobId -> job
  private groups = new Map<string, Set<string>>(); // topic -> group names

  async enqueue(job: Job): Promise<void> {
    this.jobs.set(job.id, job);
  }

  async dequeue(topic: string, group: string): Promise<Job | null> {
    let picked: Job | null = null;
    for (const job of this.jobs.values()) {
      if (
        job.topic === topic &&
        job.consumerGroup === group &&
        job.status === "pending" &&
        job.visibleAt <= Date.now() &&
        (picked === null || job.visibleAt < picked.visibleAt)
      ) {
        picked = job;
      }
    }

    if (picked) {
      picked.status = "invisible";
      picked.visibleAt = Date.now() + picked.visibilityTimeout;
    }

    return structuredClone(picked);
  }

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

  async registerGroup(topic: string, group: string): Promise<void> {
    let set = this.groups.get(topic);
    if (!set) {
      set = new Set();
      this.groups.set(topic, set);
    }
    set.add(group);
  }

  async getGroups(topic: string): Promise<string[]> {
    return [...(this.groups.get(topic) ?? [])];
  }

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
