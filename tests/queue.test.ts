import { test, expect, describe } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory";
import {
  DEFAULT_MAX_ATTEMPTS,
  DEFAULT_VISIBILITY_TIMEOUT_MS,
  type Job,
} from "../src/core/job";

/** Build a ready-to-run job; override only the fields a test cares about. */
function makeJob(overrides: Partial<Job> = {}): Job {
  return {
    id: crypto.randomUUID(),
    status: "pending",
    topic: "send_sms",
    consumerGroup: "sms-sender",
    attempts: 0,
    maxAttempts: DEFAULT_MAX_ATTEMPTS,
    createdAt: Date.now(),
    visibleAt: Date.now(),
    visibilityTimeout: DEFAULT_VISIBILITY_TIMEOUT_MS,
    payload: { phone: "+910000000000" },
    traceparent: null,
    ...overrides,
  };
}

describe("dequeue", () => {
  test("returns an enqueued job, then hides it from the next caller", async () => {
    const adapter = new MemoryAdapter();
    const job = makeJob();
    await adapter.enqueue(job);

    expect((await adapter.dequeue("send_sms", "sms-sender"))?.id).toBe(job.id);
    // still leased by the first caller, so nobody else may have it
    expect(await adapter.dequeue("send_sms", "sms-sender")).toBeNull();
  });

  test("returns null for an empty queue", async () => {
    const adapter = new MemoryAdapter();
    expect(await adapter.dequeue("send_sms", "sms-sender")).toBeNull();
  });

  test("isolates topics and consumer groups", async () => {
    const adapter = new MemoryAdapter();
    await adapter.enqueue(makeJob());

    expect(await adapter.dequeue("other_topic", "sms-sender")).toBeNull();
    expect(await adapter.dequeue("send_sms", "other-group")).toBeNull();
    expect(await adapter.dequeue("send_sms", "sms-sender")).not.toBeNull();
  });

  test("hides jobs whose visibleAt is still in the future", async () => {
    const adapter = new MemoryAdapter();
    await adapter.enqueue(makeJob({ visibleAt: Date.now() + 60_000 }));

    expect(await adapter.dequeue("send_sms", "sms-sender")).toBeNull();
  });

  test("delivers the oldest ready job first", async () => {
    const adapter = new MemoryAdapter();
    const now = Date.now();
    const middle = makeJob({ visibleAt: now - 2_000 });
    const oldest = makeJob({ visibleAt: now - 3_000 });
    const newest = makeJob({ visibleAt: now - 1_000 });

    // insertion order deliberately differs from expected delivery order
    await adapter.enqueue(middle);
    await adapter.enqueue(oldest);
    await adapter.enqueue(newest);

    expect((await adapter.dequeue("send_sms", "sms-sender"))?.id).toBe(oldest.id);
    expect((await adapter.dequeue("send_sms", "sms-sender"))?.id).toBe(middle.id);
    expect((await adapter.dequeue("send_sms", "sms-sender"))?.id).toBe(newest.id);
  });

  test("hands back a copy, so callers cannot corrupt stored state", async () => {
    const adapter = new MemoryAdapter();
    await adapter.enqueue(makeJob({ visibilityTimeout: 0 }));

    const claimed = await adapter.dequeue("send_sms", "sms-sender");
    claimed!.attempts = 999;
    claimed!.payload.phone = "tampered";

    await adapter.requeueTimedOut();
    const again = await adapter.dequeue("send_sms", "sms-sender");
    expect(again!.attempts).toBe(1); // 1 from the requeue, not 999
    expect(again!.payload.phone).toBe("+910000000000");
  });
});

describe("ack", () => {
  test("removes the job permanently", async () => {
    const adapter = new MemoryAdapter();
    const job = makeJob({ visibilityTimeout: 0 });
    await adapter.enqueue(job);
    await adapter.dequeue("send_sms", "sms-sender");

    await adapter.ack(job.id);

    // an acked job must not come back even once its lease has expired
    expect(await adapter.requeueTimedOut()).toBe(0);
    expect(await adapter.dequeue("send_sms", "sms-sender")).toBeNull();
  });

  test("acking an unknown job is a no-op", async () => {
    const adapter = new MemoryAdapter();
    expect(adapter.ack("does-not-exist")).resolves.toBeUndefined();
  });
});

describe("nack", () => {
  test("counts the attempt and defers the job to retryAt", async () => {
    const adapter = new MemoryAdapter();
    const job = makeJob();
    await adapter.enqueue(job);
    await adapter.dequeue("send_sms", "sms-sender");

    await adapter.nack(job.id, Date.now() + 60_000);
    expect(await adapter.dequeue("send_sms", "sms-sender")).toBeNull();

    await adapter.nack(job.id, Date.now() - 1);
    const retried = await adapter.dequeue("send_sms", "sms-sender");
    expect(retried?.attempts).toBe(2);
  });
});

describe("requeueTimedOut", () => {
  test("reclaims jobs from workers that never acked", async () => {
    const adapter = new MemoryAdapter();
    await adapter.enqueue(makeJob({ visibilityTimeout: 0 }));

    const claimed = await adapter.dequeue("send_sms", "sms-sender");
    expect(claimed).not.toBeNull();
    expect(await adapter.dequeue("send_sms", "sms-sender")).toBeNull();

    expect(await adapter.requeueTimedOut()).toBe(1);

    const reclaimed = await adapter.dequeue("send_sms", "sms-sender");
    expect(reclaimed?.id).toBe(claimed!.id);
    // a timed-out delivery is a spent attempt, or a stuck job would loop forever
    expect(reclaimed?.attempts).toBe(1);
  });

  test("leaves jobs whose lease is still valid", async () => {
    const adapter = new MemoryAdapter();
    await adapter.enqueue(makeJob({ visibilityTimeout: 60_000 }));
    await adapter.dequeue("send_sms", "sms-sender");

    expect(await adapter.requeueTimedOut()).toBe(0);
  });
});

describe("consumer groups", () => {
  test("records subscribers and ignores duplicates", async () => {
    const adapter = new MemoryAdapter();
    await adapter.registerGroup("order.placed", "notification-service");
    await adapter.registerGroup("order.placed", "settlement-service");
    await adapter.registerGroup("order.placed", "notification-service");

    expect((await adapter.getGroups("order.placed")).sort()).toEqual([
      "notification-service",
      "settlement-service",
    ]);
  });

  test("returns an empty list for an unknown topic", async () => {
    const adapter = new MemoryAdapter();
    expect(await adapter.getGroups("nobody.listens")).toEqual([]);
  });
});

describe("dead letter queue", () => {
  test("parks the job and keeps it out of circulation", async () => {
    const adapter = new MemoryAdapter();
    const job = makeJob();
    await adapter.enqueue(job);

    await adapter.moveToDLQ(job.id);

    expect(await adapter.dequeue("send_sms", "sms-sender")).toBeNull();
    expect((await adapter.listDLQ("send_sms", "sms-sender")).map((j) => j.id)).toEqual([job.id]);
  });

  test("scopes listings to one topic and group", async () => {
    const adapter = new MemoryAdapter();
    const mine = makeJob();
    const theirs = makeJob({ consumerGroup: "other-group" });
    await adapter.enqueue(mine);
    await adapter.enqueue(theirs);
    await adapter.moveToDLQ(mine.id);
    await adapter.moveToDLQ(theirs.id);

    expect((await adapter.listDLQ("send_sms", "sms-sender")).map((j) => j.id)).toEqual([mine.id]);
  });

  test("replay makes the job runnable again with a clean slate", async () => {
    const adapter = new MemoryAdapter();
    const job = makeJob({ attempts: 3 });
    await adapter.enqueue(job);
    await adapter.moveToDLQ(job.id);

    await adapter.replayFromDLQ(job.id);

    const replayed = await adapter.dequeue("send_sms", "sms-sender");
    expect(replayed?.id).toBe(job.id);
    expect(replayed?.attempts).toBe(0);
    expect(await adapter.listDLQ("send_sms", "sms-sender")).toEqual([]);
  });

  test("discard deletes the job for good", async () => {
    const adapter = new MemoryAdapter();
    const job = makeJob();
    await adapter.enqueue(job);
    await adapter.moveToDLQ(job.id);

    await adapter.discardFromDLQ(job.id);

    expect(await adapter.listDLQ("send_sms", "sms-sender")).toEqual([]);
    expect(await adapter.dequeue("send_sms", "sms-sender")).toBeNull();
  });
});
