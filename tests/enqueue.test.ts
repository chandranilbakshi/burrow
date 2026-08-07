import { test, expect, describe } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory";
import { Queue, type QueueOptions } from "../src/core/queue";
import {
  DEFAULT_MAX_ATTEMPTS,
  DEFAULT_VISIBILITY_TIMEOUT_MS,
} from "../src/core/job";

function setup(config: Omit<QueueOptions, "adapter"> = {}) {
  const adapter = new MemoryAdapter();
  return { adapter, queue: new Queue("sms-sender", { adapter, ...config }) };
}

describe("Queue.add", () => {
  test("fills in every field storage requires", async () => {
    const { queue } = setup();
    const job = await queue.add("send_sms", { phone: "+910000000000" });

    expect(job.id).toBeString();
    expect(job.topic).toBe("send_sms");
    expect(job.consumerGroup).toBe("sms-sender"); // queue name is the group
    expect(job.status).toBe("pending");
    expect(job.attempts).toBe(0);
    expect(job.traceparent).toBeNull();
    expect(job.payload).toEqual({ phone: "+910000000000" });
  });

  test("stores the job where a matching dequeue can find it", async () => {
    const { adapter, queue } = setup();
    const job = await queue.add("send_sms", {});

    expect((await adapter.dequeue("send_sms", "sms-sender"))?.id).toBe(job.id);
  });

  test("gives each job a distinct id", async () => {
    const { queue } = setup();
    const a = await queue.add("send_sms", {});
    const b = await queue.add("send_sms", {});

    expect(a.id).not.toBe(b.id);
  });

  test("applies library defaults when nothing is configured", async () => {
    const { queue } = setup();
    const job = await queue.add("send_sms", {});

    expect(job.maxAttempts).toBe(DEFAULT_MAX_ATTEMPTS);
    expect(job.visibilityTimeout).toBe(DEFAULT_VISIBILITY_TIMEOUT_MS);
    expect(job.visibleAt).toBe(job.createdAt); // no delay means ready immediately
  });

  test("queue-level defaults override library defaults", async () => {
    const { queue } = setup({
      defaultMaxAttempts: 10,
      defaultVisibilityTimeout: 5_000,
    });
    const job = await queue.add("send_sms", {});

    expect(job.maxAttempts).toBe(10);
    expect(job.visibilityTimeout).toBe(5_000);
  });

  test("per-call options beat queue-level defaults", async () => {
    const { queue } = setup({
      defaultMaxAttempts: 10,
      defaultVisibilityTimeout: 5_000,
    });
    const job = await queue.add(
      "send_sms",
      {},
      { maxAttempts: 2, visibilityTimeout: 1_000 },
    );

    expect(job.maxAttempts).toBe(2);
    expect(job.visibilityTimeout).toBe(1_000);
  });

  test("honours an explicit zero rather than treating it as unset", async () => {
    const { queue } = setup({ defaultMaxAttempts: 10 });
    const job = await queue.add("send_sms", {}, { maxAttempts: 0, delay: 0 });

    expect(job.maxAttempts).toBe(0); // ?? not ||
    expect(job.visibleAt).toBe(job.createdAt);
  });
});

describe("Queue.add delays", () => {
  test("pushes visibleAt into the future by the delay", async () => {
    const { queue } = setup();
    const job = await queue.add("send_sms", {}, { delay: 60_000 });

    expect(job.visibleAt).toBe(job.createdAt + 60_000);
  });

  test("a delayed job is not dequeueable yet", async () => {
    const { adapter, queue } = setup();
    await queue.add("send_sms", {}, { delay: 60_000 });

    expect(await adapter.dequeue("send_sms", "sms-sender")).toBeNull();
  });

  test("a delay that has already elapsed is dequeueable", async () => {
    const { adapter, queue } = setup();
    const job = await queue.add("send_sms", {}, { delay: -1 });

    expect((await adapter.dequeue("send_sms", "sms-sender"))?.id).toBe(job.id);
  });
});
