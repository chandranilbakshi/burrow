import { test, expect, describe } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory";
import { Queue } from "../src/core/queue";
import type { Job } from "../src/core/job";

const TOPIC = "send_sms";
const GROUP = "sms-workers";

function setup() {
  const adapter = new MemoryAdapter();
  return { adapter, queue: new Queue(GROUP, { adapter }) };
}

/** Poll until `check` passes. Avoids fixed sleeps, which are slow and flaky. */
async function waitFor(
  check: () => boolean | Promise<boolean>,
  timeoutMs = 2_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await Bun.sleep(1);
  }
  throw new Error(`condition not met within ${timeoutMs}ms`);
}

describe("end to end", () => {
  test("runs an enqueued job and acks it", async () => {
    const { adapter, queue } = setup();
    const seen: Job[] = [];

    const job = await queue.add(TOPIC, { phone: "+910000000000" });
    const worker = queue.process(
      TOPIC,
      async (j) => {
        seen.push(j);
      },
      { pollInterval: 1 },
    );

    await waitFor(() => seen.length === 1);
    await worker.stop();

    expect(seen[0]?.id).toBe(job.id);
    expect(seen[0]?.payload).toEqual({ phone: "+910000000000" });

    // acked: gone from storage entirely, not merely hidden
    expect(await adapter.dequeue(TOPIC, GROUP)).toBeNull();
    expect(await adapter.requeueTimedOut()).toBe(0);
  });

  test("only consumes its own topic", async () => {
    const { queue } = setup();
    let calls = 0;

    await queue.add("send_email", {});
    const worker = queue.process(TOPIC, async () => void calls++, {
      pollInterval: 1,
    });

    await Bun.sleep(30);
    await worker.stop();

    expect(calls).toBe(0);
  });

  test("stops polling after stop()", async () => {
    const { queue } = setup();
    let calls = 0;

    const worker = queue.process(TOPIC, async () => void calls++, {
      pollInterval: 1,
    });
    await worker.stop();

    await queue.add(TOPIC, {});
    await Bun.sleep(30);

    expect(calls).toBe(0);
  });
});

describe("retry", () => {
  test("defers a failed job instead of losing or dlq-ing it", async () => {
    const { adapter, queue } = setup();
    let calls = 0;

    await queue.add(TOPIC, {}, { maxAttempts: 3 });
    const worker = queue.process(
      TOPIC,
      async () => {
        calls++;
        throw new Error("boom");
      },
      { pollInterval: 1 },
    );

    await waitFor(() => calls === 1);
    await worker.stop();

    // backoff is 2^0 * 1000ms, so it must not be runnable yet
    expect(await adapter.dequeue(TOPIC, GROUP)).toBeNull();
    expect(await adapter.listDLQ(TOPIC, GROUP)).toEqual([]);
  });

  test("retries after the backoff and succeeds on the second attempt", async () => {
    const { adapter, queue } = setup();
    let calls = 0;

    await queue.add(TOPIC, {}, { maxAttempts: 3 });
    const worker = queue.process(
      TOPIC,
      async () => {
        calls++;
        if (calls === 1) throw new Error("transient");
      },
      { pollInterval: 5 },
    );

    // first attempt fails, then ~1s of backoff before the retry
    await waitFor(() => calls === 2, 5_000);
    await waitFor(async () => (await adapter.dequeue(TOPIC, GROUP)) === null);
    await worker.stop();

    expect(calls).toBe(2);
    expect(await adapter.listDLQ(TOPIC, GROUP)).toEqual([]);
  });

  test("counts a retry against the attempt budget", async () => {
    const { adapter, queue } = setup();

    // maxAttempts 1 means the very first failure is terminal
    const job = await queue.add(TOPIC, {}, { maxAttempts: 1 });
    const worker = queue.process(
      TOPIC,
      async () => {
        throw new Error("boom");
      },
      { pollInterval: 1 },
    );

    await waitFor(async () => (await adapter.listDLQ(TOPIC, GROUP)).length === 1);
    await worker.stop();

    const dead = await adapter.listDLQ(TOPIC, GROUP);
    expect(dead[0]?.id).toBe(job.id);
    // a dlq-ed job must never be handed to a worker again
    expect(await adapter.dequeue(TOPIC, GROUP)).toBeNull();
  });

  test("survives a handler that throws repeatedly, then gives up", async () => {
    const { adapter, queue } = setup();
    let calls = 0;

    await queue.add(TOPIC, {}, { maxAttempts: 2 });
    const worker = queue.process(
      TOPIC,
      async () => {
        calls++;
        throw new Error("always fails");
      },
      { pollInterval: 5 },
    );

    // attempt 1 fails, 1s backoff, attempt 2 fails, budget exhausted
    await waitFor(async () => (await adapter.listDLQ(TOPIC, GROUP)).length === 1, 5_000);
    await worker.stop();

    expect(calls).toBe(2);
  });
});
