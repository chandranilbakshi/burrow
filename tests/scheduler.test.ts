import { test, expect, describe } from "bun:test";
import { MemoryAdapter } from "../src/adapters/memory";
import { Queue } from "../src/core/queue";
import { Scheduler } from "../src/core/scheduler";
import { waitFor } from "./helpers";

const TOPIC = "send_sms";
const GROUP = "sms-workers";

function setup() {
  const adapter = new MemoryAdapter();
  return { adapter, queue: new Queue(GROUP, { adapter }) };
}

/** Simulates a worker that claimed a job and then died without acking. */
async function claimAndAbandon(adapter: MemoryAdapter) {
  const claimed = await adapter.dequeue(TOPIC, GROUP);
  expect(claimed).not.toBeNull();
  return claimed!;
}

describe("Scheduler", () => {
  test("reclaims a job whose worker died mid-flight", async () => {
    const { adapter, queue } = setup();
    await queue.add(TOPIC, {}, { visibilityTimeout: 0 });
    const abandoned = await claimAndAbandon(adapter);

    const scheduler = new Scheduler({ adapter, interval: 1 });
    scheduler.start();

    let recovered: Awaited<ReturnType<MemoryAdapter["dequeue"]>> = null;
    await waitFor(async () => {
      recovered = await adapter.dequeue(TOPIC, GROUP);
      return recovered !== null;
    });
    await scheduler.stop();

    expect(recovered!.id).toBe(abandoned.id);
  });

  test("reports how many jobs it reclaimed", async () => {
    const { adapter, queue } = setup();
    await queue.add(TOPIC, {}, { visibilityTimeout: 0 });
    await queue.add(TOPIC, {}, { visibilityTimeout: 0 });
    await claimAndAbandon(adapter);
    await claimAndAbandon(adapter);

    let reclaimed = 0;
    const scheduler = new Scheduler({
      adapter,
      interval: 1,
      onRequeue: (count) => {
        reclaimed += count;
      },
    });
    scheduler.start();

    await waitFor(() => reclaimed === 2);
    await scheduler.stop();

    expect(reclaimed).toBe(2);
  });

  test("counts the lost delivery against the attempt budget", async () => {
    const { adapter, queue } = setup();
    await queue.add(TOPIC, {}, { visibilityTimeout: 0 });
    await claimAndAbandon(adapter);

    const scheduler = new Scheduler({ adapter, interval: 1 });
    scheduler.start();

    let requeued: Awaited<ReturnType<MemoryAdapter["dequeue"]>> = null;
    await waitFor(async () => {
      requeued = await adapter.dequeue(TOPIC, GROUP);
      return requeued !== null;
    });
    await scheduler.stop();

    // without this, a job that always times out would redeliver forever
    expect(requeued!.attempts).toBe(1);
  });

  test("leaves a job alone while its lease is still valid", async () => {
    const { adapter, queue } = setup();
    await queue.add(TOPIC, {}, { visibilityTimeout: 60_000 });
    await claimAndAbandon(adapter);

    let called = 0;
    const scheduler = new Scheduler({
      adapter,
      interval: 1,
      onRequeue: () => {
        called++;
      },
    });
    scheduler.start();
    await Bun.sleep(30);
    await scheduler.stop();

    expect(called).toBe(0);
    expect(await adapter.dequeue(TOPIC, GROUP)).toBeNull();
  });

  test("stays alive when a sweep throws", async () => {
    const { adapter, queue } = setup();
    await queue.add(TOPIC, {}, { visibilityTimeout: 0 });
    await claimAndAbandon(adapter);

    let sweeps = 0;
    const flaky = Object.create(adapter) as MemoryAdapter;
    flaky.requeueTimedOut = async () => {
      sweeps++;
      if (sweeps === 1) throw new Error("storage unavailable");
      return MemoryAdapter.prototype.requeueTimedOut.call(adapter);
    };

    let reclaimed = 0;
    const scheduler = new Scheduler({
      adapter: flaky,
      interval: 1,
      onRequeue: (count) => {
        reclaimed += count;
      },
    });
    scheduler.start();

    // the first sweep blew up; the loop must survive to run the second
    await waitFor(() => reclaimed === 1);
    await scheduler.stop();

    expect(sweeps).toBeGreaterThan(1);
  });

  test("stops sweeping after stop()", async () => {
    const { adapter, queue } = setup();
    let called = 0;
    const scheduler = new Scheduler({
      adapter,
      interval: 1,
      onRequeue: () => {
        called++;
      },
    });
    scheduler.start();
    await scheduler.stop();

    await queue.add(TOPIC, {}, { visibilityTimeout: 0 });
    await claimAndAbandon(adapter);
    await Bun.sleep(30);

    expect(called).toBe(0);
  });
});
