# Burrow

A Bun-native distributed task queue with first-class distributed tracing.

Burrow supports both **producer-consumer** (work distribution) and **pub/sub** (event fan-out) patterns on a single underlying primitive, with pluggable persistence and — most importantly — W3C trace context propagation across the queue boundary.

> **Status: early, in active development.** The core queue, visibility timeout, retry, DLQ storage, pub/sub fan-out, and the crash-recovery scheduler work today against the in-memory adapter and are covered by tests. Redis and SQLite adapters and the OpenTelemetry layer are next. Sections below are marked ✅ shipped or 🚧 work in progress. See [Roadmap](#roadmap) for the full picture.

```typescript
import { Queue } from "./src/core/queue";
import { MemoryAdapter } from "./src/adapters/memory";

const queue = new Queue("notifications", { adapter: new MemoryAdapter() });

// Producer
await queue.add("send_sms", { phone: "+91...", body: "Your order is on its way" });

// Consumer
const worker = queue.process("send_sms", async (job) => {
  await twilio.send(job.payload.phone, job.payload.body);
});
```

---

## Why Burrow

Every task queue emits metrics. Almost none of them tell you *why* a job ran.

When a job fails in BullMQ or SQS, you get a dashboard entry: job ID, error message, attempt count. What you don't get is the request that caused it. The HTTP trace ends at "job enqueued." A new, unrelated trace starts inside the worker — if one starts at all. Two disconnected halves of the same story, and correlating them means grepping timestamps by hand.

**This is the gap Burrow exists to close, and it is the one thing no mainstream task queue does out of the box.** Burrow captures the active W3C `traceparent` at enqueue time, stores it on the job, and restores it as the parent span context when a worker picks the job up — including across retries, and across the process and machine boundary in between. The result is one unbroken trace:

```
POST /orders                                    [212ms]
├── db.insert orders                            [ 12ms]
├── burrow.enqueue send_sms                     [  2ms]
│   └── burrow.job.execute  attempt=1  FAILED   [5001ms]  Twilio timeout
│       └── burrow.job.execute  attempt=2  OK   [ 340ms]
│           └── http.client twilio.com          [ 320ms]
└── response 201
```

One trace ID. One waterfall. The retry that fixed it and the upstream call that caused it are in the same view as the request that started it.

🚧 **Not yet implemented.** The `traceparent` field exists on every job and is threaded through enqueue, fan-out, and retry; capturing and restoring it via `@opentelemetry/api` is the next phase of work.

---

## Features

**Two patterns, one primitive.** ✅ Producer-consumer and pub/sub are the same mechanism with different fan-out behaviour — a task queue is pub/sub with exactly one consumer group. Burrow exposes both without running two systems.

**Visibility timeout.** ✅ Jobs are not deleted on pickup. They are moved to an invisible set with an expiry deadline. If a worker crashes mid-job, the deadline lapses and the job returns to the pending set automatically. At-least-once delivery, no lost work.

**Crash recovery without a supervisor.** ✅ The deadline lives in storage, not in a timer in your process, so it survives the process dying. A background scheduler sweeps for expired leases and returns those jobs to pending.

**Exponential backoff.** ✅ Failed jobs retry on a `2^attempts * 1000ms` schedule rather than hammering an already-struggling upstream.

**Dead letter queue.** ✅ storage and replay/discard operations. 🚧 the `queue.dlq.*` convenience API.

**Pluggable persistence.** ✅ interface plus in-memory adapter. 🚧 Redis (via Bun's native client) and `bun:sqlite`.

**Distributed tracing across the queue boundary.** 🚧 The differentiator — see [Why Burrow](#why-burrow).

**Bun-native throughout.** ✅ No Node.js compatibility shims, no JavaScript Redis client, no build step. Written in strict TypeScript against Bun APIs directly.

---

## Delivery guarantees

Burrow is **at-least-once**. A job is never lost, but it may run more than once — a worker that completes its work and dies before acking will have that job redelivered, and a handler slower than its visibility timeout can be redelivered while still running.

**Your handlers must be idempotent.** Where the downstream API supports an idempotency key, pass `job.id` as that key. This is a property of every queue in this class (SQS included), not a limitation specific to Burrow.

---

## Installation

🚧 Not yet published to a registry. For now, clone the repo and import from `src/`.

```bash
bun install
bun test
```

Requires Bun 1.3 or later (for the native Redis client, used by the forthcoming `RedisAdapter`).

---

## Usage

### Producer-consumer ✅

Work is distributed across workers in the same consumer group. Each job is handled by exactly one of them.

```typescript
import { Queue } from "./src/core/queue";
import { MemoryAdapter } from "./src/adapters/memory";

const queue = new Queue("notifications", {
  adapter: new MemoryAdapter(),
  defaultVisibilityTimeout: 30_000,
  defaultMaxAttempts: 3,
});

await queue.add("send_sms", { phone: "+91...", body: "..." });

const worker = queue.process("send_sms", async (job) => {
  await twilio.send(job.payload.phone, job.payload.body);
});

// later
await worker.stop();
```

The queue name is the consumer group. Run five copies of that worker and each pulls a different job; throughput scales with worker count.

### Pub/sub ✅

Every subscriber group receives its own copy of every event, with its own attempts, backoff, and DLQ.

```typescript
import { EventBus } from "./src/core/event-bus";
import { MemoryAdapter } from "./src/adapters/memory";

const bus = new EventBus({ adapter: new MemoryAdapter() });

// subscribe first — a publish only reaches groups registered at that moment
await bus.subscribe("order.placed", "notification-service", async (job) => {
  await sendOrderConfirmation(job.payload.orderId);
});

await bus.subscribe("order.placed", "settlement-service", async (job) => {
  await openSettlementRecord(job.payload.sellerId);
});

await bus.publish("order.placed", { orderId: "ord_123", sellerId: "sel_9" });
```

Both handlers fire. Neither knows the other exists. If settlement fails and retries four times, notification never knows — the two copies are fully independent. Adding a third subscriber tomorrow requires no change to the publisher.

Burrow is a message bus, not a durable log: publishing to a topic with no registered subscribers creates no jobs and returns an empty array. Late subscribers do not receive past events.

### Delayed jobs ✅

```typescript
await queue.add("send_reminder", { orderId }, { delay: 60_000 });
```

### Crash recovery ✅

The scheduler reclaims jobs whose worker died or stalled. Construct one per process alongside your workers.

```typescript
import { Scheduler } from "./src/core/scheduler";

const scheduler = new Scheduler({
  adapter,
  interval: 5_000,
  onRequeue: (count) => console.warn(`reclaimed ${count} stalled jobs`),
});
scheduler.start();
```

A sustained nonzero `onRequeue` count means workers are crashing, or your visibility timeout is shorter than your handlers actually take.

🚧 Wiring a scheduler automatically when a worker starts.

### Observing failures ✅

```typescript
queue.process("send_sms", handler, {
  pollInterval: 1_000,
  onError: (job, error) => logger.error({ jobId: job.id, error }),
  onDeadLetter: (job) => alert(`job ${job.id} exhausted its retries`),
});
```

### Dead letter queue 🚧

The adapter supports listing, replaying, and discarding dead-lettered jobs today. The convenience API below is not yet wired up:

```typescript
const failed = await queue.dlq.list();
await queue.dlq.replay(failed[0].id);
await queue.dlq.replayAll();
await queue.dlq.discard(jobId);
```

---

## Persistence adapters

| Adapter | Use case | Durability | External infra | Status |
|---|---|---|---|---|
| `MemoryAdapter` | Tests, local development | None | None | ✅ |
| `RedisAdapter` | Production, multi-worker, multi-server | Configurable (AOF/RDB) | Redis | 🚧 |
| `SQLiteAdapter` | Single-server, edge, local dev | Full, on disk | None | 🚧 |

Adapters implement a single `PersistenceAdapter` interface — twelve methods covering enqueue/dequeue, ack/nack, lease reclamation, consumer group registration, and DLQ operations. Writing your own (Postgres, DynamoDB, anything) means implementing those twelve.

The interface is deliberately **mechanism only**. Retry backoff, the DLQ decision, and tracing all live in `src/core` and are passed to adapters as plain values — an adapter never computes a backoff delay or decides that a job is exhausted.

---

## OpenTelemetry 🚧

Not yet implemented. The planned design:

Tracing will be on by default when an OTel SDK is configured in the host process, using the global propagator and tracer from `@opentelemetry/api` — no separate configuration.

```typescript
import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";

new NodeSDK({
  traceExporter: new OTLPTraceExporter({
    url: "http://localhost:4318/v1/traces",
  }),
}).start();
```

Every enqueue captures the active context onto the job; every job execution becomes a child span of it.

### Spans planned

| Span | When | Key attributes |
|---|---|---|
| `burrow.enqueue` | Job added to queue | `topic`, `consumer_group`, `job_id` |
| `burrow.job.execute` | Worker begins handler | `job_id`, `topic`, `attempt`, `consumer_group` |
| `burrow.job.retry` | Retry scheduled | `job_id`, `attempt`, `backoff_ms` |
| `burrow.job.dlq` | Retries exhausted | `job_id`, `total_attempts`, `final_error` |

Failures will record the exception on the span and set span status to `ERROR`, so they surface in Tempo's error view and in RED metrics derived from spans.

### With Observiz 🚧

Burrow is built to drop into Observiz, a self-hosted LGTM stack (Loki, Grafana, Tempo, Prometheus, OTel Collector). Point the exporter at the Observiz collector and job traces appear alongside your HTTP traces with no extra instrumentation:

```typescript
new OTLPTraceExporter({ url: "http://localhost:4318/v1/traces" });
```

`examples/observiz.ts` will demonstrate an end-to-end run: an HTTP request that enqueues a job, a worker that fails once and succeeds on retry, and the full trace rendered in Tempo.

---

## How it works

A job is addressed by `(topic, consumerGroup)`. The topic says what kind of work it is; the consumer group says whose copy this is. Jobs move between three states.

```
pending     ready to run   — score = visibleAt (unix ms)
invisible   claimed        — score = lease expiry (unix ms)
dlq         given up on
```

🚧 The Redis layout these map onto:

```
burrow:<topic>:<group>:pending      sorted set
burrow:<topic>:<group>:invisible    sorted set
burrow:<topic>:<group>:dlq          sorted set
burrow:<topic>:groups               set of consumer group names
burrow:job:<id>                     hash — payload, attempts, status, traceparent
```

**Enqueue** writes the job and adds it to `pending` scored by when it should become visible. A `delay` is simply a score in the future.

**Dequeue** takes the lowest-scored member of `pending` whose score is not in the future, moves it to `invisible` scored `now + visibilityTimeout`, and returns a copy.

⚠️ This read-remove-move sequence is **not** atomic across multiple Redis commands — two workers polling concurrently can interleave inside it and claim the same job. `RedisAdapter` will perform the whole sequence in a single Lua script via `EVAL` so it executes server-side as one unit. This is the central correctness requirement of the Redis adapter.

**Ack** removes the job from `invisible` and deletes it. It is gone for good.

**Nack** increments `attempts` and returns the job to `pending` with a future score — the backoff delay, computed by the worker, not the adapter.

**The DLQ decision** belongs to the worker: if `attempts + 1 >= maxAttempts` the job is parked instead of retried. With `maxAttempts: 3` a job executes exactly three times before it is dead-lettered.

**The scheduler** sweeps `invisible` on an interval for members whose lease has expired and returns them to `pending`, counting the lost delivery as a spent attempt — without which a permanently stalled job would redeliver forever. This is what makes worker crashes recoverable with no supervisor process. Note that `nack` handles jobs that *fail*; the scheduler handles workers that fail — a crashed or hung process never gets to nack anything.

**Pub/sub** is the same machinery with a fan-out step: publishing to a topic looks up its registered consumer groups and writes one job per group, each with its own id and its own independent lifecycle. Downstream, nothing distinguishes a job created by `publish` from one created by `add`.

---

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 1 | Job model, adapter interface, in-memory adapter | ✅ |
| 2 | Producer-consumer: `Queue`, `Worker`, retry, backoff, DLQ | ✅ |
| 3 | Pub/sub: `EventBus`, fan-out, per-group isolation | ✅ |
| 7 | Requeue scheduler for crashed workers | ✅ |
| 4 | **OTel trace context propagation** — the differentiator | 🚧 next |
| 5 | `RedisAdapter` (Lua-atomic dequeue), `SQLiteAdapter` | 🚧 |
| 8 | `queue.dlq` replay API | 🚧 |
| — | Published package with a proper `burrow` entry point | 🚧 |
| — | Worked examples in `examples/` | 🚧 |
| — | Handler concurrency above 1 per worker | 🚧 |
| — | Lease fencing tokens to prevent stale acks | 🚧 |
| — | `job.extend()` heartbeat for long-running handlers | 🚧 |

---

## Development

```bash
bun install
bun test              # full suite, in-memory adapter
bun run typecheck     # tsc --noEmit
```

🚧 Once `RedisAdapter` lands, the Redis suite will run against a real server:

```bash
docker run -d -p 6379:6379 redis:7-alpine
REDIS_URL=redis://localhost:6379 bun test
```

---

## License

MIT
