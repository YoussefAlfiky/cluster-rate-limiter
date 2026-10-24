# cluster-rate-limiter

A zero-dependency rate limiter for Node.js apps running in [`cluster`](https://nodejs.org/api/cluster.html) mode — one primary process, several workers, one machine. Coordination happens entirely over native IPC; there's no Redis, no network hop, no external service to run.

## Table of contents

- [What this is (and isn't)](#what-this-is-and-isnt)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Choosing a mode](#choosing-a-mode)
- [API reference](#api-reference)
- [Warnings and constraints](#warnings-and-constraints)
- [Performance & Architecture](#performance--architecture)
- [Fault Tolerance & Crash Recovery](#fault-tolerance--crash-recovery)
- [Design trade-offs](#design-trade-offs)
- [License](#license)

## What this is (and isn't)

This package coordinates two different things, and picking the right one for your API matters:

- **Semaphore mode** caps how many calls can be *in flight at once* — a pure concurrency limit. Good fit for "this database allows 20 concurrent connections."
- **Duration mode** caps how many calls can *start within a trailing time window* — a real rate limit. Good fit for "this API allows 100 requests per second."

These are not the same guarantee, and reaching for the wrong one is the most common way to misuse a limiter like this: a concurrency cap alone does nothing to stop you from making 500 fast, sequential requests in a second, since none of them overlap.

**Scope, explicitly:** this coordinates the workers spawned by *one* primary process on *one* machine, via `cluster.fork()`. It is not a distributed rate limiter. If you run multiple machines or containers, each one gets its own independent limiter and its own independent pool — the true aggregate rate across N machines is N times whatever you configure, not the configured value. If you need one shared limit across a horizontally-scaled, multi-host deployment, you want something backed by a shared external store (Redis, etc.), not this. See [Design trade-offs](#design-trade-offs) for when each approach actually fits.

## Installation

```bash
npm install cluster-rate-limiter
```

## Quick start

**Primary process** — construct the limiter once, before forking workers:

```javascript
import cluster from "node:cluster";
import os from "node:os";
import { createPrimaryClusterRateLimiter } from "cluster-rate-limiter";

if (cluster.isPrimary) {
  createPrimaryClusterRateLimiter({
    apiProviderId: "stripe",
    maxConcurrentRequests: 100,
    mode: "duration",
    durationInSeconds: 1, // at most 100 Stripe calls started per second, across every worker
  });

  const numWorkers = os.availableParallelism?.() ?? os.cpus().length;
  for (let i = 0; i < numWorkers; i++) {
    cluster.fork();
  }
} else {
  await import("./worker.js");
}
```

**Worker process** (`worker.js`) — every worker that needs to call the rate-limited API gets its own worker-side handle, matching the primary's `apiProviderId` and `mode`.

**Pro-tip:** `run` automatically forwards arguments, so you can pass functions like `fetch` directly.

```javascript
import { createWorkerClusterRateLimiter } from "cluster-rate-limiter";

const stripeLimiter = createWorkerClusterRateLimiter({
  apiProviderId: "stripe",
  mode: "duration",
  permitTimeoutDurationInSeconds: 5, // maximum time a request will wait for an available slot
});

async function chargeCustomer(customerId, amountCents) {
  return stripeLimiter.run(
    fetch, 
    "https://api.stripe.com/v1/charges", 
    {
      method: "POST",
      body: JSON.stringify({ customer: customerId, amount: amountCents }),
    }
  );
}
```

`run()` acquires a permit, invokes your function with whatever arguments you pass it, and releases the permit once it settles (or, in duration mode, once the request has actually been dispatched — see [Performance & Architecture](#performance--architecture)). Whatever your function returns or throws is exactly what `run()` resolves or rejects with.

### Handling errors

Every failure this package raises is a `RateLimiterError` with a stable `.code` you can branch on instead of matching message strings:

```javascript
import { RateLimiterError } from "cluster-rate-limiter";

try {
  await stripeLimiter.run(doWork, arg1, arg2);
} catch (err) {
  if (err instanceof RateLimiterError && err.code === "ERR_PERMIT_TIMEOUT") {
    // The request waited too long for a slot. Safe to retry or return HTTP 429.
  }
  throw err;
}
```

## Choosing a mode

| | Semaphore mode | Duration mode |
|---|---|---|
| Guarantees | At most N calls in flight at once | At most N calls **started** in any trailing window |
| Capacity restored | When your function's promise settles | Automatically as the time window passes |
| Worker → primary traffic | One ask, one release, per call | One ask per call — no release message at all |
| Use for | "This has N concurrent connection slots" | "This API allows N requests per second" |

If the thing you're protecting has a hard concurrency ceiling (a connection pool, a license count), use **semaphore**. If it's an API with a published rate limit, use **duration** — a concurrency cap alone won't stop you from bursting past a requests-per-second limit.

## API reference

### `createPrimaryClusterRateLimiter(config)`

Call once, in the primary process, before forking workers (or any time after — workers that were forked earlier are found and reached automatically).

```typescript
type PrimarySemaphoreConfig = {
  apiProviderId: string;
  maxConcurrentRequests: number;
  mode: "semaphore";
};

type PrimaryDurationConfig = {
  apiProviderId: string;
  maxConcurrentRequests: number;
  mode: "duration";
  durationInSeconds: number; // Required for duration mode
};

function createPrimaryClusterRateLimiter(config: PrimarySemaphoreConfig): PrimarySemaphoreLimiter;
function createPrimaryClusterRateLimiter(config: PrimaryDurationConfig): PrimaryDurationLimiter;
```

Calling this again with the **same** `apiProviderId` and the **same** configuration is safe and idempotent — you get back the existing instance. Calling it again with the same `apiProviderId` but **different** settings throws `ERR_CONFLICTING_LIMITER_CONFIG` rather than silently keeping whichever configuration registered first.

### Primary Limiters: `PrimaryDurationLimiter` & `PrimarySemaphoreLimiter`

Primary limiters operate as the central orchestration engine for the cluster. They manage shared memory state, calculate token distribution in real-time, and route IPC capacity grants, acting as high-speed traffic controllers while leaving the execution of actual business logic to the worker processes. They expose properties for inspection and an event emitter interface to handle background IPC anomalies.

**Properties**
* `apiProviderId: string` (Readonly)
* `mode: "duration" | "semaphore"` (Readonly)

**Event Methods**
* `on(event, listener)` / `once(event, listener)`
* `off(event, listener)` / `clearListeners()`

**Events**
* `"grantPermitFailure": (workerId: number, apiProviderId: string, requestId: number, err: unknown) => void`
  Emitted when the primary process fails to transmit a capacity grant to a worker (e.g., worker died mid-flight). The primary safely refunds the slot internally.
* `"error": (err: unknown) => void`
  Emitted for critical but non-fatal background errors (e.g., handshake failures).


### `createWorkerClusterRateLimiter(config)`

Creates or retrieves a worker-side limiter for a specific `apiProviderId`. 

Because this factory is **idempotent**, you do not need to pass the limiter instance around your app. You can safely call this function wherever you need it (e.g., across different route files). As long as the `apiProviderId` and configuration match, it will return the exact same shared instance for that worker.

```typescript
type WorkerSemaphoreConfig = {
  apiProviderId: string;
  mode: "semaphore"; // Must match the primary's mode
  permitTimeoutDurationInSeconds: number;
};

type WorkerDurationConfig = {
  apiProviderId: string;
  mode: "duration"; // Must match the primary's mode
  permitTimeoutDurationInSeconds: number;
};

function createWorkerClusterRateLimiter(config: WorkerSemaphoreConfig): WorkerSemaphoreLimiter;
function createWorkerClusterRateLimiter(config: WorkerDurationConfig): WorkerDurationLimiter;
```

### Worker Limiters: `WorkerDurationLimiter` & `WorkerSemaphoreLimiter`

Worker limiters are the application-facing interfaces used to wrap and execute your actual business logic.

**Common API (Both Modes)**
* `apiProviderId: string` (Readonly)
* `mode: "duration" | "semaphore"` (Readonly)
* `ready(): Promise<boolean>`
  Manually awaits or triggers the IPC connection handshake with the primary process.
* `run<T, A extends unknown[]>(caller: (...args: A) => T | Promise<T>, ...args: A): Promise<T>`
  Automatically acquires a permit, executes the `caller` function, and returns its result.

**`WorkerSemaphoreLimiter` Only**

Because semaphore mode explicitly releases concurrency tokens when a task finishes, this class implements an event emitter to trap background release transmission failures.

**Event Methods**
* `on(event, listener)` / `once(event, listener)`
* `off(event, listener)` / `clearListeners()`

**Worker Events**
* `"releasePermitFailure": (apiProviderId: string, requestId: number, err: unknown) => void`
* `"error": (err: unknown) => void`

### `RateLimiterError`

```typescript
class RateLimiterError extends Error {
  code: string;
}
```

| Code | Raised when |
|---|---|
| `ERR_NOT_PRIMARY` | A primary limiter was constructed from a worker process. |
| `ERR_NOT_WORKER` | A worker limiter was constructed outside of a cluster worker (e.g., in the primary process or a standalone script). |
| `ERR_INVALID_API_PROVIDER_ID` | `apiProviderId` is not a valid string. |
| `ERR_INVALID_MAX_CONCURRENT_REQUESTS` | `maxConcurrentRequests` is not a positive integer. |
| `ERR_INVALID_MODE` | `mode` is not `"semaphore"` or `"duration"`. |
| `ERR_INVALID_DURATION` | `mode: "duration"` is configured without a valid, positive `durationInSeconds`. |
| `ERR_INVALID_TIMEOUT_DURATION` | `permitTimeoutDurationInSeconds` is not a positive finite number. |
| `ERR_INVALID_EVENT` / `ERR_INVALID_EVENT_TYPE` | An unrecognized event string was passed to `.on()`, `.once()`, or `.off()`. |
| `ERR_DUPLICATE_PRIMARY_LIMITER` / `ERR_DUPLICATE_WORKER_LIMITER` | Internal — surfaced as `ERR_CONFLICTING_LIMITER_CONFIG` by the factories when settings genuinely differ; otherwise handled silently. |
| `ERR_CONFLICTING_LIMITER_CONFIG` | The same `apiProviderId` was constructed twice with different settings in the same process. |
| `ERR_MODE_MISMATCH` | A worker's `mode` does not match the primary's initialized mode for this `apiProviderId`. |
| `ERR_HANDSHAKE_FAILED` | The primary process failed to send the initial boot configuration back to a worker. |
| `ERR_IPC_CHANNEL_CLOSED` | A worker attempted to acquire/release a permit, or complete a handshake, but its IPC channel to the primary process was closed or disconnected. |
| `ERR_PRIMARY_ACK_TIMEOUT` | The primary process never confirmed the worker's initial handshake. |
| `ERR_PERMIT_TIMEOUT` | `.run()` waited longer than `permitTimeoutDurationInSeconds` for capacity. |

## Warnings and constraints

- **Native `cluster` only.** The primary process must fork its own workers via `cluster.fork()`. Process managers that abstract or replace the primary process (PM2's cluster mode, for example) don't preserve the IPC tree this relies on — worker messages won't reach the limiter.
- **One machine.** See [What this is (and isn't)](#what-this-is-and-isnt). This does not aggregate across hosts.
- **Mode must match.** A worker configured for `"semaphore"` talking to a primary configured for `"duration"` (or vice versa) rejects on the first `.run()` call. The worker validates its configuration against the primary automatically in the background during the initial IPC handshake. This means the validation state is usually already resolved and waiting by the time `.run()` is invoked, completely preventing silent bypasses.
- **Don't block the event loop inside `run()`.** This depends on fast, timely IPC in both directions. A CPU-bound synchronous function passed as `caller` freezes the event loop, delays every other permit request and release on that worker, and causes real rate-limit drift. Offload heavy synchronous work to `node:worker_threads`; only the coordination needs to happen on the main thread.
- **Construct the primary limiter once per `apiProviderId`, and share it.** The factory is idempotent for matching configuration, so accidentally calling it more than once with the same settings is harmless. Calling it with conflicting settings is treated as the misconfiguration it almost certainly is, not silently resolved by picking one.

## Performance & Architecture

Engineered for bare-metal Node.js speed. This library relies on strict V8 optimizations and lock-free concurrency models to ensure the limiter never bottlenecks your throughput.

- **Zero-allocation window tracking (O(1)).** The sliding time-window log is powered by a fixed-capacity `Float64Array` ring buffer. Enqueue and prune operations execute strictly over raw memory, completely eliminating Garbage Collection (GC) spikes.
- **Zero-allocation request queues (O(1)).** Incoming requests waiting for capacity are tracked via flat `Int32Array` ring buffers. Routing pending cluster and request IDs requires zero new object allocations.
- **V8 SMI IPC payloads.** Hot-path messages use fixed-length arrays of Small Integers (SMIs) rather than JSON objects, drastically reducing V8 serialization/deserialization overhead.
- **O(1) fast-path routing.** Bypasses string hashing by upgrading established worker connections to instant integer-based `Map` lookups.
- **O(1) event loop footprint.** Uses a global `Symbol.for` registry to ensure exactly *one* `process.on('message')` listener is attached per process, regardless of how many limiters you instantiate. No `MaxListenersExceededWarning` or O(N) degradation.
- **One IPC hop, zero network overhead.** Coordination travels exclusively over `node:cluster`'s native libuv pipes. No TCP, no Redis, no network round-trips.
- **50% less IPC traffic in duration mode.** Time-based capacity requires no explicit release signals from the worker, halving the coordination overhead compared to semaphore mode.

> **The Philosophy:** The external API call you are gating will almost always cost orders of magnitude more time than the permit round-trip. The goal of this architecture is to guarantee that the limiter's internal bookkeeping is *never* the reason your throughput degrades under heavy load.

## Fault Tolerance & Crash Recovery

A worker that dies mid-request is detected via `cluster`'s own `'exit'` event, ensuring system stability without relying on arbitrary TTL timeouts:
- **Semaphore mode (Instant release):** Active capacity is instantly freed the moment the OS reaps the process, ensuring your throughput doesn't bottleneck waiting for a timeout.
- **Duration mode (State-preserving):** Crash events do not erase historical grants. Sent requests remain accurately counted until they naturally expire, guaranteeing zero rate-limit violations during worker restarts.

## Design trade-offs

|  | This package | A Redis-backed limiter (e.g. Bottleneck's clustering) |
|---|---|---|
| Coordinates across | One primary's own `cluster` workers | Any number of independent Node processes, any machines |
| External dependencies | None | Redis — provisioned, operated, secured separately |
| Per-permit cost | One local IPC round trip | One network round trip to Redis |
| Crash recovery | **Semaphore:** Instant release<br>**Duration:** State-preserving | Timeout/TTL-based |
| Ordering across workers | Exact — one arbiter, one queue | Approximate — each instance has its own local queue |
| Duration mode shape | Strict sliding-window log: never exceeds N in *any* trailing window, no burst credit | Often a token bucket: allows a burst up to the bucket size by spending capacity saved from a quiet period |

Neither column is strictly better — they solve different problems. If you're rate-limiting an external API from workers that all live on one box, the left column gets you a tighter guarantee with less infrastructure and lower latency. If you need one limit shared across a fleet of machines, you need the right column's shape of solution, and this package intentionally doesn't try to be that.

### A note on burst tolerance (Strict Window vs. Token Bucket)

By design, `duration` mode has **zero burst tolerance**. It uses a strict sliding window and will *never* exceed `maxConcurrentRequests` in any trailing time window. It does not act like a token bucket (which saves up capacity from quiet periods to allow sudden bursts).

* **If the target API enforces a strict window:** This limiter matches their logic exactly.
* **If the target API uses a token bucket:** This limiter will simply be more conservative than strictly necessary—which is the safest direction to be wrong in.

## License

MIT