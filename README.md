# cluster-rate-limiter

A zero-dependency rate limiter for Node.js apps running in [`cluster`](https://nodejs.org/api/cluster.html) mode — one primary process, several workers, one machine. Coordination happens entirely over native IPC; there's no Redis, no network hop, no external service to run.

## Table of contents

- [What this is (and isn't)](#what-this-is-and-isnt)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Choosing a mode](#choosing-a-mode)
- [API reference](#api-reference)
- [Warnings and constraints](#warnings-and-constraints)
- [Performance](#performance)
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

`run()` acquires a permit, invokes your function with whatever arguments you pass it, and releases the permit once it settles (or, in duration mode, once the request has actually been dispatched — see [Performance](#performance)). Whatever your function returns or throws is exactly what `run()` resolves or rejects with.

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
type PrimaryConfig =
  | { apiProviderId: string; maxConcurrentRequests: number; mode: "semaphore" }
  | { apiProviderId: string; maxConcurrentRequests: number; mode: "duration"; durationInSeconds: number };

function createPrimaryClusterRateLimiter(config: PrimaryConfig): PrimaryRateLimiter_ClustersMode;
```

Calling this again with the **same** `apiProviderId` and the **same** configuration is safe and idempotent — you get back the existing instance. Calling it again with the same `apiProviderId` but **different** settings throws `ERR_CONFLICTING_LIMITER_CONFIG` rather than silently keeping whichever configuration registered first.

### `createWorkerClusterRateLimiter(config)`

Creates or retrieves a worker-side limiter for a specific `apiProviderId`. 

Because this factory is **idempotent**, you do not need to pass the limiter instance around your app. You can safely call this function wherever you need it (e.g., across different route files). As long as the `apiProviderId` and configuration match, it will return the exact same shared instance for that worker.

```typescript
type WorkerConfig = {
  apiProviderId: string;
  mode: "semaphore" | "duration"; // must match the primary's mode
  permitTimeoutDurationInSeconds: number;
};

function createWorkerClusterRateLimiter(config: WorkerConfig): WorkerRateLimiter_ClustersMode;
```

Returns an object with one method:

```typescript
run<T, A extends unknown[]>(caller: (...args: A) => T | Promise<T>, ...args: A): Promise<T>;
```

### `RateLimiterError`

```typescript
class RateLimiterError extends Error {
  code: string;
}
```

| Code | Raised when |
|---|---|
| `ERR_NOT_PRIMARY` | A primary limiter was constructed from a worker process |
| `ERR_NOT_WORKER` | A worker limiter was constructed from the primary process |
| `ERR_INVALID_API_PROVIDER_ID` | `apiProviderId` isn't a string |
| `ERR_INVALID_MAX_CONCURRENT_REQUESTS` | `maxConcurrentRequests` isn't a positive integer |
| `ERR_INVALID_MODE` | `mode` isn't `"semaphore"` or `"duration"` |
| `ERR_INVALID_DURATION` | `mode: "duration"` without a valid, positive `durationInSeconds` |
| `ERR_INVALID_TIMEOUT_DURATION` | `permitTimeoutDurationInSeconds` isn't a positive finite number |
| `ERR_DUPLICATE_PRIMARY_LIMITER` / `ERR_DUPLICATE_WORKER_LIMITER` | Internal — surfaced as `ERR_CONFLICTING_LIMITER_CONFIG` by the factories when settings genuinely differ; otherwise handled silently |
| `ERR_CONFLICTING_LIMITER_CONFIG` | Same `apiProviderId` constructed twice with different settings |
| `ERR_MODE_MISMATCH` | A worker's `mode` doesn't match the primary's for this `apiProviderId` |
| `ERR_PRIMARY_ACK_TIMEOUT` | The primary never confirmed the worker's handshake |
| `ERR_PERMIT_TIMEOUT` | `run()` waited longer than `permitTimeoutDurationInSeconds` for a permit |

## Warnings and constraints

- **Native `cluster` only.** The primary process must fork its own workers via `cluster.fork()`. Process managers that abstract or replace the primary process (PM2's cluster mode, for example) don't preserve the IPC tree this relies on — worker messages won't reach the limiter.
- **One machine.** See [What this is (and isn't)](#what-this-is-and-isnt). This does not aggregate across hosts.
- **Mode must match.** A worker configured for `"semaphore"` talking to a primary configured for `"duration"` (or vice versa) fails fast with `ERR_MODE_MISMATCH`, once, at construction — it won't silently half-work.
- **Don't block the event loop inside `run()`.** This depends on fast, timely IPC in both directions. A CPU-bound synchronous function passed as `caller` freezes the event loop, delays every other permit request and release on that worker, and causes real rate-limit drift. Offload heavy synchronous work to `node:worker_threads`; only the coordination needs to happen on the main thread.
- **Construct the primary limiter once per `apiProviderId`, and share it.** The factory is idempotent for matching configuration, so accidentally calling it more than once with the same settings is harmless. Calling it with conflicting settings is treated as the misconfiguration it almost certainly is, not silently resolved by picking one.

## Performance

- **No allocation on the hot path.** The duration-mode grant log is a fixed-capacity `Float64Array` ring buffer, not a linked list or array of boxed objects — enqueue, dequeue, and prune are `O(1)` / `O(log n)` operations over raw memory, with zero per-request heap allocation.
- **One IPC hop, not a network round trip.** Coordination goes over the pipe `cluster` already sets up between primary and worker — no serialization to a wire protocol, no separate service, no network stack. This is the fastest a cross-process rate limiter can plausibly be on one machine.
- **Duration mode sends fewer messages than you'd expect.** Because capacity is governed entirely by *when* a grant was issued, not by explicit confirmation, a duration-mode worker never sends a release message at all — half the IPC traffic of the equivalent semaphore-mode call.
- **Crash recovery is immediate, not timeout-based.** A worker that dies mid-request is detected via `cluster`'s own `'exit'` event — held capacity is reclaimed the moment the OS reaps the process, not after some multi-second TTL expires.

In practice, none of this bookkeeping is what limits your throughput — the external API call you're gating almost always costs orders of magnitude more time than the permit round trip. The point of the design above isn't to make the limiter fast in isolation; it's to make sure the limiter is never the reason your throughput is lower than it should be.

## Design trade-offs

|  | This package | A Redis-backed limiter (e.g. Bottleneck's clustering) |
|---|---|---|
| Coordinates across | One primary's own `cluster` workers | Any number of independent Node processes, any machines |
| External dependencies | None | Redis — provisioned, operated, secured separately |
| Per-permit cost | One local IPC round trip | One network round trip to Redis |
| Crash recovery | Immediate (`cluster` `'exit'` event) | Timeout/TTL-based |
| Ordering across workers | Exact — one arbiter, one queue | Approximate — each instance has its own local queue |
| Duration mode shape | Strict sliding-window log: never exceeds N in *any* trailing window, no burst credit | Often a token bucket: allows a burst up to the bucket size by spending capacity saved from a quiet period |

Neither column is strictly better — they solve different problems. If you're rate-limiting an external API from workers that all live on one box, the left column gets you a tighter guarantee with less infrastructure and lower latency. If you need one limit shared across a fleet of machines, you need the right column's shape of solution, and this package intentionally doesn't try to be that.

### A note on burst tolerance (Strict Window vs. Token Bucket)

By design, `duration` mode has **zero burst tolerance**. It uses a strict sliding window and will *never* exceed `maxConcurrentRequests` in any trailing time window. It does not act like a token bucket (which saves up capacity from quiet periods to allow sudden bursts).

* **If the target API enforces a strict window:** This limiter matches their logic exactly.
* **If the target API uses a token bucket:** This limiter will simply be more conservative than strictly necessary—which is the safest direction to be wrong in.

## License

MIT