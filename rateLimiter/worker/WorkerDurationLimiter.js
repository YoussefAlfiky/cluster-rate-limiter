//@ts-check
import { BaseWorkerLimiter, _kAcquirePermit } from "./BaseWorkerLimiter.js"

/**
    * Worker-side client for duration-based rate limiting in a Node.js cluster.
    * 
    * This class interfaces with the centralized rate limiter in the primary process via IPC 
    * to coordinate request execution. It delegates the state management and time-window 
    * calculations entirely to the primary process.
    * 
    * Permits acquired by this limiter are consumed immediately upon grant. 
    * 
    * This implementation does not require or dispatch release signals upon request completion, as the primary 
    * process automatically replenishes capacity over time.
*/
export class WorkerDurationLimiter extends BaseWorkerLimiter {
    /**
        * Initializes the worker-side duration rate limiter and establishes IPC routing.
        *
        * @param {string} apiProviderId
        * The unique identifier for the target API. Must strictly match the identifier 
        * registered in the primary process.
        * 
        * @param {number} permitTimeoutDurationInSeconds
        * Maximum time (in seconds) to wait for a permit before rejecting the request.
        * 
        * @throws {RateLimiterError} If instantiated within the primary process.
        * @throws {RateLimiterError} If a limiter for this `apiProviderId` is already registered in the worker.
        * @throws {RateLimiterError} If any configuration parameter is invalid or out of bounds.
    */
    constructor(apiProviderId, permitTimeoutDurationInSeconds) {
        super(apiProviderId, "duration", permitTimeoutDurationInSeconds, null);
    };
    /**
        * Executes the provided function after acquiring a capacity permit from the primary process.
        *
        * Permits are consumed upon acquisition. No IPC release signal is transmitted upon the 
        * resolution or rejection of the `caller`.
        * 
        * ** Warning: Event Loop Blocking**
        * This rate limiter relies on non-blocking IPC messaging. Passing CPU-bound synchronous 
        * functions directly as the caller will block the Node.js event loop, delay IPC message 
        * processing, and cause severe rate-limit drift. CPU-intensive tasks should be offloaded 
        * to `node:worker_threads`.
        * 
        * @template T
        * @template {unknown[]} A
        *
        * @param {(...args: A) => T | Promise<T>} caller
        * The target function to execute once the permit is acquired.
        *
        * @param {A} args
        * Arguments passed to the `caller`.
        *
        * @returns {Promise<T>}
        * A promise that resolves or rejects with the exact output of the `caller`.
        *
        * @throws {RateLimiterError}
        * If the primary process handshake fails or if the permit acquisition exceeds the timeout duration.
        *
        * @example
        * ```ts
        * const result = await rateLimiter.run(fetch, "https://api.example.com/data");
        * ```
    */
    async run(caller, ...args) {
        await this[_kAcquirePermit]();
        return caller(...args);
    };
};