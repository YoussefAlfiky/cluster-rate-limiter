//@ts-check
import { BaseWorkerLimiter, _kAcquirePermit, _kReleasePermit } from "./BaseWorkerLimiter.js";
import { EventListenerRegistry } from "../events/EventListenerRegistry.js";
/**
    * @typedef {(apiProviderId: string, requestId: number, err: unknown) => void | Promise<void>} ReleaseFailureCallback
    * @typedef {(err: unknown) => void | Promise<void>} ErrorCallback
    * 
    * @typedef {Object} ListenerBuckets
    * @property {{ on: Set<ReleaseFailureCallback>, once: Set<ReleaseFailureCallback> }} releasePermitFailure
    * @property {{ on: Set<ErrorCallback>, once: Set<ErrorCallback> }} error
    *
    * @typedef {Object} LimiterEvents
    * @property {ReleaseFailureCallback} releasePermitFailure
    * @property {ErrorCallback} error
*/

/**
    * Worker-side client for semaphore-based concurrency limiting in a Node.js cluster.
    * 
    * This class interfaces with the centralized rate limiter in the primary process to coordinate 
    * strict concurrency boundaries. It holds acquired permits for the entire lifecycle of the 
    * request and explicitly dispatches an IPC release signal once the target function settles 
    * (resolves or rejects).
    * 
    * Because the release phase executes asynchronously in the background to avoid blocking the 
    * caller's resolution, developers should attach `releasePermitFailure` and `error` listeners 
    * to handle potential IPC network anomalies during teardown.
*/
export class WorkerSemaphoreLimiter extends BaseWorkerLimiter {
    /**
        * @readonly
        * @type {EventListenerRegistry<ListenerBuckets>}
    */
    #eventEmitter;
    /**
        * Initializes the worker-side semaphore rate limiter and establishes IPC routing.
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
        /**@type {ListenerBuckets}*/
        const listenerBuckets = {
            releasePermitFailure: { on: new Set(), once: new Set() },
            error: { on: new Set(), once: new Set() }
        };
        super(apiProviderId, "semaphore", permitTimeoutDurationInSeconds, listenerBuckets);
        this.#eventEmitter = new EventListenerRegistry(listenerBuckets);
    };
    /**
        * Executes the provided function after acquiring a concurrency permit from the primary process.
        *
        * The permit occupies a concurrency slot for the entire duration of the target function's 
        * execution. Once the `caller` promise settles (resolves or rejects), an IPC release message 
        * is dispatched to the primary process.
        * 
        * **Warning: Event Loop Blocking**
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
        const requestId = await this[_kAcquirePermit]();
        try {
            return await caller(...args);
        } finally {
            this[_kReleasePermit](requestId);
        };
    };
    /**
        * Adds a persistent listener for the specified event.
        * 
        * @template {keyof LimiterEvents} E
        * @param {E} event 
        * @param {LimiterEvents[E]} listener 
        * @returns {void}
    */
    on(event, listener) {
        this.#eventEmitter.on(event, listener);
    };
    /**
        * Adds a one-time listener for the specified event.
        * 
        * @template {keyof LimiterEvents} E
        * @param {E} event 
        * @param {LimiterEvents[E]} listener 
        * @returns {void}
    */
    once(event, listener) {
        this.#eventEmitter.once(event, listener);
    };
    /**
        * Removes a specific listener from the specified event.
        * 
        * @template {keyof LimiterEvents} E
        * @param {E} event 
        * @param {LimiterEvents[E]} listener 
        * @returns {void}
    */
    off(event, listener) {
        this.#eventEmitter.off(event, listener);
    };
    /**
        * Removes all registered event listeners.
        * 
        * @returns {void}
    */
    clearListeners() {
        this.#eventEmitter.clearListeners();
    };
};