// @ts-check
import cluster from "node:cluster";
import { performance } from "node:perf_hooks";
import { setTimeout as setTimeoutPromise } from "node:timers/promises";
import { Int32Queue } from "../../lib/Int32Queue.js"
import { Float64SortedQueue } from "../../lib/Float64SortedQueue.js";
import { BasePrimaryLimiter, _kTriggerGrantFailureEvent } from "./BasePrimaryLimiter.js";
import { IPC_SIGNATURE, IPC_OPCODES, IPC_INDEX } from "../shared/ipc-protocol.js";
import { registerLimiterByUniqueNumber, unregisterLimiter } from "./registry/registerPrimaryLimiter.js";

/** @typedef {import("node:cluster").Worker} Worker */

/**
    * Primary-side sliding window (duration-based) rate limiter.
    * 
    * Manages capacity strictly by time, ensuring that no more than `maxConcurrentRequests` 
    * are granted within the specified `durationInSeconds`.
    * 
    * @extends {BasePrimaryLimiter}
*/
export class PrimaryDurationLimiter extends BasePrimaryLimiter {
    /** @readonly*/
    #requestsIdParallelQueue = new Int32Queue();
    /** @readonly*/
    #clustersIdParallelQueue = new Int32Queue();
    /** 
        * @readonly
        * @type {Float64SortedQueue}
    */
    #grantTimestampSortedLog;
    /** 
        * @readonly 
        * @type {number}
    */
    #rateLimitDurationInMilliSeconds = 0;
    /** 
        * @readonly
        * @type {number}
    */
    #maxConcurrentRequests = 0;
    #isGrantingPermitsForPendingRequests = false;
    /**
        * Creates a duration-based cluster rate limiter.
        *
        * @param {string} apiProviderId Unique identifier for the target API (e.g., 'stripe-api').
        * @param {number} maxConcurrentRequests Maximum number of requests allowed within the duration window.
        * @param {number} durationInSeconds The length of the sliding time window in seconds.
        *
        * @throws {RateLimiterError} If instantiated outside the primary cluster process.
        * @throws {RateLimiterError} If arguments are malformed or invalid.
    */
    constructor(apiProviderId, maxConcurrentRequests, durationInSeconds) {
        super(apiProviderId, "duration", maxConcurrentRequests, durationInSeconds);
        //  Register
        registerLimiterByUniqueNumber(this._apiUniqueNumber, {
            handleHotMessage: (worker, msg) => this.#handleHotMessage(worker, msg),
            handleExit: () => this.#grantPendingRequests()
        });
        // Assign Properties Values
        this.#maxConcurrentRequests = maxConcurrentRequests;
        this.#rateLimitDurationInMilliSeconds = (durationInSeconds * 1000) + 50 /* safety jitter buffer */;
        try {
            this.#grantTimestampSortedLog = new Float64SortedQueue(maxConcurrentRequests);
            this._executeProactiveHandshake();
        } catch (err) {
            unregisterLimiter(apiProviderId, this._apiUniqueNumber)
            throw err
        }
    };
    /**
        * Sweeps naturally expired timestamps from the log. 
        * 
        * If capacity is exhausted, it pauses execution without blocking the event loop, 
        * waiting exactly until the oldest timestamp expires to free a slot.
        * @returns {Promise<void>}
    */
    async #pruneGrantLog() {
        // Instantly sweep away any naturally expired timestamps
        this.#grantTimestampSortedLog.removeBefore(performance.now() - this.#rateLimitDurationInMilliSeconds);
        // If the queue is still full, we must wait for the oldest request to expire
        while (this.#grantTimestampSortedLog.size >= this.#maxConcurrentRequests) {
            const firstGrantTimeStamp = this.#grantTimestampSortedLog.peek();
            if (firstGrantTimeStamp === undefined) break; 
            const remainingTimeTillNextTick = this.#rateLimitDurationInMilliSeconds - (performance.now() - firstGrantTimeStamp);
            if (remainingTimeTillNextTick > 0) {
                // Wait exactly until the oldest timestamp falls out of the sliding window
                await setTimeoutPromise(remainingTimeTillNextTick);
            };
            this.#grantTimestampSortedLog.removeBefore(performance.now() - this.#rateLimitDurationInMilliSeconds);
        };
    };
    /**
        * Processes the pending requests queue sequentially. 
        * Uses an async lock to prevent overlapping executions while waiting for capacity.
        * @returns {Promise<void>}
    */
    async #grantPendingRequests() {
        if (this.#isGrantingPermitsForPendingRequests || !cluster.workers) return;
        this.#isGrantingPermitsForPendingRequests = true;
        try {
            while ( this.#requestsIdParallelQueue.size > 0 ) {
                await this.#pruneGrantLog();
                /* 
                    this.#requestsIdParallelQueue.dequeue() and this.#clustersIdParallelQueue.dequeue()
                    can't be undefined while requestsIdParallelQueue.size > 0 
                */
                const requestId = /**@type {number}*/(this.#requestsIdParallelQueue.dequeue());
                const clusterId = /**@type {number}*/(this.#clustersIdParallelQueue.dequeue());
                const worker = cluster.workers[clusterId];
                if (!worker || worker.isDead()) continue;
                const timestamp = performance.now();
                this.#grantTimestampSortedLog.enqueue(timestamp);
                this.#grantRequest(worker, [IPC_SIGNATURE, this._apiUniqueNumber, IPC_OPCODES.GRANT, requestId], timestamp, 0);
            };
        } finally {
            this.#isGrantingPermitsForPendingRequests = false;
        };
    };
    /** 
        * Safely reclaims a capacity slot if an IPC transmission fails.
        * Uses a conservative O(1) mathematical shift by dropping the oldest timestamp 
        * rather than performing an expensive array search for the exact failed timestamp.
        * 
        * @param {number} failedRequestTimestamp The exact time the failed request was originally granted.
    */
    #refundSlot(failedRequestTimestamp) {
        /* Prevent "Double Refund": Only dequeue if the failed request's 
        original timestamp hasn't already been naturally pruned by time!*/
        const hasNaturallyExpired = (performance.now() - failedRequestTimestamp) >= this.#rateLimitDurationInMilliSeconds;
        if (!hasNaturallyExpired && this.#grantTimestampSortedLog.size > 0) {
            /* We purposefully call `dequeue()` to drop the *oldest* timestamp 
            rather than doing an expensive O(N) search to delete this exact failed timestamp.
            This safely refunds 1 capacity slot instantly. It mathematically causes a slight 
            "conservative shift" (throttling future requests slightly), 
            which guarantees we never breach the API limit while maintaining bare-metal C++ speed.*/
            this.#grantTimestampSortedLog.dequeue();
        };
        this.#grantPendingRequests();
    };
    /** 
        * Transmits the capacity grant to the target worker via IPC.
        * Implements robust fault tolerance: handles synchronous APM wrapper crashes, 
        * asynchronous channel closures, and transient transmission failures via retries.
        * 
        * @param {Worker} worker The target Node.js cluster worker.
        * @param {number[]} message The high-performance integer array IPC payload.
        * @param {number} timestamp The timestamp when the permit was granted.
        * @param {number} retryCount Current retry iteration (max 3).
    */
    #grantRequest(worker, message, timestamp, retryCount) {
        try {
            worker.send(message, (err) => {
                if (err) {
                    if (worker.isDead()) {
                        this.#refundSlot(timestamp);
                    } else {
                        if (retryCount < 3) {
                            setTimeout(() => this.#grantRequest(worker, message, timestamp, retryCount + 1), 50);
                        } else {
                            this.#refundSlot(timestamp);
                            this[_kTriggerGrantFailureEvent](worker.id, message[IPC_INDEX.PAYLOAD] /*request id*/, err);
                        };
                    };
                };
            });
        } catch (syncErr) {
            this.#refundSlot(timestamp);
            if (worker.isDead()) return;
            try {
                const normalizedError = syncErr instanceof Error ? syncErr : new Error(String(syncErr));
                this[_kTriggerGrantFailureEvent](worker.id, message[IPC_INDEX.PAYLOAD]/*request id*/, normalizedError);
            } catch {
                process.emitWarning(
                    `Failed to route synchronous IPC error for worker ${worker.id}. The error object may be corrupted.`,
                    "RateLimiterWarning",
                    "A critical exception was swallowed during permit grant error handling."
                );
            };
        };
    };
    /** 
        * @param {Worker} worker
        * @param {number[]} msg 
    */
    #handleHotMessage(worker, msg) {
        const opCode = msg[IPC_INDEX.OPCODE];
        if (opCode === IPC_OPCODES.ACQUIRE) {
            const requestId = msg[IPC_INDEX.PAYLOAD]
            this.#requestsIdParallelQueue.enqueue(requestId)
            this.#clustersIdParallelQueue.enqueue(worker.id)
            this.#grantPendingRequests();
        };
    };
};