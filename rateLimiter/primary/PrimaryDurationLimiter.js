// @ts-check
import cluster from "node:cluster";
import { performance } from "node:perf_hooks";
import { Int32Queue } from "../../lib/Int32Queue.js"
import { Float64SortedQueue } from "../../lib/Float64SortedQueue.js";
import { BasePrimaryLimiter, _kTriggerGrantFailureEvent, _kPublish } from "./BasePrimaryLimiter.js";
import { IPC_SIGNATURE, IPC_OPCODES, IPC_INDEX } from "../shared/ipc-protocol.js";
import { setTimeout as setTimeoutPromise } from "node:timers/promises";

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
    /** @type {{resolve: () => void, timeout: NodeJS.Timeout} | null} */
    #capacityWaitTrackers = null;
    #startedGranting = false;
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
        // Assign Properties Values
        this.#maxConcurrentRequests = maxConcurrentRequests;
        this.#rateLimitDurationInMilliSeconds = (durationInSeconds * 1000) + 50 /* safety jitter buffer */;
        this.#grantTimestampSortedLog = new Float64SortedQueue(maxConcurrentRequests);
        // register
        this[_kPublish]({
            handleHotMessage: (worker, msg) => this.#handleHotMessage(worker, msg),
            handleExit: () => {} // no-op
        });
    };
    /**
        * Sweeps timestamps that have left the sliding window from the log and, if the log is
        * still full, waits for a free slot.
        * 
        * The wait does not block the event loop and ends at whichever comes first: the oldest
        * timestamp leaving the window, or `#refundSlot` freeing a slot early and cutting the
        * sleep short. After every wake-up the log is swept and checked again instead of
        * assuming there is room, so the limit always comes from the log, never from the wake-up.
        * 
        * Only `#grantPendingRequests` calls this, and its lock keeps at most one wait in flight.
        * The promise never rejects.
        * @returns {Promise<void>} Resolves once at least one capacity slot is free.
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
                // Wait until the oldest timestamp leaves the sliding window, or until #refundSlot frees a slot early
                await /**@type {Promise<void>}*/(new Promise((resolve) => {
                    const timeout = setTimeout(resolve, Math.min(remainingTimeTillNextTick, 2_147_483_647 /*24.8 days*/));
                    this.#capacityWaitTrackers = {resolve, timeout};
                }));
                this.#capacityWaitTrackers = null;
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
            if (!this.#startedGranting) {
                const now = performance.now();
                if (this.#rateLimitDurationInMilliSeconds <= 2050 && now < this.#rateLimitDurationInMilliSeconds) {
                    // Wait one window after primary start before granting => For windows up to 2 s
                    await setTimeoutPromise(this.#rateLimitDurationInMilliSeconds - now);
                };
                this.#startedGranting = true;
            };
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
                this.#grantRequest(worker, [IPC_SIGNATURE, this._apiUniqueNumber, IPC_OPCODES.GRANT, requestId, timestamp], 0);
            };
        } finally {
            this.#isGrantingPermitsForPendingRequests = false;
        };
    };
    /**
        * Reclaims a capacity slot for a grant that was never used: its IPC transmission failed,
        * or the worker's request had already timed out when the grant arrived (the worker then
        * answers with a RELEASE carrying the grant timestamp).
        * Uses a conservative O(1) shift by dropping the oldest timestamp rather than searching
        * for the exact one. If the grant loop is sleeping for capacity, it is woken so the
        * freed slot is used immediately.
        * 
        * @param {number} failedRequestTimestamp The time the unused grant was originally issued.
        * @returns {boolean} `true` if a slot was freed; `false` if nothing was removed because the
        * grant had already expired from the window or the log was empty.
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
            if (this.#capacityWaitTrackers) {
                const { resolve, timeout } = this.#capacityWaitTrackers;
                clearTimeout(timeout);
                resolve();
            };
            return true;
        };
        return false;
    };
    /** 
        * Transmits the capacity grant to the target worker via IPC.
        * Implements robust fault tolerance: handles synchronous APM wrapper crashes, 
        * asynchronous channel closures, and transient transmission failures via retries.
        * 
        * @param {Worker} worker The target Node.js cluster worker.
        * @param {number[]} message The GRANT payload: [signature, apiUniqueNumber, opcode, requestId, grantTimestamp].
        * @param {number} retryCount Current retry iteration (max 3).
    */
    #grantRequest(worker, message, retryCount) {
        const timestamp = message[IPC_INDEX.DURATION_MODE_GRANTED_REQUEST_TIMESTAMP];
        try {
            worker.send(message, (err) => {
                if (err) {
                    if (worker.isDead()) return this.#refundSlot(timestamp);
                    if (retryCount >= 3) {
                        this.#refundSlot(timestamp);
                        this[_kTriggerGrantFailureEvent](worker.id, message[IPC_INDEX.PAYLOAD] /*request id*/, err);
                        return;
                    }
                    setTimeout(() => {
                        const refundedSlot = this.#refundSlot(timestamp);
                        if (worker.isDead()) return;
                        if (!refundedSlot && this.#grantTimestampSortedLog.size >= this.#maxConcurrentRequests) {
                            this[_kTriggerGrantFailureEvent](worker.id, message[IPC_INDEX.PAYLOAD] /*request id*/, err);
                            return;
                        }
                        const newTimestamp = performance.now();
                        this.#grantTimestampSortedLog.enqueue(newTimestamp);
                        message[IPC_INDEX.DURATION_MODE_GRANTED_REQUEST_TIMESTAMP] = newTimestamp;
                        this.#grantRequest(worker, message, retryCount + 1)
                    }, 50);
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
        } else if (opCode === IPC_OPCODES.RELEASE) { 
            const timestamp = msg[IPC_INDEX.PAYLOAD]
            this.#refundSlot(timestamp)
        }
    };
};