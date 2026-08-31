// @ts-check
import cluster from "node:cluster";
import { performance } from "node:perf_hooks";
import { setTimeout as setTimeoutPromise } from "node:timers/promises";
import { Queue } from "../lib/Queue.js";
import { Float64SortedQueue } from "../lib/Float64SortedQueue.js"
import { RateLimiterError } from "./errors/RateLimiterError.js";
/**
    * @typedef {import("node:cluster").Worker} Worker
    * @typedef {import("node:crypto").UUID} UUID
    * @typedef {{clusterId: number, requestId: UUID}} ClusterRequest
*/

/**
    * @typedef {Object} PrimaryRegistryEntry
    * @property {PrimaryRateLimiter_ClustersMode} limiter
    * @property {string} config
    * @property {(worker: Worker, msg: Object) => void} handleMessage
    * @property {(workerId: number) => void} handleExit
    * 
    * @typedef {typeof globalThis & { [key: symbol]: Map<string, PrimaryRegistryEntry> }} PrimaryGlobalObject
*/

const API_PROVIDERS_REGISTRY_KEY = Symbol.for('PrimaryRateLimiter_ClustersMode:registryMap');

/**
    * Coordinates request permits across Node.js cluster workers.
    * 
    * The limiter supports two modes:
    *
    * - **Semaphore mode** — (`mode === "semaphore"`)
        - The limiter allows up to `maxConcurrentRequests` requests to be in progress simultaneously.
        - Each granted permit represents one available concurrent-request slot.
        - A slot becomes available again when the worker finishes the request and releases its permit.
        - Therefore, the number of available permits depends on how many requests are currently in flight.
    *
    * - **Duration mode** — (`mode === "duration"`)
        - The limiter allows up to `maxConcurrentRequests` requests to be started during each duration window.
        - Releasing a permit acknowledges the request started, but does not make another permit available during the same time window.
        - Once `maxConcurrentRequests` requests have been granted, no additional requests are granted until the oldest timestamps expire from the sliding time window.
    *
    * **The primary process is responsible for maintaining the global permit state**,
    * while worker processes request and release permits through Node.js IPC.
    *
    * **This class must be instantiated only in the primary cluster process.**
    *
    * @example
    * ```ts
    * // Semaphore Mode: Allow at most 100 concurrent requests across all workers.
    * const rateLimiter = new PrimaryRateLimiter_ClustersMode(
    *     "my-api",
    *     100,
    *     "semaphore"
    * );
    * ```
    *
    * @example
    * ```ts
    * // Duration Mode: Allow at most 100 requests per second.
    * const rateLimiter = new PrimaryRateLimiter_ClustersMode(
    *     "my-api",
    *     100,
    *     "duration",
    *     1
    * );
    * ```
*/
export class PrimaryRateLimiter_ClustersMode {
    /** 
        * @readonly
        * @type {string}
    */
    #apiProviderId;
    /** 
        * @readonly
        * @type {Map<number, Worker>}
    */
    #clusters = new Map();
    /** 
        * @readonly
        * @type {Queue<ClusterRequest>}
    */
    #requestsQueue = new Queue();
    /** 
        * @readonly
        * @type {Float64SortedQueue}
    */
    #grantTimestampSortedLog;
    /** 
        * @readonly
        * @type {Map<number, Set<UUID>>}
    */
    #clusterPendingRequests = new Map();
    /** 
        * @readonly
        * @type {number}
    */
    #maxConcurrentRequests;
    /** 
     * @readonly 
     * @type {number}
    */
    #rateLimitDurationInMilliSeconds = 0;
    /** 
        * @readonly
        * @type {"semaphore" | "duration"} 
    */
    #mode;
    /** @type {number} */
    #remainingTokens;
    #isGrantingPermitsForPendingRequests = false;
    /** 
        * @param {number} failedRequestTimestamp 
    */
    #refundDurationSlot(failedRequestTimestamp) {
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
        * @param {Worker} worker
        * @param {{processingRequest: boolean, apiProviderId: string, requestId: UUID}} message 
        * @param {number} timestamp
        * @param {number} retryCount
    */
    #grantRequest(worker, message, timestamp, retryCount) {
        worker.send(message, (err) => {
            if (err) {
                if (worker.isDead()) {
                    if (this.#mode === "duration") {
                        this.#refundDurationSlot(timestamp);
                    };
                    // (Semaphore tokens are automatically refunded inside #unregisterWorker)
                    this.#unregisterWorker(worker.id);
                } else {
                    if (retryCount < 3) {
                        setTimeout(() => this.#grantRequest(worker, message, timestamp, retryCount + 1), 50);
                    } else {
                        if (this.#mode === "duration") {
                            this.#refundDurationSlot(timestamp);
                        } else {
                            this.#decrementSemaphoreWorkerUsage(worker.id, message.requestId);
                        };
                        console.error(`Failed to grant ${message.requestId} request for ${worker.id} worker.`);
                    };
                };
            };
        });
    };
    //
    async #grantPendingRequests() {
        if (this.#isGrantingPermitsForPendingRequests) return;
        this.#isGrantingPermitsForPendingRequests = true;
        try {
            while ( this.#requestsQueue.size > 0 ) {
                if (this.#mode === "semaphore") {
                    if (this.#remainingTokens <= 0) break;
                } else {
                    await this.#pruneGrantLog();
                };
                // this.#requestsQueue.dequeue() can't be undefined while requestsQueue.size > 0
                const { clusterId, requestId } = /**@type {ClusterRequest}*/(this.#requestsQueue.dequeue());
                const worker = this.#clusters.get(clusterId);
                if (!worker) continue;
                const timestamp = performance.now();
                if (this.#mode === "duration") {
                    this.#grantTimestampSortedLog.enqueue(timestamp);
                } else {
                    this.#incrementSemaphoreWorkerUsage(clusterId, requestId);
                };
                const message = {processingRequest: true, apiProviderId: this.#apiProviderId, requestId};
                this.#grantRequest(worker, message, timestamp, 0);
            };
        } finally {
            this.#isGrantingPermitsForPendingRequests = false;
        };
    };
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
        * @param {number} clusterId
        * @param {UUID} requestId 
    */
    #incrementSemaphoreWorkerUsage(clusterId, requestId) {
        const workerPendingRequestsSet = this.#clusterPendingRequests.get(clusterId);
        if (workerPendingRequestsSet) {
            workerPendingRequestsSet.add(requestId);
        } else {
            this.#clusterPendingRequests.set(clusterId, new Set([requestId]));
        };
        this.#remainingTokens--;
    };
    /** 
        * @param {number} clusterId
        * @param {UUID} requestId 
    */
    #decrementSemaphoreWorkerUsage(clusterId, requestId) {
        const workerPendingRequestsSet = this.#clusterPendingRequests.get(clusterId);
        if (!workerPendingRequestsSet) return;
        if (workerPendingRequestsSet.delete(requestId)) {
            if (workerPendingRequestsSet.size === 0) this.#clusterPendingRequests.delete(clusterId);
            this.#remainingTokens++;
            this.#grantPendingRequests();
        };
    };
    /** @param {number} clusterId */
    #unregisterWorker(clusterId) {
        if (this.#mode === "semaphore") {
            const workerPendingRequestsSet = this.#clusterPendingRequests.get(clusterId);
            this.#clusterPendingRequests.delete(clusterId);
            if (workerPendingRequestsSet) this.#remainingTokens += workerPendingRequestsSet.size;
        };
        this.#clusters.delete(clusterId);
        this.#grantPendingRequests();
    };
    /** 
        * @param {Worker} worker
        * @param {Object} msg 
    */
    #handleWorkerMessage(worker, msg) {
        if ('requestId' in msg && typeof msg.requestId === "string") {
            if ('askForToken' in msg && msg.askForToken === true) {
                const requestId = /**@type {UUID}*/(msg.requestId);
                this.#requestsQueue.enqueue({clusterId: worker.id, requestId});
                this.#clusters.set(worker.id, worker);
                this.#grantPendingRequests();
            };
            if ('terminateOneRequest' in msg && msg.terminateOneRequest === true) {
                const requestId = /**@type {UUID}*/(msg.requestId);
                this.#decrementSemaphoreWorkerUsage(worker.id, requestId);
            };
        };
        if ('acknowledgePrimary' in msg && msg.acknowledgePrimary === "pending") { 
            worker.send({apiProviderId: this.#apiProviderId, mode: this.#mode}) 
        };
    };
    /** 
        * @param {number} workerId 
    */
    #handleWorkerExit(workerId) {
        this.#unregisterWorker(workerId);
    }
    /**
        * Creates a cluster-wide rate limiter.
        *
        * @param {string} apiProviderId
        * Unique identifier of the API provider whose requests are being limited.
        *
        * @param {number} maxConcurrentRequests
        * Maximum number of permits available.
        * 
        * @param {"semaphore" | "duration"} mode
        * The rate-limiting strategy: "semaphore" for concurrency limits, or "duration" for time-window limits.
        *
        * @param {number | null | undefined} durationInSeconds
        * Required if mode is "duration". The length of the sliding request window in seconds.
        *
        * @throws {RateLimiterError}
        * If this class is instantiated from a cluster worker.
        *
        * @throws {RateLimiterError}
        * If `maxConcurrentRequests` is not a positive integer.
    */
    constructor(apiProviderId, maxConcurrentRequests, mode, durationInSeconds = undefined) {
        // Validations
        if (!cluster.isPrimary) {
            throw new RateLimiterError("ERR_NOT_PRIMARY", "PrimaryRateLimiter_ClustersMode must be instantiated only in the primary cluster process.");
        }
        if (typeof apiProviderId !== "string") {
            throw new RateLimiterError("ERR_INVALID_API_PROVIDER_ID", `Invalid apiProviderId: expected a string, but received ${typeof apiProviderId}.`);
        }
        if (!Number.isInteger(maxConcurrentRequests) || maxConcurrentRequests <= 0) {
            throw new RateLimiterError("ERR_INVALID_MAX_CONCURRENT_REQUESTS", "maxConcurrentRequests must be a positive integer.");
        }
        if (mode !== "duration" && mode !== "semaphore") {
            throw new RateLimiterError("ERR_INVALID_MODE", `Invalid mode: expected 'semaphore' or 'duration', but received '${mode}'.`);
        };
        if (mode === "duration") {
            if (!durationInSeconds || !Number.isFinite(durationInSeconds) || durationInSeconds <= 0) {
                throw new RateLimiterError("ERR_INVALID_DURATION", "InvalidDuration: when mode is duration, durationInSeconds must be a positive numeric value.");
            };
            this.#rateLimitDurationInMilliSeconds = (durationInSeconds * 1000) + 50 /* safety jitter buffer */;
        };
        // assign properties values
        this.#mode = mode;
        this.#apiProviderId = apiProviderId;
        this.#maxConcurrentRequests = maxConcurrentRequests;
        this.#grantTimestampSortedLog = new Float64SortedQueue(maxConcurrentRequests);
        this.#remainingTokens = maxConcurrentRequests;
        // Register Limiter and initiate listeners
        const globalObject = /**@type {PrimaryGlobalObject}*/(globalThis);
        const limitersRegistry = globalObject[API_PROVIDERS_REGISTRY_KEY];
        const registryEntry = {
            limiter: this,
            config: `${maxConcurrentRequests}-${mode}-${durationInSeconds}`,
            handleMessage: /**@param {Worker} worker @param {Object} msg*/(worker, msg) => this.#handleWorkerMessage(worker, msg),
            handleExit: /**@param {number} workerId*/(workerId) => this.#handleWorkerExit(workerId)
        };
        if (!limitersRegistry) {
            globalObject[API_PROVIDERS_REGISTRY_KEY] = new Map([[apiProviderId, registryEntry]]);
            cluster.on("message", (worker, msg) => {
                if (msg && typeof msg === "object" && 'apiProviderId' in msg && typeof msg.apiProviderId === "string") {
                    const entry = globalObject[API_PROVIDERS_REGISTRY_KEY].get(msg.apiProviderId);
                    if (entry) entry.handleMessage(worker, msg);
                }
            });
            cluster.on("exit", (worker) => {
                for (const [, entry] of globalObject[API_PROVIDERS_REGISTRY_KEY]) {
                    entry.handleExit(worker.id);
                }
            });
        } else {
            if (limitersRegistry.has(apiProviderId)) {
                throw new RateLimiterError("ERR_DUPLICATE_PRIMARY_LIMITER", `DuplicateRateLimiter: A PrimaryRateLimiter for apiProviderId '${apiProviderId}' has already been instantiated. You must share a single instance across your primary process.`);
            }
            limitersRegistry.set(apiProviderId, registryEntry);
        };
        // proactive primary-workers handshake
        for (const workerId in cluster.workers) {
            const worker = cluster.workers[workerId];
            if (!worker) continue;
            worker.send({apiProviderId, mode});
        };
    };
};