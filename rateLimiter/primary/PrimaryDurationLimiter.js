// @ts-check
import cluster from "node:cluster";
import { performance } from "node:perf_hooks";
import { setTimeout as setTimeoutPromise } from "node:timers/promises";
import { Queue } from "../../lib/Queue.js";
import { Float64SortedQueue } from "../../lib/Float64SortedQueue.js";
import { validatePrimaryLimiterConstructorInput } from "./validators/validatePrimaryLimiter.js";
import { registerPrimaryLimiter } from "./registry/registerPrimaryLimiter.js";
import { EventListenerRegistry } from "../events/EventListenerRegistry.js";
import { triggerEvent } from "../events/triggerEvent.js"

/**
    * @typedef {import("node:cluster").Worker} Worker
    * @typedef {{clusterId: number, requestId: number}} ClusterRequest
*/
/**
    * @typedef {(workerId: number, apiProviderId: string, requestId: number, err: unknown) => void | Promise<void>} GrantPermitFailureCallback
    * @typedef {(err: unknown) => void | Promise<void>} ErrorCallback
    * 
    * @typedef {Object} PrimaryListenerBuckets
    * @property {{ on: Set<GrantPermitFailureCallback>, once: Set<GrantPermitFailureCallback> }} grantPermitFailure
    * @property {{ on: Set<ErrorCallback>, once: Set<ErrorCallback> }} error
*/

/** 
    * @extends {EventListenerRegistry<PrimaryListenerBuckets>}
*/
export class PrimaryDurationLimiter extends EventListenerRegistry {
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
        * @type {number}
    */
    #rateLimitDurationInMilliSeconds = 0;
    /** 
        * @readonly
        * @type {number}
    */
    #maxConcurrentRequests = 0;
    /**
        * @readonly
        * @type {"duration"} 
    */
    #mode = "duration";
    /**
        * @readonly
        * @type {PrimaryListenerBuckets} 
    */
    #listenerBuckets;
    #isGrantingPermitsForPendingRequests = false;
    //
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
    async #grantPendingRequests() {
        if (this.#isGrantingPermitsForPendingRequests) return;
        this.#isGrantingPermitsForPendingRequests = true;
        try {
            while ( this.#requestsQueue.size > 0 ) {
                await this.#pruneGrantLog();
                // this.#requestsQueue.dequeue() can't be undefined while requestsQueue.size > 0
                const { clusterId, requestId } = /**@type {ClusterRequest}*/(this.#requestsQueue.dequeue());
                const worker = this.#clusters.get(clusterId);
                if (!worker) continue;
                const timestamp = performance.now();
                this.#grantTimestampSortedLog.enqueue(timestamp);
                this.#grantRequest(worker, {processingRequest: true, apiProviderId: this.#apiProviderId, requestId}, timestamp, 0);
            };
        } finally {
            this.#isGrantingPermitsForPendingRequests = false;
        };
    };
    /** @param {number} failedRequestTimestamp */
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
        * @param {Worker} worker
        * @param {{processingRequest: boolean, apiProviderId: string, requestId: number}} message 
        * @param {number} timestamp
        * @param {number} retryCount
    */
    #grantRequest(worker, message, timestamp, retryCount) {
        worker.send(message, (err) => {
            if (err) {
                if (worker.isDead()) {
                    this.#refundSlot(timestamp);
                    this.#handleWorkerExit(worker.id);
                } else {
                    if (retryCount < 3) {
                        setTimeout(() => this.#grantRequest(worker, message, timestamp, retryCount + 1), 50);
                    } else {
                        this.#refundSlot(timestamp);
                        triggerEvent(
                            this.#listenerBuckets.grantPermitFailure,
                            this.#listenerBuckets.error,
                            worker.id,
                            message.apiProviderId,
                            message.requestId,
                            err
                        )
                    };
                };
            };
        });
    };
    /** @param {number} workerId */
    #handleWorkerExit(workerId) {
        this.#clusters.delete(workerId);
        this.#grantPendingRequests();
    };
    /** 
        * @param {Worker} worker
        * @param {Object} msg 
    */
    #handleWorkerMessage(worker, msg) {
        if ('requestId' in msg && typeof msg.requestId === "number") {
            const requestId = msg.requestId;
            if ('askForToken' in msg && msg.askForToken === true) {
                this.#requestsQueue.enqueue({clusterId: worker.id, requestId});
                this.#clusters.set(worker.id, worker);
                this.#grantPendingRequests();
            };
        } else if ('acknowledgePrimary' in msg && msg.acknowledgePrimary === "pending") { 
            worker.send({apiProviderId: this.#apiProviderId, mode: this.#mode}) 
        };
    };
    /**
        * Creates a cluster-wide rate limiter.
        *
        * @param {string} apiProviderId
        * Unique identifier of the API provider whose requests are being limited.
        *
        * @param {number} maxConcurrentRequests
        * Maximum number of permits available.
        * 
        * @param {number} durationInSeconds
        * The length of the sliding request window in seconds.
        *
        * @throws {RateLimiterError}
        * If this class is instantiated from a process which is not the primary cluster process.
        *
        * @throws {RateLimiterError}
        * If `maxConcurrentRequests` is not a positive integer.
    */
    constructor(apiProviderId, maxConcurrentRequests, durationInSeconds) {
        // Validate Input Configuration
        validatePrimaryLimiterConstructorInput({mode: "duration", apiProviderId, maxConcurrentRequests, durationInSeconds});
        // Initialize Event Registry
        /** @type {PrimaryListenerBuckets} */
        const listenerBuckets = {
            grantPermitFailure: { on: new Set(), once: new Set() },
            error: { on: new Set(), once: new Set() }
        };
        super(listenerBuckets);
        // Assign Properties Values
        this.#apiProviderId = apiProviderId;
        this.#maxConcurrentRequests = maxConcurrentRequests;
        this.#rateLimitDurationInMilliSeconds = (durationInSeconds * 1000) + 50 /* safety jitter buffer */;
        this.#grantTimestampSortedLog = new Float64SortedQueue(maxConcurrentRequests);
        this.#listenerBuckets = listenerBuckets;
        // Register Limiter and Initiate IPC Listeners
        registerPrimaryLimiter(apiProviderId, {
            limiter: /**@type {PrimaryDurationLimiter}*/(this),
            config: `${maxConcurrentRequests}-${this.#mode}-${durationInSeconds}`,
            handleMessage: (worker, msg) => this.#handleWorkerMessage(worker, msg),
            handleExit: (workerId) => this.#handleWorkerExit(workerId)
        });
        // Execute Proactive Primary-Workers Handshake
        for (const workerId in cluster.workers) {
            const worker = cluster.workers[workerId];
            if (!worker) continue;
            worker.send({apiProviderId, mode: this.#mode});
        };
    };
};