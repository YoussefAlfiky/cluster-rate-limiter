//@ts-check
import cluster from "node:cluster";
import { Int32Queue } from "../../lib/Int32Queue.js";
import { registerLimiterByUniqueNumber } from "./registry/registerPrimaryLimiter.js";
import { IPC_SIGNATURE, IPC_OPCODES , IPC_INDEX } from "../shared/ipc-protocol.js";
import { _kTriggerGrantFailureEvent, BasePrimaryLimiter } from "./BasePrimaryLimiter.js";

/**
    * @typedef {import("node:cluster").Worker} Worker
    * @typedef {{clusterId: number, requestId: number}} ClusterRequest
*/
/**
    * Primary-side concurrency (semaphore-based) rate limiter.
    * 
    * Enforces a strict maximum number of active concurrent operations across the cluster. 
    * 
    * @extends {BasePrimaryLimiter}
*/
export class PrimarySemaphoreLimiter extends BasePrimaryLimiter {
    /** @readonly*/
    #requestsIdParallelQueue = new Int32Queue();
    /** @readonly*/
    #clustersIdParallelQueue = new Int32Queue();
    /** 
        * @readonly
        * @type {Map<number, Set<number>>}
    */
    #clusterPendingRequests = new Map();
    /** @type {number} */
    #remainingTokens = 0 /* temp value till init */;
    /**
        * Creates a semaphore-based cluster rate limiter.
        *
        * @param {string} apiProviderId Unique identifier for the target API (e.g., 'stripe-api').
        * @param {number} maxConcurrentRequests Maximum number of active concurrent permits allowed.
        *
        * @throws {RateLimiterError} If instantiated outside the primary cluster process.
        * @throws {RateLimiterError} If `maxConcurrentRequests` is not a positive integer.
    */
    constructor(apiProviderId, maxConcurrentRequests) {
        super(apiProviderId, "semaphore", maxConcurrentRequests, null);
        registerLimiterByUniqueNumber(this._apiUniqueNumber, {
            handleHotMessage: (worker, msg) => this.#handleHotMessage(worker, msg),
            handleExit: (workerId) => this.#handleWorkerExit(workerId)
        });
        // Assign Properties Values
        this.#remainingTokens = maxConcurrentRequests;
    };
    /** 
        * Transmits the concurrency token grant to the target worker via IPC.
        * Reclaims the leased token immediately if transmission fails permanently or the worker dies.
        * 
        * @param {Worker} worker The target Node.js cluster worker.
        * @param {number[]} message The high-performance integer array IPC payload.
        * @param {number} retryCount Current retry iteration (max 3).
    */
    #grantRequest(worker, message, retryCount) {
        try {
            worker.send(message, (err) => {
                if (err) {
                    if (worker.isDead()) {
                        this.#handleWorkerExit(worker.id);
                    } else {
                        if (retryCount < 3) {
                            setTimeout(() => this.#grantRequest(worker, message, retryCount + 1), 50);
                        } else {
                            const requestId = message[IPC_INDEX.PAYLOAD];
                            this.#decrementWorkerUsage(worker.id, requestId);
                            this[_kTriggerGrantFailureEvent](worker.id, requestId, err);
                        };
                    };
                };
            });
        } catch (syncErr) {
            if (worker.isDead()) {
                this.#handleWorkerExit(worker.id);
                return
            };
            const requestId = message[IPC_INDEX.PAYLOAD];
            this.#decrementWorkerUsage(worker.id, requestId);
            try {
                const normalizedError = syncErr instanceof Error ? syncErr : new Error(String(syncErr));
                this[_kTriggerGrantFailureEvent](worker.id, requestId, normalizedError);
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
        * Synchronously dispatches available tokens to queued requests in FIFO order.
    */
    #grantPendingRequests() {
        if (!cluster.workers) return;
        while ( this.#requestsIdParallelQueue.size > 0 ) {
            if (this.#remainingTokens <= 0) break;
            /* 
                this.#requestsIdParallelQueue.dequeue() and this.#clustersIdParallelQueue.dequeue()
                can't be undefined while requestsIdParallelQueue.size > 0 
            */
            const requestId = /**@type {number}*/(this.#requestsIdParallelQueue.dequeue());
            const clusterId = /**@type {number}*/(this.#clustersIdParallelQueue.dequeue());
            const worker = cluster.workers[clusterId];
            if (!worker || worker.isDead()) continue;
            this.#incrementWorkerUsage(clusterId, requestId);
            this.#grantRequest(worker, [IPC_SIGNATURE, this._apiUniqueNumber, IPC_OPCODES.GRANT, requestId], 0);
        };
    };
    /** 
        * Records a new token against the target worker and decrements available capacity.
        * 
        * @param {number} clusterId The ID of the worker receiving the permit.
        * @param {number} requestId The unique ID of the granted request.
    */
    #incrementWorkerUsage(clusterId, requestId) {
        const workerPendingRequestsSet = this.#clusterPendingRequests.get(clusterId);
        if (workerPendingRequestsSet) {
            workerPendingRequestsSet.add(requestId);
        } else {
            this.#clusterPendingRequests.set(clusterId, new Set([requestId]));
        };
        this.#remainingTokens--;
    };
    /** 
        * Releases a specific leased token from a worker, restores capacity, and triggers the queue.
        * Ignores duplicate or invalid release attempts safely.
        * 
        * @param {number} clusterId The ID of the worker releasing the permit.
        * @param {number} requestId The unique ID of the completed request.
    */
    #decrementWorkerUsage(clusterId, requestId) {
        const workerPendingRequestsSet = this.#clusterPendingRequests.get(clusterId);
        if (!workerPendingRequestsSet) return;
        if (workerPendingRequestsSet.delete(requestId)) {
            if (workerPendingRequestsSet.size === 0) this.#clusterPendingRequests.delete(clusterId);
            this.#remainingTokens++;
            this.#grantPendingRequests();
        };
    };
    /** 
        * Idempotently reclaims all active tokens leased to a disconnected or dead worker.
        * 
        * @param {number} clusterId The ID of the exited worker.
    */
    #reclaimDeadWorkerTokens(clusterId) {
        const workerPendingRequestsSet = this.#clusterPendingRequests.get(clusterId);
        this.#clusterPendingRequests.delete(clusterId);
        if (workerPendingRequestsSet) this.#remainingTokens += workerPendingRequestsSet.size;
    };
    /** @param {number} workerId */
    #handleWorkerExit(workerId) {
        this.#reclaimDeadWorkerTokens(workerId);
        this.#grantPendingRequests();
    };
    /** 
        * @param {Worker} worker
        * @param {number[]} msg 
    */
    #handleHotMessage(worker, msg) {
        const requestId = msg[IPC_INDEX.PAYLOAD];
        const opCode = msg[IPC_INDEX.OPCODE];
        if (opCode === IPC_OPCODES.ACQUIRE) {
            this.#requestsIdParallelQueue.enqueue(requestId);
            this.#clustersIdParallelQueue.enqueue(worker.id);
            this.#grantPendingRequests();
        } else if (opCode === IPC_OPCODES.RELEASE) {
            this.#decrementWorkerUsage(worker.id, requestId);
        };
    };
};