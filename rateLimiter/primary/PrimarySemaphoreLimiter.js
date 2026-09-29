//@ts-check
import cluster from "node:cluster";
import { Queue } from "../../lib/Queue.js";
import { validatePrimaryLimiterConstructorInput } from "./validators/validatePrimaryLimiter.js";
import { registerPrimaryLimiter } from "./registry/registerPrimaryLimiter.js";
import { EventListenerRegistry } from "../events/EventListenerRegistry.js";
import { triggerEvent } from "../events/triggerEvent.js"

/**
    * @typedef {import("node:cluster").Worker} Worker
    * @typedef {import("node:crypto").UUID} UUID
    * @typedef {{clusterId: number, requestId: UUID}} ClusterRequest
*/
/**
    * @typedef {(workerId: number, apiProviderId: string, requestId: UUID, err: unknown) => void | Promise<void>} GrantPermitFailureCallback
    * @typedef {(err: unknown) => void | Promise<void>} ErrorCallback
    * 
    * @typedef {Object} PrimaryListenerBuckets
    * @property {{ on: Set<GrantPermitFailureCallback>, once: Set<GrantPermitFailureCallback> }} grantPermitFailure
    * @property {{ on: Set<ErrorCallback>, once: Set<ErrorCallback> }} error
*/

/** 
    * @extends {EventListenerRegistry<PrimaryListenerBuckets>}
*/
export class PrimarySemaphoreLimiter extends EventListenerRegistry {
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
        * @type {"semaphore"} 
    */
    #mode = "semaphore";
    /** 
        * @readonly
        * @type {Map<number, Set<UUID>>}
    */
    #clusterPendingRequests = new Map();
    /**
        * @readonly
        * @type {PrimaryListenerBuckets} 
    */
    #listenerBuckets;
    /** @type {number} */
    #remainingTokens = 0;
    /** 
        * @param {Worker} worker
        * @param {{processingRequest: boolean, apiProviderId: string, requestId: UUID}} message 
        * @param {number} retryCount
    */
    #grantRequest(worker, message, retryCount) {
        worker.send(message, (err) => {
            if (err) {
                if (worker.isDead()) {
                    this.#handleWorkerExit(worker.id);
                } else {
                    if (retryCount < 3) {
                        setTimeout(() => this.#grantRequest(worker, message, retryCount + 1), 50);
                    } else {
                        this.#decrementWorkerUsage(worker.id, message.requestId);
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
    #grantPendingRequests() {
        while ( this.#requestsQueue.size > 0 ) {
            if (this.#remainingTokens <= 0) break;
            // this.#requestsQueue.dequeue() can't be undefined while requestsQueue.size > 0
            const { clusterId, requestId } = /**@type {ClusterRequest}*/(this.#requestsQueue.dequeue());
            const worker = this.#clusters.get(clusterId);
            if (!worker) continue;
            this.#incrementWorkerUsage(clusterId, requestId);
            this.#grantRequest(worker, {processingRequest: true, apiProviderId: this.#apiProviderId, requestId}, 0);
        };
    };
    /** 
        * @param {number} clusterId
        * @param {UUID} requestId 
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
        * @param {number} clusterId
        * @param {UUID} requestId 
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
    /** @param {number} clusterId */
    #reclaimDeadWorkerTokens(clusterId) {
        const workerPendingRequestsSet = this.#clusterPendingRequests.get(clusterId);
        this.#clusterPendingRequests.delete(clusterId);
        if (workerPendingRequestsSet) this.#remainingTokens += workerPendingRequestsSet.size;
    };
    /** @param {number} workerId */
    #handleWorkerExit(workerId) {
        this.#reclaimDeadWorkerTokens(workerId);
        this.#clusters.delete(workerId);
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
            } else if ('terminateOneRequest' in msg && msg.terminateOneRequest === true) {
                const requestId = /**@type {UUID}*/(msg.requestId);
                this.#decrementWorkerUsage(worker.id, requestId);
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
        * @throws {RateLimiterError}
        * If this class is instantiated from a process which is not the primary cluster process.
        *
        * @throws {RateLimiterError}
        * If `maxConcurrentRequests` is not a positive integer.
    */
    constructor(apiProviderId, maxConcurrentRequests) {
        // Validate Input Configuration
        validatePrimaryLimiterConstructorInput({mode: "semaphore", apiProviderId, maxConcurrentRequests});
        // Initialize Event Registry
        /** @type {PrimaryListenerBuckets} */
        const listenerBuckets = {
            grantPermitFailure: { on: new Set(), once: new Set() },
            error: { on: new Set(), once: new Set() }
        };
        super(listenerBuckets);
        // Assign Properties Values
        this.#apiProviderId = apiProviderId;
        this.#remainingTokens = maxConcurrentRequests;
        this.#listenerBuckets = listenerBuckets;
        // Register Limiter and Initiate IPC Listeners
        registerPrimaryLimiter(apiProviderId, {
            limiter: /**@type {PrimarySemaphoreLimiter}*/(this),
            config: `${maxConcurrentRequests}-${this.#mode}-${undefined}`,
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