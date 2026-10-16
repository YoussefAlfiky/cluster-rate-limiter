//@ts-check
import cluster from "node:cluster";
import { EventListenerRegistry } from "../events/EventListenerRegistry.js";
import { triggerEvent, emitSafeError } from "../events/triggerEvent.js";
import { validatePrimaryLimiter } from "./validators/validatePrimaryLimiter.js";
import { generateApiUniqueNumber, registerLimiterByProviderId } from "./registry/registerPrimaryLimiter.js";
import { RateLimiterError } from "../errors/RateLimiterError.js";

/**
    * @typedef {import("node:cluster").Worker} Worker 
    * 
    * @typedef {(workerId: number, apiProviderId: string, requestId: number, err: unknown) => void | Promise<void>} GrantPermitFailureCallback
    * @typedef {(err: unknown) => void | Promise<void>} ErrorCallback
    * 
    * @typedef {Object} PrimaryListenerBuckets
    * @property {{ on: Set<GrantPermitFailureCallback>, once: Set<GrantPermitFailureCallback> }} grantPermitFailure
    * @property {{ on: Set<ErrorCallback>, once: Set<ErrorCallback> }} error
*/

/**
    * Protected symbol used to trigger grant failure events internally 
    * without exposing the emission method to the public API.
*/
export const _kTriggerGrantFailureEvent = Symbol("kTriggerGrantFailureEvent");

/** 
    * Abstract base class for all primary-process cluster rate limiters.
    * 
    * Orchestrates shared cluster infrastructure, including configuration validation, 
    * unique integer ID assignment, event listener registration (`grantPermitFailure` and `error`), 
    * and initial cold-path IPC handshakes with worker processes.
    * 
    * @abstract
    * @extends {EventListenerRegistry<PrimaryListenerBuckets>}
*/
export class BasePrimaryLimiter extends EventListenerRegistry {
    /**
        * @readonly
        * @type {number} 
    */
    #apiUniqueNumber = generateApiUniqueNumber();
    /** 
        * @readonly
        * @type {string}
    */
    #apiProviderId;
    /**
        * @readonly
        * @type {"semaphore" | "duration"} 
    */
    #mode;
    /**
        * @readonly
        * @type {PrimaryListenerBuckets} 
    */
    #listenerBuckets;
    /**
        * Abstract base constructor for primary-side rate limiters.
        * Validates configuration, initializes the event registry, and attaches the IPC cold-path router.
        *
        * @param {string} apiProviderId Unique identifier for the target API (e.g., 'stripe-api').
        * @param {"semaphore" | "duration"} mode The rate-limiting strategy to enforce.
        * @param {number} maxConcurrentRequests Maximum number of permits or active tokens allowed.
        * @param {number | null} durationInSeconds Sliding window duration in seconds (must be `null` for semaphore mode).
        * @throws {Error} If instantiated directly (class is abstract).
        * @throws {RateLimiterError} If arguments are invalid or the provider ID is already registered.
    */
    constructor(apiProviderId, mode, maxConcurrentRequests, durationInSeconds) {
        // constructor's input validation
        if (new.target === BasePrimaryLimiter) {
            throw new Error("BasePrimaryLimiter is an abstract class and cannot be instantiated directly.");
        };
        validatePrimaryLimiter(apiProviderId, mode, maxConcurrentRequests, durationInSeconds);
        // Initialize Event Registry
        /** @type {PrimaryListenerBuckets} */
        const listenerBuckets = {
            grantPermitFailure: { on: new Set(), once: new Set() },
            error: { on: new Set(), once: new Set() }
        };
        super(listenerBuckets);
        //
        registerLimiterByProviderId(apiProviderId, {
            limiter: this,
            config: `${maxConcurrentRequests}-${mode}-${durationInSeconds}`,
            handleColdMessage: (worker, msg) => this.#handleColdMessage(worker, msg)
        });
        // Assign Property values
        this.#listenerBuckets = listenerBuckets;
        this.#apiProviderId = apiProviderId;
        this.#mode = mode;
        // Execute Proactive Primary-Workers Handshake
        for (const workerId in cluster.workers) {
            const worker = cluster.workers[workerId];
            if (!worker) continue;
            worker.send({apiProviderId, mode, apiUniqueNumber: this.#apiUniqueNumber});
        };
    };
    /** @returns {string} */
    get apiProviderId() {
        return this.#apiProviderId;
    };
    /** @returns {"semaphore" | "duration"} */
    get mode() {
        return this.#mode;
    };
    /**
        * @protected
        * @returns {number} 
    */
    get _apiUniqueNumber() {
        return this.#apiUniqueNumber;
    };
    /** 
        * @protected
        * @param {number} workerId 
        * @param {number} requestId 
        * @param {unknown} err
        * @returns {void}
    */
    [_kTriggerGrantFailureEvent](workerId, requestId, err) {
        if (!triggerEvent(
            this.#listenerBuckets.grantPermitFailure,
            this.#listenerBuckets.error,
            workerId,
            this.#apiProviderId,
            requestId,
            err
        )) {
            emitSafeError(this.#listenerBuckets.error, err);
        };
    };
    /** 
        * Processes initial handshakes from newly booted workers. 
        * Broadcasts the primary configuration and integer ID back to the worker 
        * so it can upgrade itself to the O(1) fast-path routing map.
        * 
        * @param {Worker} worker The cluster worker requesting initialization.
        * @param {Object} msg The cold-path JSON payload.
    */
    #handleColdMessage(worker, msg) {
        if ('acknowledgePrimary' in msg && msg.acknowledgePrimary === "pending") {
            try {
                worker.send({
                    apiProviderId: this.#apiProviderId, 
                    mode: this.#mode, 
                    apiUniqueNumber: this.#apiUniqueNumber
                });
            } catch (syncErr) {
                if (worker.isDead()) return; 
                try {
                    const normalizedError = syncErr instanceof Error ? syncErr : new Error(String(syncErr));
                    const handshakeError = new RateLimiterError(
                        "ERR_HANDSHAKE_FAILED",
                        `Failed to send boot configuration to worker ${worker.id}. The worker likely disconnected or crashed. Details: ${normalizedError.message}`
                    );
                    handshakeError.cause = normalizedError;
                    emitSafeError(this.#listenerBuckets.error, handshakeError);
                } catch {
                    process.emitWarning(
                        `Failed to route handshake error for worker ${worker.id}. The error object may be corrupted.`,
                        "RateLimiterWarning",
                        "A critical exception was swallowed during worker cold-path registration."
                    );
                }
            };
        };
    };
};