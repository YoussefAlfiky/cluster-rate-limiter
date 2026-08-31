// @ts-check
import cluster from "node:cluster";
import { randomUUID } from "node:crypto";
import { RateLimiterError } from "./errors/RateLimiterError.js";
/**
    * @typedef {import("node:crypto").UUID} UUID
    * @typedef {{resolve: (requestId: UUID) => void, reject: (reason?: unknown) => void, timeout: NodeJS.Timeout}} PendingRequestTrackers
    * @typedef {{resolve: () => void, reject: (reason?: unknown) => void, timeout: NodeJS.Timeout}} PrimaryAcknowledgementTrackers
*/

/**
    * @typedef {Object} WorkerRegistryEntry
    * @property {WorkerRateLimiter_ClustersMode} limiter
    * @property {string} config
    * @property {(msg: Object) => void} handleMessage
    * 
    * @typedef {typeof globalThis & { [key: symbol]: Map<string, WorkerRegistryEntry> }} WorkerGlobalObject
*/

const API_PROVIDERS_REGISTRY_KEY = Symbol.for('WorkerRateLimiter_ClustersMode:registryMap');

/**
    * Acquires and releases request permits through the primary cluster process.
    *
    * This class is intended for use inside Node.js cluster workers and
    * communicates with {@link PrimaryRateLimiter_ClustersMode} through IPC.
    *
    * A worker does not maintain the global rate-limit state itself. Instead,
    * it requests a permit from the primary process and waits until the primary
    * process grants it.
    *
    * A permit request expires after `permitTimeoutDurationInSeconds` if the
    * primary process does not grant it.
    *
    * This class must be instantiated only in a cluster worker process.
*/
export class WorkerRateLimiter_ClustersMode {
    /** 
        * @readonly
        * @type {RateLimiterError} 
    */
    static #notAClusterWorker_Error = new RateLimiterError("ERR_NOT_WORKER", "WorkerRateLimiter_ClustersMode must run in a cluster worker.");
    /** 
        * @readonly
        * @type {string} 
    */
    #apiProviderId;
    /** 
        * @type {Promise<void>} 
    */
    #primaryAcknowledgementPromise;
    /** 
        * @readonly
        * @type {Map<UUID, PendingRequestTrackers>} 
    */
    #pendingRequests = new Map();
    /** 
        * @readonly
        * @type {number} 
    */
    #permitTimeoutDurationMs;
    /** 
        * @readonly
        * @type {"semaphore" | "duration"} 
    */
    #mode;
    #didCompletePrimaryHandshake = false;
    #didAcknowledgePrimaryMatch = false;
    /** @type {PrimaryAcknowledgementTrackers | null} */
    #primaryAcknowledgementTrackers = null;
    /** 
        * @returns {Promise<UUID>} 
    */
    #acquirePermit() {
        const requestId = randomUUID();
        return new Promise((resolve, reject) => {
            if (!process.send) {
                return reject(WorkerRateLimiter_ClustersMode.#notAClusterWorker_Error);
            };
            const timeout = setTimeout(() => {
                const pendingRequestTrackers = this.#pendingRequests.get(requestId);
                if (pendingRequestTrackers) {
                    const { reject } = pendingRequestTrackers;
                    this.#pendingRequests.delete(requestId);
                    reject(new RateLimiterError("ERR_PERMIT_TIMEOUT", `Timed out waiting for a permit for ${requestId} request.`));
                };
            }, this.#permitTimeoutDurationMs);
            this.#pendingRequests.set(requestId, {resolve, reject, timeout});
            const askForAToken = { 
                apiProviderId: this.#apiProviderId, 
                requestId, 
                askForToken: true
            };
            process.send(askForAToken, (err) => {
                if (err) {
                    this.#pendingRequests.delete(requestId);
                    clearTimeout(timeout);
                    reject(err);
                };
            });
        });
    };
    /** 
        * @param {UUID} requestId
        * @returns {Promise<void>}
    */
    #releasePermit(requestId) {
        return new Promise((resolve, reject) => {
            const message = {
                apiProviderId: this.#apiProviderId,
                requestId,
                terminateOneRequest: true
            };
            const sendPermitTermination = /** @param {Number} retryCount */(retryCount) => {
                if (!process.send) {
                    return reject(WorkerRateLimiter_ClustersMode.#notAClusterWorker_Error);
                };
                process.send(message, (err) => {
                    if (err) {
                        if (retryCount > 3) {
                            reject(err);
                        } else {
                            setTimeout(() => {
                                sendPermitTermination(retryCount + 1);
                            }, 50);
                        };
                    } else {
                        resolve();
                    };
                });
            };
            sendPermitTermination(0);
        });
    };
    /** 
        * @param {boolean} isFirstCall 
        * @returns {Promise<void>} 
    */
    #acknowledgePrimary(isFirstCall) {
        if (this.#didCompletePrimaryHandshake || this.#primaryAcknowledgementTrackers) {
            return this.#primaryAcknowledgementPromise;
        };
        this.#primaryAcknowledgementPromise = new Promise((resolve, reject) => {
            if (!process.send) {
                return reject(WorkerRateLimiter_ClustersMode.#notAClusterWorker_Error);
            };
            const timeout = setTimeout(() => {
                this.#primaryAcknowledgementTrackers = null;
                reject(
                    new RateLimiterError(
                        "ERR_PRIMARY_ACK_TIMEOUT",
                        `TimedOutAckPrimaryRateLimiter: The Primary Rate Limiter didn't acknowledge the worker ${isFirstCall ? "within 2000ms" : ""}. ` +
                        "Primary process maybe busy, crashed or didn't initialize its PrimaryRateLimiter_ClustersMode. " +
                        "Ensure PrimaryRateLimiter_ClustersMode is started in the primary process before spinning up workers."
                    )
                );
            }, 2000);
            this.#primaryAcknowledgementTrackers = {resolve, reject, timeout};
            const checkPrimaryLimiterMatch = {
                apiProviderId: this.#apiProviderId, 
                acknowledgePrimary: "pending"
            };
            process.send(checkPrimaryLimiterMatch, (err) => {
                if (err) {
                    this.#primaryAcknowledgementTrackers = null;
                    clearTimeout(timeout);
                    reject(err);
                };
            });
        });
        return this.#primaryAcknowledgementPromise;
    };
    /**@param {Object} msg */
    #handlePrimaryMessage(msg) {
        if (
            'processingRequest' in msg && msg.processingRequest === true 
            && 
            'requestId' in msg && typeof msg.requestId === "string"
        ) {
            const requestId = /** @type {UUID} */(msg.requestId);
            const trackers = this.#pendingRequests.get(requestId);
            if (!trackers) {
                if (this.#mode === "semaphore") {
                    this.#releasePermit(requestId).catch((err) => {
                        console.error(`Failed to release late permit for request ${requestId}:`, err);
                    });
                };
                return;
            };
            this.#pendingRequests.delete(requestId);
            clearTimeout(trackers.timeout);
            trackers.resolve(requestId);
        };
        if ( 'mode' in msg && (msg.mode === "semaphore" || msg.mode === "duration") ) {
            if (this.#didCompletePrimaryHandshake) return;
            this.#didCompletePrimaryHandshake = true;
            const modeMismatchError = new RateLimiterError("ERR_MODE_MISMATCH", [
                `RateLimiterMismatch: Mode mismatch for apiProviderId '${this.#apiProviderId}'.`,
                `Primary's Rate Limiter was initialized in '${msg.mode}' mode, while Worker's is trying to initialize in '${this.#mode}' mode.`,
                `Fix: The Worker's Rate Limiter must initialize in '${msg.mode}' mode to match the Primary.`
            ].join('\n'));
            if (!this.#primaryAcknowledgementTrackers) {
                if (msg.mode === this.#mode) {
                    this.#primaryAcknowledgementPromise = Promise.resolve();
                } else {
                    this.#primaryAcknowledgementPromise = Promise.reject(modeMismatchError);
                    this.#primaryAcknowledgementPromise.catch(() => {}); // Avoid temporary unhandled rejection
                };
                return;
            };
            const { resolve, reject, timeout } = this.#primaryAcknowledgementTrackers;
            clearTimeout(timeout);
            if (msg.mode === this.#mode) {
                resolve();
            } else {
                reject(modeMismatchError);
            };
        };
    };
    /**
        * Creates a worker-side rate limiter.
        *
        * @param {string} apiProviderId
        * Identifier of the API provider handled by the corresponding primary rate limiter.
        * 
        * @param {"semaphore" | "duration"} mode
        * Must match the mode initialized in the primary limiter ("semaphore" or "duration").
        *
        * @param {number} permitTimeoutDurationInSeconds
        * Maximum amount of time to wait for a permit before the request fails.
        *
        * @throws {RateLimiterError}
        * If this class is instantiated from the primary cluster process.
        *
        * @throws {RateLimiterError}
        * If `permitTimeoutDurationInSeconds` is not greater than zero.
    */
    constructor (apiProviderId, mode, permitTimeoutDurationInSeconds) {
        // Validations
        if (cluster.isPrimary) {
            throw new RateLimiterError("ERR_NOT_WORKER", "WorkerRateLimiter_ClustersMode must be instantiated only in the cluster worker process.");
        }
        if (typeof apiProviderId !== "string") {
            throw new RateLimiterError("ERR_INVALID_API_PROVIDER_ID", `Invalid apiProviderId: expected a string, but received ${typeof apiProviderId}.`);
        }
        if (mode !== "duration" && mode !== "semaphore") {
            throw new RateLimiterError("ERR_INVALID_MODE", `Invalid mode: expected 'semaphore' or 'duration', but received '${mode}'.`);
        }
        if (!Number.isFinite(permitTimeoutDurationInSeconds) || permitTimeoutDurationInSeconds <= 0) {
            throw new RateLimiterError(
                "ERR_INVALID_TIMEOUT_DURATION", 
                `Invalid permitTimeoutDurationInSeconds: must be a strictly positive finite number, but received ${typeof permitTimeoutDurationInSeconds === "number" ? permitTimeoutDurationInSeconds : typeof permitTimeoutDurationInSeconds}.`
            );
        }
        // Assign properties values 
        this.#mode = mode;
        this.#apiProviderId = apiProviderId;
        this.#permitTimeoutDurationMs = permitTimeoutDurationInSeconds * 1000;
        // Register Limiter and initiate listeners
        const globalObject = /**@type {WorkerGlobalObject}*/(globalThis);
        const limitersRegistry = globalObject[API_PROVIDERS_REGISTRY_KEY];
        const registryEntry = {
            limiter: this,
            config: `${mode}-${permitTimeoutDurationInSeconds}`,
            handleMessage: /**@param {Object} msg*/(msg) => this.#handlePrimaryMessage(msg)
        };
        if (!limitersRegistry) {
            globalObject[API_PROVIDERS_REGISTRY_KEY] = new Map([[apiProviderId, registryEntry]]);
            process.on("message", (msg) => {
                if (msg && typeof msg === "object" && 'apiProviderId' in msg && typeof msg.apiProviderId === "string") {
                    const entry = globalObject[API_PROVIDERS_REGISTRY_KEY].get(msg.apiProviderId);
                    if (entry) entry.handleMessage(msg);
                };
            })
        } else {
            if (limitersRegistry.has(apiProviderId)) {
                throw new RateLimiterError("ERR_DUPLICATE_WORKER_LIMITER", `DuplicateRateLimiter: A WorkerRateLimiter for apiProviderId '${apiProviderId}' has already been instantiated. You must share a single instance across your process.`);
            }
            limitersRegistry.set(apiProviderId, registryEntry);
        };
        // start worker-primary handshake
        this.#primaryAcknowledgementPromise = this.#acknowledgePrimary(true);
        this.#primaryAcknowledgementPromise.catch((reason) => console.warn(reason));
    };
    /**
        * Runs a request after acquiring a permit.
        *
        * Permit release behavior depends on the initialized `mode`:
        * - **Semaphore mode**: The permit is held until the caller's promise settles (resolves or rejects). 
        *   It occupies a concurrency slot for the entire duration of the request.
        * - **Duration mode**: The permit is consumed instantly upon grant. No release IPC message is sent 
        *   back to the primary process because capacity is naturally managed by the sliding time window.
        * 
        * If `releasePermit()` fails, its error takes precedence over an error from the caller.
        * 
        * @warning **Event Loop Blocking:**
            * This rate limiter relies on fast IPC messaging. Do not pass heavy, 
            * CPU-bound synchronous functions (e.g., massive JSON parsing or cryptography) 
            * directly as the caller. Doing so will freeze the Node.js event loop, delay IPC 
            * messages, and cause severe rate-limit drift. Offload heavy synchronous math 
            * to `node:worker_threads` to ensure healthy event loop performance.
        *
        * 
        * @template T
        * @typeParam T
        * The value produced by the caller's promise.
        *
        * @template {unknown[]} A
        * @typeParam A
        * The argument tuple accepted by the caller.
        *
        * @param {(...args: A) => T | Promise<T>} caller
        * Function that starts the request.
        *
        * @param {A} args
        * Arguments passed to `caller`.
        *
        * @returns {Promise<T>}
        * The promise returned by `caller`.
        *
        * @throws
        * An error if acquiring the permit fails, including a permit timeout.
        *
        * @throws
        * An error if releasing the permit fails. A release failure takes precedence
        * over an error produced by `caller`.
        *
        * @example
        * ```ts
        * const result = await rateLimiter.run(
        *     fetch,
        *     "https://api.example.com/data"
        * );
        * ```
    */
    async run(caller, ...args) {
        if (!this.#didAcknowledgePrimaryMatch) {
            try {
                await this.#primaryAcknowledgementPromise;
            } catch {
                // If the initial boot-time promise failed, try exactly once more.
                await this.#acknowledgePrimary(false);
            };
            this.#didAcknowledgePrimaryMatch = true;
        }
        //
        const requestId = await this.#acquirePermit();
        try {
            if (this.#mode === "duration") {
                return caller(...args);
            } else {
                return await caller(...args);
            };
        } finally {
            if (this.#mode === "semaphore") {
                await this.#releasePermit(requestId);
            };
        };
    };
};