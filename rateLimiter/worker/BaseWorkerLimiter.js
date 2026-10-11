// @ts-check
import { RateLimiterError } from "../errors/RateLimiterError.js";
import { validateWorkerLimiter } from "./validators/validateWorkerLimiter.js";
import { registerLimiterByProviderId, registerLimiterByUniqueNumber } from "./registry/workerLimiterRegistry.js";
import { IPC_SIGNATURE, IPC_OPCODES, IPC_INDEX } from "../shared/ipc-protocol.js";
import { emitSafeError, triggerEvent } from "../events/triggerEvent.js";
/**
    * @typedef {{resolve: (requestId: number) => void, reject: (reason?: unknown) => void, timeout: NodeJS.Timeout}} PendingRequestTrackers
    * @typedef {{resolve: () => void, reject: (reason?: unknown) => void, timeout: NodeJS.Timeout}} PrimaryAcknowledgementTrackers
*/
/**
    * @typedef {(apiProviderId: string, requestId: number, err: unknown) => void | Promise<void>} ReleaseFailureCallback
    * @typedef {(err: unknown) => void | Promise<void>} ErrorCallback
    * 
    * @typedef {Object} ReleasePermitListenerBuckets
    * @property {{ on: Set<ReleaseFailureCallback>, once: Set<ReleaseFailureCallback> }} releasePermitFailure
    * @property {{ on: Set<ErrorCallback>, once: Set<ErrorCallback> }} error
*/
export const _kAcquirePermit = Symbol("kAcquirePermit");
export const _kReleasePermit = Symbol("kReleasePermit");

/**
    * Abstract base class for cluster worker-side rate limiters.
    * 
    * This class establishes and manages the foundational IPC (Inter-Process Communication) 
    * pipeline between the worker process and the primary process. It handles the initial 
    * boot-time handshake, safely routes incoming capacity permits to the correct pending 
    * requests, and traps orphaned permits caused by network timeouts.
    * 
    * **Performance Details:**
    * To prevent `MaxListenersExceededWarning` and O(N) event loop degradation, this class 
    * uses a global singleton registry via `Symbol.for`. It guarantees that no matter how 
    * many rate limiters are instantiated across the application, exactly one 
    * `process.on("message")` listener is ever attached.
    * 
    * @abstract
*/
export class BaseWorkerLimiter {
    /** 
        * @readonly
        * @type {string} 
    */
    #apiProviderId;
    #apiUniqueNumber = 0 /* temp value till handshake */;
    /** 
        * @readonly
        * @type {Map<number, PendingRequestTrackers>} 
    */
    #pendingRequests = new Map();
    /** 
        * @readonly
        * @type {"semaphore" | "duration"} 
    */
    #mode;
    /** @type {Promise<void>} */
    #primaryAcknowledgementPromise;
    /** @type {PrimaryAcknowledgementTrackers | null} */
    #primaryAcknowledgementTrackers = null;
    #didCompletePrimaryHandshake = false;
    #didAcknowledgePrimaryMatch = false;
    /** 
        * @readonly
        * @type {number} 
    */
    #permitTimeoutDurationMs = 0 /* temp value till init */;
    /**
        * @readonly
        * @type {ReleasePermitListenerBuckets | null} 
    */
    #releasePermitListenerBuckets;
    /**@readonly */
    #MIN_SMI = -(1 << 30);
    /**@readonly */
    #MAX_SMI =  (1 << 30) - 1;
    #requestId = this.#MIN_SMI;
    /**
        * Initializes the foundational state, validations, and IPC routing for a worker-side rate limiter.
        *
        * This constructor establishes the global registry entry and begins the background handshake. 
        * It is designed strictly to be invoked via `super()` from derived concrete subclasses.
        *
        * @param {string} apiProviderId
        * The unique identifier for the target API. This must perfectly match the ID registered 
        * by the rate limiter in the primary process.
        * 
        * @param {"semaphore" | "duration"} mode
        * The operational rate-limiting strategy. Must match the strategy initialized in the primary process.
        * 
        * @param {number} permitTimeoutDurationInSeconds
        * Maximum duration (in seconds) a request will wait in the queue for a primary permit before timing out.
        * 
        * @param {ReleasePermitListenerBuckets | null} releasePermitListenerBuckets
        * Injected event buckets used exclusively by 'semaphore' limiters to report 
        * network anomalies during background permit releases. Must be `null` for 'duration' limiters.
        * 
        * @throws {Error}
        * If invoked directly with `new BaseWorkerLimiter()` (enforces abstract class behavior).
        * 
        * @throws {RateLimiterError}
        * If instantiated within the primary process (strictly worker-only).
        * 
        * @throws {RateLimiterError}
        * If a worker rate limiter for the provided `apiProviderId` has already been registered in this 
        * process (enforces singleton pattern per API provider).
        * 
        * @throws {RateLimiterError}
        * If any configuration parameter is malformed, missing, or out of bounds.
    */
    constructor (apiProviderId, mode, permitTimeoutDurationInSeconds, releasePermitListenerBuckets) {
        // Validations
        if (new.target === BaseWorkerLimiter) {
            throw new Error("BaseWorkerLimiter is an abstract class and cannot be instantiated directly.");
        };
        validateWorkerLimiter(apiProviderId, mode, permitTimeoutDurationInSeconds, releasePermitListenerBuckets);
        // Assign properties values 
        this.#mode = mode;
        this.#apiProviderId = apiProviderId;
        this.#permitTimeoutDurationMs = permitTimeoutDurationInSeconds * 1000;
        this.#releasePermitListenerBuckets = releasePermitListenerBuckets;
        // Register Limiter and initiate IPC listeners
        registerLimiterByProviderId(apiProviderId, {
            limiter: this,
            config: `${mode}-${permitTimeoutDurationInSeconds}`,
            handleColdMessage: (message) => this.#handleColdMessage(message)
        });
        // start worker-primary handshake
        this.#primaryAcknowledgementPromise = this.#acknowledgePrimary(true);
        this.#primaryAcknowledgementPromise.catch(() => {});
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
        * Ensures the primary process has acknowledged and validated this worker's rate limiter configuration.
        * 
        * The connection handshake initiates automatically in the background upon instantiation. 
        * If the initial boot-time handshake fails (e.g., due to temporary IPC congestion or delayed 
        * initialization on the primary), calling this method triggers exactly one retry attempt.
        * 
        * **Note:** The `.run()` method invokes this automatically to guarantee safe execution. 
        * However, you can call this manually during application startup to fail-fast if the 
        * configuration is invalid.
        * 
        * @returns {Promise<boolean>}
        * Resolves to `true` once the primary process confirms successful registration.
        * 
        * @throws {RateLimiterError}
        * If the primary process times out during the retry, or if a fatal configuration mismatch 
        * occurs (e.g., initializing a worker in `'semaphore'` mode while the primary expects `'duration'`).
    */
    async ready() {
        try {
            await this.#primaryAcknowledgementPromise;
        } catch {
            // If the initial boot-time promise failed, try exactly once more.
            await this.#acknowledgePrimary(false);
        };
        return true;
    };
    /**
        * Acquires a capacity permit from the primary process via IPC.
        * 
        * This method manages the complete acquisition lifecycle: it guarantees the initial 
        * IPC handshake is settled, generates a high-performance V8 SMI request ID, registers 
        * the timeout traps, and dispatches the hot-path `ACQUIRE` message.
        * 
        * **Subclass Usage:** 
        * The resolved `requestId` is crucial. While "duration" limiters can safely ignore it, 
        * "semaphore" limiters **must** capture this ID and pass it to `[_kReleasePermit]` 
        * once the user's workload settles.
        * 
        * @protected
        * @async
        * @returns {Promise<number>} Resolves with the unique local `requestId` assigned to this permit.
        * 
        * @throws {RateLimiterError} 
        * - `ERR_IPC_CHANNEL_CLOSED`: If the worker is orphaned or the IPC channel drops before sending.
        * - `ERR_PERMIT_TIMEOUT`: If the primary process does not grant capacity within the configured window.
        * - `ERR_MODE_MISMATCH`: If the lazy handshake fails due to a configuration mismatch.
    */
    async [_kAcquirePermit]() {
        if (!this.#didAcknowledgePrimaryMatch) {
            await this.ready();
            this.#didAcknowledgePrimaryMatch = true;
        };
        //
        if (this.#requestId === this.#MAX_SMI) { 
            this.#requestId = this.#MIN_SMI;
        } else {
            this.#requestId++;
        };
        const reqId = this.#requestId;
        //
        return new Promise((resolve, reject) => {
            if (!process.send) {
                return reject(new RateLimiterError(
                    "ERR_IPC_CHANNEL_CLOSED", 
                    "Cannot acquire permit: The IPC channel is closed. The worker may be disconnected or orphaned."
                ));
            };
            const timeout = setTimeout(() => {
                const pendingRequestTrackers = this.#pendingRequests.get(reqId);
                if (pendingRequestTrackers) {
                    const { reject } = pendingRequestTrackers;
                    this.#pendingRequests.delete(reqId);
                    reject(new RateLimiterError("ERR_PERMIT_TIMEOUT", `Timed out waiting for a permit for ${reqId} request.`));
                };
            }, this.#permitTimeoutDurationMs);
            this.#pendingRequests.set(reqId, {resolve, reject, timeout});
            process.send([IPC_SIGNATURE, this.#apiUniqueNumber, IPC_OPCODES.ACQUIRE, reqId], (err) => {
                if (err) {
                    this.#pendingRequests.delete(reqId);
                    clearTimeout(timeout);
                    reject(err);
                };
            });
        });
    };
    /**
        * Asynchronously dispatches a permit release signal to the primary process.
        * 
        * This is a "fire-and-forget" background operation designed to execute without blocking 
        * the subclass's resolution cycle. It includes a built-in micro-retry mechanism to handle 
        * transient IPC congestion. 
        * 
        * If the release ultimately fails (e.g., due to a closed IPC channel or max retries exceeded), 
        * it silently swallows the V8 exception and pipes the failure to the registered 
        * `releasePermitListenerBuckets` for developer visibility.
        * 
        * **Subclass Usage:**
        * Should only be invoked by "semaphore" mode subclasses inside a `finally` block to 
        * guarantee the concurrency slot is released regardless of the workload's outcome.
        * 
        * @protected
        * @param {number} requestId The unique local ID of the permit to release.
        * @returns {void}
    */
    [_kReleasePermit](requestId) {
        /**@param {Error} err */
        const reportError = (err) => {
            if (!this.#releasePermitListenerBuckets) return;
            const { releasePermitFailure, error: errorListeners } = this.#releasePermitListenerBuckets;
            if (!triggerEvent(releasePermitFailure, errorListeners, this.#apiProviderId, requestId, err)) {
                emitSafeError(errorListeners, err);
            };
        };
        const message = [IPC_SIGNATURE, this.#apiUniqueNumber, IPC_OPCODES.RELEASE, requestId];
        const sendPermitTermination = /** @param {number} retryCount */(retryCount) => {
            if (!process.send) {
                reportError(new RateLimiterError(
                    "ERR_IPC_CHANNEL_CLOSED", 
                    "Cannot release permit: The IPC channel is closed. The worker may be disconnected or orphaned."
                ));
                return;
            };
            try {
                process.send(message, (err) => {
                    if (err) {
                        if (retryCount > 3) {
                            reportError(err);
                        } else {
                            setTimeout(() => {
                                sendPermitTermination(retryCount + 1);
                            }, 50);
                        };
                    }
                });
            } catch (err) {
                try {
                    const normalizedError = err instanceof Error ? err : new Error(String(err));
                    reportError(normalizedError);
                } catch {
                    process.emitWarning(
                        "Unknown error",
                        "RateLimiterWarning",
                        "An 'error' listener threw a synchronous exception."
                    );
                }
            };
        };
        sendPermitTermination(0);
    };
    /** 
        * @param {boolean} isFirstCall 
        * @returns {Promise<void>} 
    */
    #acknowledgePrimary(isFirstCall) {
        if (this.#didCompletePrimaryHandshake || this.#primaryAcknowledgementTrackers) {
            return this.#primaryAcknowledgementPromise;
        };
        return this.#primaryAcknowledgementPromise = new Promise((resolve, reject) => {
            if (!process.send) {
                return reject(new RateLimiterError(
                    "ERR_IPC_CHANNEL_CLOSED", 
                    "Cannot complete primary handshake: The IPC channel is closed. The worker may be disconnected or orphaned."
                ));
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
            process.send({apiProviderId: this.#apiProviderId, acknowledgePrimary: "pending"}, (err) => {
                if (err) {
                    this.#primaryAcknowledgementTrackers = null;
                    clearTimeout(timeout);
                    reject(err);
                };
            });
        });
    };
    /**@param {number[]} msg */
    #handleHotMessage(msg) {
        const opCode = msg[IPC_INDEX.OPCODE];
        if (opCode === IPC_OPCODES.GRANT) {
            const requestId = msg[IPC_INDEX.PAYLOAD];
            const trackers = this.#pendingRequests.get(requestId);
            if (!trackers) {
                if (this.#mode === "semaphore") {
                    this[_kReleasePermit](requestId);
                }
                return;
            };
            this.#pendingRequests.delete(requestId);
            clearTimeout(trackers.timeout);
            trackers.resolve(requestId);
        };
    };
    /**@param {Object} msg */
    #handleColdMessage(msg) {
        if ( 
            'mode' in msg && (msg.mode === "semaphore" || msg.mode === "duration") 
            && 'apiUniqueNumber' in msg && typeof msg.apiUniqueNumber === "number"
        ) {
            if (this.#didCompletePrimaryHandshake) return;
            this.#didCompletePrimaryHandshake = true;
            const modeMismatchError = new RateLimiterError("ERR_MODE_MISMATCH", [
                `RateLimiterMismatch: Mode mismatch for apiProviderId '${this.#apiProviderId}'.`,
                `Primary's Rate Limiter was initialized in '${msg.mode}' mode, while Worker's is trying to initialize in '${this.#mode}' mode.`,
                `Fix: The Worker's Rate Limiter must initialize in '${msg.mode}' mode to match the Primary.`
            ].join('\n'));
            if (!this.#primaryAcknowledgementTrackers) {
                if (msg.mode === this.#mode) {
                    registerLimiterByUniqueNumber(msg.apiUniqueNumber, {handleHotMessage: (message) => this.#handleHotMessage(message)});
                    this.#apiUniqueNumber = msg.apiUniqueNumber;
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
                registerLimiterByUniqueNumber(msg.apiUniqueNumber, {handleHotMessage: (message) => this.#handleHotMessage(message)});
                this.#apiUniqueNumber = msg.apiUniqueNumber;
                resolve();
            } else {
                reject(modeMismatchError);
            };
        };
    };
};