// @ts-check
import { RateLimiterError } from "../errors/RateLimiterError.js";
import { validateWorkerLimiter } from "./validators/validateWorkerLimiter.js";
import { registerWorkerLimiter, registerLimiterApiUniqueNumber } from "./registry/registerWorkerLimiter.js";
import { IPC_OPCODES, IPC_INDEX } from "../shared/ipc-protocol.js";
/**
    * @typedef {{resolve: (requestId: number) => void, reject: (reason?: unknown) => void, timeout: NodeJS.Timeout}} PendingRequestTrackers
    * @typedef {{resolve: () => void, reject: (reason?: unknown) => void, timeout: NodeJS.Timeout}} PrimaryAcknowledgementTrackers
*/

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
    #pendingRequests;
    /** 
        * @readonly
        * @type {"semaphore" | "duration"} 
    */
    #mode;
    /** @type {Promise<void>} */
    #primaryAcknowledgementPromise;
    /** @type {PrimaryAcknowledgementTrackers | null} */
    #primaryAcknowledgementTrackers = null;
    /** @type {( (apiUniqueNumber: number, requestId: number) => void ) | null}*/
    #onUnclaimedPermit;
    #didCompletePrimaryHandshake = false;
    #permitTimeoutDurationInSeconds = 0 /* temp value till init */;
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
    /**@param {Object} msg */
    #handlePrimaryMessage(msg) {
        if (Array.isArray(msg)) {
            const opCode = msg[IPC_INDEX.OPCODE];
            if (opCode === IPC_OPCODES.GRANT) {
                const requestId = msg[IPC_INDEX.PAYLOAD];
                const trackers = this.#pendingRequests.get(requestId);
                if (!trackers) {
                    // this.#onUnclaimedPermit is always null in duration mode
                    if (this.#onUnclaimedPermit) {
                        this.#onUnclaimedPermit(this.#apiUniqueNumber, requestId);
                    };
                    return;
                };
                this.#pendingRequests.delete(requestId);
                clearTimeout(trackers.timeout);
                trackers.resolve(requestId);
            };
            return;
        };
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
                    registerLimiterApiUniqueNumber(
                        msg.apiUniqueNumber, 
                        {
                            limiter: this,
                            config: `${this.#mode}-${this.#permitTimeoutDurationInSeconds}`,
                            handleMessage: (message) => this.#handlePrimaryMessage(message)
                        }
                    );
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
                registerLimiterApiUniqueNumber(
                    msg.apiUniqueNumber, 
                    {
                        limiter: this,
                        config: `${this.#mode}-${this.#permitTimeoutDurationInSeconds}`,
                        handleMessage: (message) => this.#handlePrimaryMessage(message)
                    }
                );
                this.#apiUniqueNumber = msg.apiUniqueNumber;
                resolve();
            } else {
                reject(modeMismatchError);
            };
        };
    };
    /** @protected */
    get _apiUniqueNumber() {
        return this.#apiUniqueNumber;
    };
    get apiProviderId() {
        return this.#apiProviderId;
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
        * @param {Map<number, PendingRequestTrackers>} pendingRequests
        * Shared map utilized by subclasses to track pending requests awaiting primary acknowledgment.
        * 
        * @param {( (apiUniqueNumber: number, requestId: number) => void ) | null} onUnclaimedPermit
        * Injected callback to safely release permits that arrive after their local request has timed out. 
        * Required for 'semaphore' mode to release the lock, but explicitly `null` for 'duration' mode.
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
    constructor (apiProviderId, mode, permitTimeoutDurationInSeconds, pendingRequests, onUnclaimedPermit) {
        // Validations
        if (new.target === BaseWorkerLimiter) {
            throw new Error("BaseWorkerLimiter is an abstract class and cannot be instantiated directly.");
        };
        validateWorkerLimiter(apiProviderId, mode, permitTimeoutDurationInSeconds, pendingRequests, onUnclaimedPermit);
        // Assign properties values 
        this.#mode = mode;
        this.#apiProviderId = apiProviderId;
        this.#pendingRequests = pendingRequests;
        this.#onUnclaimedPermit = onUnclaimedPermit;
        this.#permitTimeoutDurationInSeconds = permitTimeoutDurationInSeconds;
        // Register Limiter and initiate IPC listeners
        registerWorkerLimiter(apiProviderId, {
            limiter: this,
            config: `${mode}-${permitTimeoutDurationInSeconds}`,
            handleMessage: (message) => this.#handlePrimaryMessage(message)
        });
        // start worker-primary handshake
        this.#primaryAcknowledgementPromise = this.#acknowledgePrimary(true);
        this.#primaryAcknowledgementPromise.catch(() => {});
    };
};