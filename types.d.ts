// --- PUBLIC CONFIG TYPES ---

export interface PrimarySemaphoreConfig {
    /** Unique identifier for the API. */
    apiProviderId: string;
    /** Max active permits. */
    maxConcurrentRequests: number;
    /** "semaphore" for concurrency. */
    mode: "semaphore";
}

export interface PrimaryDurationConfig {
    /** Unique identifier for the API. */
    apiProviderId: string;
    /** Requests per duration limit. */
    maxConcurrentRequests: number;
    /** "duration" for time-window limits. */
    mode: "duration";
    /** Required: The length of the sliding request window. */
    durationInSeconds: number;
}

export type PrimaryConfig = PrimarySemaphoreConfig | PrimaryDurationConfig;

export interface WorkerSemaphoreConfig {
    /** Unique identifier for the API. */
    apiProviderId: string;
    /** Must match the Primary's mode. */
    mode: "semaphore";
    /** Max time to wait for IPC token. */
    permitTimeoutDurationInSeconds: number;
}

export interface WorkerDurationConfig {
    /** Unique identifier for the API. */
    apiProviderId: string;
    /** Must match the Primary's mode. */
    mode: "duration";
    /** Max time to wait for IPC token. */
    permitTimeoutDurationInSeconds: number;
}

export type WorkerConfig = WorkerSemaphoreConfig | WorkerDurationConfig;


// --- INTERNAL BASE INTERFACES (Not exported, keeps types DRY) ---

interface BasePrimaryLimiter {
    /** The unique identifier for the target API. */
    readonly apiProviderId: string;

    /** Subscribes to an event continuously. */
    on(event: "error", listener: (err: Error | unknown) => void | Promise<void>): void;
    on(event: "grantPermitFailure", listener: (workerId: number, apiProviderId: string, requestId: number, err: Error | unknown) => void | Promise<void>): void;

    /** Subscribes to an event for a single trigger. */
    once(event: "error", listener: (err: Error | unknown) => void | Promise<void>): void;
    once(event: "grantPermitFailure", listener: (workerId: number, apiProviderId: string, requestId: number, err: Error | unknown) => void | Promise<void>): void;

    /** Removes a specific active event listener. */
    off(event: "error", listener: (err: Error | unknown) => void | Promise<void>): void;
    off(event: "grantPermitFailure", listener: (workerId: number, apiProviderId: string, requestId: number, err: Error | unknown) => void | Promise<void>): void;

    /** 
     * Clears all registered event listeners.
     * Essential for cleanup during graceful shutdowns or test teardowns.
     */
    clearListeners(): void;
}

interface BaseWorkerLimiter {
    /** The unique identifier for the target API. */
    readonly apiProviderId: string;

    /** 
     * Resolves when the worker has successfully completed its IPC handshake 
     * with the primary process and is ready to accept permits.
     */
    ready(): Promise<true>;

    /** Executes the target function while enforcing cluster-wide rate limits. */
    run<T, A extends unknown[]>(caller: (...args: A) => T | Promise<T>, ...args: A): Promise<T>;
}


// --- PUBLIC CLASSES / CONCRETE INTERFACES ---

export interface PrimaryDurationLimiter extends BasePrimaryLimiter {
    readonly mode: "duration";
}

export interface PrimarySemaphoreLimiter extends BasePrimaryLimiter {
    readonly mode: "semaphore";
}

export interface WorkerDurationLimiter extends BaseWorkerLimiter {
    readonly mode: "duration";
}

export interface WorkerSemaphoreLimiter extends BaseWorkerLimiter {
    readonly mode: "semaphore";
}


// --- FACTORY FUNCTIONS ---

/**
 * Initializes the centralized memory state for the rate limiter.
 * 
 * **MUST only be called in the Primary cluster process.**
 * 
 * **Warning:** Requires native Node.js IPC (`node:cluster`). The primary process must be self-managed
 * via explicit `cluster.fork()` calls in application code. Do not use automated cluster 
 * managers/supervisors (e.g., PM2 cluster mode) that abstract or hijack the primary process, 
 * as worker IPC messages will fail to route to the limiter.
 */
export declare function createPrimaryClusterRateLimiter(config: PrimarySemaphoreConfig): PrimarySemaphoreLimiter;
export declare function createPrimaryClusterRateLimiter(config: PrimaryDurationConfig): PrimaryDurationLimiter;
export declare function createPrimaryClusterRateLimiter(config: PrimaryConfig): PrimaryDurationLimiter | PrimarySemaphoreLimiter;

/**
 * Creates a worker instance to request permits from the Primary process.
 * 
 * **MUST only be called in the Worker processes.**
 * 
 * **Warning:** Requires a corresponding Primary limiter instance active in the application's 
 * primary process that spawned this worker via native `node:cluster`. Incompatible with managed 
 * cluster modes where workers do not share a direct native IPC tree with your application's primary code.
 */
export declare function createWorkerClusterRateLimiter(config: WorkerSemaphoreConfig): WorkerSemaphoreLimiter;
export declare function createWorkerClusterRateLimiter(config: WorkerDurationConfig): WorkerDurationLimiter;
export declare function createWorkerClusterRateLimiter(config: WorkerConfig): WorkerDurationLimiter | WorkerSemaphoreLimiter;


// --- ERRORS ---

/**
 * Custom Error class for all Rate Limiter exceptions.
 * Allows users to catch errors by .code instead of fragile message strings.
 */
export declare class RateLimiterError extends Error {
    /** The specific error code (e.g., "ERR_PERMIT_TIMEOUT", "ERR_INVALID_MODE") */
    code: string;
    constructor(code: string, message: string);
}