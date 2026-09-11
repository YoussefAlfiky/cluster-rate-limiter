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

export interface WorkerConfig {
    /** Unique identifier for the API. */
    apiProviderId: string;
    /** Must match the Primary's mode. */
    mode: "semaphore" | "duration";
    /** Max time to wait for IPC token. */
    permitTimeoutDurationInSeconds: number;
}

// --- CLASSES ---

export interface PrimaryRateLimiter_ClustersMode {
  // no public methods
}

export interface WorkerRateLimiter_ClustersMode {
    run<T, A extends unknown[]>(caller: (...args: A) => T | Promise<T>, ...args: A): Promise<T>;
}

// --- FACTORY FUNCTIONS ---
/**
    * Initializes the centralized memory state for the rate limiter.
    * **MUST only be called in the Primary cluster process.**
*/
export declare function createPrimaryClusterRateLimiter(config: PrimaryConfig): PrimaryRateLimiter_ClustersMode;

/**
    * Creates a worker instance to request permits from the Primary process.
    * **MUST only be called in the Worker processes.**
*/
export declare function createWorkerClusterRateLimiter(config: WorkerConfig): WorkerRateLimiter_ClustersMode;

// --- ERRORS ---
/**
    * Custom Error class for all Rate Limiter exceptions.
    * Allows users to catch errors by .code instead of fragile message strings.
*/
export class RateLimiterError extends Error {
    /** The specific error code (e.g., "ERR_PERMIT_TIMEOUT", "ERR_DUPLICATE_WORKER_LIMITER") */
    code: string;
    constructor(code: string, message: string);
}