// @ts-check
import { PrimaryRateLimiter_ClustersMode } from "./rateLimiter/PrimaryClusterRateLimiter.js";
import { WorkerRateLimiter_ClustersMode } from "./rateLimiter/WorkerClusterRateLimiter.js";
import { RateLimiterError } from "./rateLimiter/errors/RateLimiterError.js";
/**
    * @typedef {import("node:cluster").Worker} Worker
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

/**
    * @typedef {Object} WorkerRegistryEntry
    * @property {WorkerRateLimiter_ClustersMode} limiter
    * @property {string} config
    * @property {(msg: Object) => void} handleMessage
    * 
    * @typedef {typeof globalThis & { [key: symbol]: Map<string, WorkerRegistryEntry> }} WorkerGlobalObject
*/

/**
    * @typedef {Object} PrimarySemaphoreConfig
    * @property {string} apiProviderId Unique identifier for the API.
    * @property {number} maxConcurrentRequests Max active permits.
    * @property {"semaphore"} mode "semaphore" for concurrency.
*/

/**
    * @typedef {Object} PrimaryDurationConfig
    * @property {string} apiProviderId Unique identifier for the API.
    * @property {number} maxConcurrentRequests Requests per duration limit.
    * @property {"duration"} mode "duration" for time-window limits.
    * @property {number} durationInSeconds Required: The length of the sliding request window.
*/

/**
    * @typedef {PrimarySemaphoreConfig | PrimaryDurationConfig} PrimaryConfig
*/

/**
    * @typedef {Object} WorkerConfig
    * @property {string} apiProviderId Unique identifier for the API.
    * @property {"semaphore" | "duration"} mode Must match the Primary's mode.
    * @property {number} permitTimeoutDurationInSeconds Max time to wait for IPC token.
*/


/**
    * Initializes the centralized memory state for the rate limiter.
    * 
    * **MUST only be called in the Primary cluster process.**
    * @warning **Process Architecture Constraint:**
    * Requires native Node.js IPC (`node:cluster`). The primary process must be self-managed
    * via explicit `cluster.fork()` calls in application code. Do not use automated cluster 
    * managers/supervisors (e.g., PM2 cluster mode) that abstract or hijack the primary process, 
    * as worker IPC messages will fail to route to the limiter.
    * 
    * @param {PrimaryConfig} config 
    * @returns {PrimaryRateLimiter_ClustersMode}
*/
function createPrimaryClusterRateLimiter(config) {
    try {
        return new PrimaryRateLimiter_ClustersMode(
            config.apiProviderId,
            config.maxConcurrentRequests,
            config.mode,
            // @ts-ignore - Safely ignores undefined if mode is semaphore
            config.durationInSeconds
        );
    } catch (err) {
        if (err instanceof RateLimiterError && err.code === "ERR_DUPLICATE_PRIMARY_LIMITER") {
            const API_PROVIDERS_REGISTRY_KEY = Symbol.for('PrimaryRateLimiter_ClustersMode:registryMap');
            const limitersRegistry = /**@type {PrimaryGlobalObject}*/(globalThis)[API_PROVIDERS_REGISTRY_KEY];
            if (limitersRegistry) {
                const registryEntry = limitersRegistry.get(config.apiProviderId);
                if (registryEntry) {
                    // @ts-ignore - Safely ignores undefined if mode is semaphore
                    if (registryEntry.config === `${config.maxConcurrentRequests}-${config.mode}-${config.durationInSeconds}`) {
                        return registryEntry.limiter;
                    };
                    throw new RateLimiterError(
                        "ERR_CONFLICTING_LIMITER_CONFIG", 
                        `A Primary Rate Limiter for '${config.apiProviderId}' already exists, but was initialized with a different configuration. You cannot mix modes or limits for the same API.`
                    );
                };
            };
        };
        throw err;
    }
}

/**
    * Creates a worker instance to request permits from the Primary process.
    * 
    * **MUST only be called in the Worker processes.**
    * @warning **Process Architecture Constraint:**
    * Requires a corresponding Primary limiter instance active in the application's 
    * primary process that spawned this worker via native 
    * `node:cluster`. Incompatible with managed cluster modes where workers do not 
    * share a direct native IPC tree with your application's primary code.
    * 
    * @param {WorkerConfig} config 
    * @returns {WorkerRateLimiter_ClustersMode}
*/
function createWorkerClusterRateLimiter(config) {
    try {
        return new WorkerRateLimiter_ClustersMode(
            config.apiProviderId,
            config.mode,
            config.permitTimeoutDurationInSeconds
        );
    } catch (err) {
        if (err instanceof RateLimiterError && err.code === "ERR_DUPLICATE_WORKER_LIMITER") {
            const API_PROVIDERS_REGISTRY_KEY = Symbol.for('WorkerRateLimiter_ClustersMode:registryMap');
            const limitersRegistry = /**@type {WorkerGlobalObject}*/(globalThis)[API_PROVIDERS_REGISTRY_KEY];
            if (limitersRegistry) {
                const registryEntry = limitersRegistry.get(config.apiProviderId);
                if (registryEntry) {
                    if (registryEntry.config === `${config.mode}-${config.permitTimeoutDurationInSeconds}`) {
                        return registryEntry.limiter;
                    }
                    throw new RateLimiterError(
                        "ERR_CONFLICTING_LIMITER_CONFIG", 
                        `A Worker Rate Limiter for '${config.apiProviderId}' already exists, but was initialized with a different timeout or mode. You must use matching configurations across your app.`
                    );
                };
            };
        };
        throw err;
    };
}

export { createPrimaryClusterRateLimiter, createWorkerClusterRateLimiter, RateLimiterError };