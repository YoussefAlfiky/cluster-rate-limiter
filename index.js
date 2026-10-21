// @ts-check
import { PrimaryDurationLimiter } from "./rateLimiter/primary/PrimaryDurationLimiter.js";
import { PrimarySemaphoreLimiter } from "./rateLimiter/primary/PrimarySemaphoreLimiter.js";
import { WorkerDurationLimiter } from "./rateLimiter/worker/WorkerDurationLimiter.js";
import { WorkerSemaphoreLimiter } from "./rateLimiter/worker/WorkerSemaphoreLimiter.js";
import { RateLimiterError } from "./rateLimiter/errors/RateLimiterError.js";

/**
    * @typedef {Object} PrimaryRegistryEntry
    * @property {PrimaryDurationLimiter | PrimarySemaphoreLimiter} limiter
    * @property {string} config
    * 
    * @typedef {typeof globalThis & { [key: symbol]: Map<string, PrimaryRegistryEntry> }} PrimaryGlobalObject
*/

/**
    * @typedef {Object} WorkerRegistryEntry
    * @property {WorkerDurationLimiter | WorkerSemaphoreLimiter} limiter
    * @property {string} config
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
    * @overload
    * @param {PrimarySemaphoreConfig} config 
    * @returns {PrimarySemaphoreLimiter}
*/
/**
    * @overload
    * @param {PrimaryDurationConfig} config 
    * @returns {PrimaryDurationLimiter}
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
    * @param {PrimarySemaphoreConfig | PrimaryDurationConfig} config 
    * @returns {PrimaryDurationLimiter | PrimarySemaphoreLimiter}
*/
function createPrimaryClusterRateLimiter(config) {
    const {mode, apiProviderId, maxConcurrentRequests} = config;
    try {
        if (mode === "duration") {
            return new PrimaryDurationLimiter(apiProviderId, maxConcurrentRequests, config.durationInSeconds);
        } else if (mode === "semaphore") {
            return new PrimarySemaphoreLimiter(apiProviderId, maxConcurrentRequests);
        }
        throw new RateLimiterError("ERR_INVALID_MODE", `Invalid mode: ${mode}. Must be 'duration' or 'semaphore'.`);
    } catch (err) {
        if (!(err instanceof RateLimiterError) || err.code !== "ERR_DUPLICATE_PRIMARY_LIMITER") throw err;
        // Note: Update this symbol string if you changed it in primaryLimiterRegistry.js
        const API_PROVIDERS_ID_REGISTRY_KEY = Symbol.for('PrimaryRateLimiter_ClustersMode:apiProviderIdRegistry_Map');
        const limitersRegistry = /**@type {PrimaryGlobalObject}*/(globalThis)[API_PROVIDERS_ID_REGISTRY_KEY];
        if (!limitersRegistry) throw err;
        const registryEntry = limitersRegistry.get(apiProviderId);
        if (!registryEntry) throw err;
        // Match the exact template literal format created in BasePrimaryLimiter
        const durationStr = mode === "duration" ? config.durationInSeconds : null;
        const expectedConfig = `${maxConcurrentRequests}-${mode}-${durationStr}`;
        if (registryEntry.config === expectedConfig) {
            return registryEntry.limiter;
        }
        throw new RateLimiterError(
            "ERR_CONFLICTING_LIMITER_CONFIG", 
            `A Primary Rate Limiter for '${apiProviderId}' already exists, but was initialized with a different configuration. You cannot mix modes or limits for the same API.`
        );
    }
}

/**
    * @typedef {Object} WorkerSemaphoreConfig
    * @property {string} apiProviderId Unique identifier for the API.
    * @property {"semaphore"} mode Must match the Primary's mode.
    * @property {number} permitTimeoutDurationInSeconds Max time to wait for IPC token.
    * 
    * @typedef {Object} WorkerDurationConfig
    * @property {string} apiProviderId Unique identifier for the API.
    * @property {"duration"} mode Must match the Primary's mode.
    * @property {number} permitTimeoutDurationInSeconds Max time to wait for IPC token.
*/
/**
    * @overload
    * @param {WorkerSemaphoreConfig} config 
    * @returns {WorkerSemaphoreLimiter}
*/
/**
    * @overload
    * @param {WorkerDurationConfig} config 
    * @returns {WorkerDurationLimiter}
*/
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
    * @param {WorkerSemaphoreConfig | WorkerDurationConfig} config 
    * @returns {WorkerDurationLimiter | WorkerSemaphoreLimiter}
*/
function createWorkerClusterRateLimiter(config) {
    const { mode, apiProviderId, permitTimeoutDurationInSeconds } = config;
    try {
        if (mode === "duration") {
            return new WorkerDurationLimiter(apiProviderId, permitTimeoutDurationInSeconds);
        } else if (mode === "semaphore") {
            return new WorkerSemaphoreLimiter(apiProviderId, permitTimeoutDurationInSeconds);
        }
        throw new RateLimiterError("ERR_INVALID_MODE", `Invalid mode: ${mode}. Must be 'duration' or 'semaphore'.`);
    } catch (err) {
        if (!(err instanceof RateLimiterError) || err.code !== "ERR_DUPLICATE_WORKER_LIMITER") throw err;
        // Note: Update this symbol string if you changed it in workerLimiterRegistry.js
        const API_PROVIDERS_ID_REGISTRY_KEY = Symbol.for('WorkerRateLimiter_ClustersMode:apiProviderIdRegistry_Map');
        const limitersRegistry = /**@type {WorkerGlobalObject}*/(globalThis)[API_PROVIDERS_ID_REGISTRY_KEY];
        if (!limitersRegistry) throw err;
        const registryEntry = limitersRegistry.get(apiProviderId);
        if (!registryEntry) throw err;
        // Match the exact template literal format created in BaseWorkerLimiter
        if (registryEntry.config === `${mode}-${permitTimeoutDurationInSeconds}`) {
            return registryEntry.limiter;
        }
        throw new RateLimiterError(
            "ERR_CONFLICTING_LIMITER_CONFIG", 
            `A Worker Rate Limiter for '${apiProviderId}' already exists, but was initialized with a different timeout or mode. You must use matching configurations across your app.`
        );
    }
}

export { createPrimaryClusterRateLimiter, createWorkerClusterRateLimiter, RateLimiterError };