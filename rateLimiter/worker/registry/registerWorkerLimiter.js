//@ts-check
import { RateLimiterError } from "../../errors/RateLimiterError.js"

/**
    * @typedef {Object} WorkerRegistryEntry
    * @property {Object} limiter
    * @property {string} config
    * @property {(msg: Object) => void} handleMessage
    * 
    * @typedef {typeof globalThis & { [API_PROVIDERS_REGISTRY_KEY]?: Map<string, WorkerRegistryEntry> }} WorkerGlobalObject
*/

/**@type {unique symbol}*/
const API_PROVIDERS_REGISTRY_KEY = Symbol.for('WorkerRateLimiter_ClustersMode:registryMap');

/**
    * @param {string} apiProviderId
    * @param {WorkerRegistryEntry} registryEntry
*/
export function registerWorkerLimiter(apiProviderId, registryEntry) {
    if (!apiProviderId || typeof registryEntry !== "object") {
        throw new RateLimiterError(
            "ERR_MISSING_REGISTRY_ARGUMENTS", 
            "Failed to register BaseWorkerLimiter: Both 'apiProviderId' and 'registryEntry' are strictly required."
        );
    };
    const globalObject = /**@type {WorkerGlobalObject}*/(globalThis);
    const limitersRegistry = globalObject[API_PROVIDERS_REGISTRY_KEY];
    if (!limitersRegistry) {
        globalObject[API_PROVIDERS_REGISTRY_KEY] = new Map([[apiProviderId, registryEntry]]);
        process.on("message", (msg) => {
            if (msg && typeof msg === "object" && 'apiProviderId' in msg && typeof msg.apiProviderId === "string") {
                const entry = globalObject[API_PROVIDERS_REGISTRY_KEY]?.get(msg.apiProviderId);
                if (entry) entry.handleMessage(msg);
            };
        });
    } else {
        if (limitersRegistry.has(apiProviderId)) {
            throw new RateLimiterError(
                "ERR_DUPLICATE_WORKER_LIMITER", 
                `DuplicateRateLimiter: A BaseWorkerLimiter for apiProviderId '${apiProviderId}' has already been instantiated. You must share a single instance across your process.`
            );
        }
        limitersRegistry.set(apiProviderId, registryEntry);
    };
};