// @ts-check
import cluster from "node:cluster";
import { RateLimiterError } from "../../errors/RateLimiterError.js";

/**
    * @typedef {Object} PrimaryRegistryEntry
    * @property {Object} limiter
    * @property {string} config
    * @property {(worker: import('node:cluster').Worker, msg: Object) => void} handleMessage
    * @property {(workerId: number) => void} handleExit
    * 
    * @typedef {typeof globalThis & { 
    *   [API_PROVIDERS_ID_REGISTRY_KEY]?: Map<string, PrimaryRegistryEntry>, 
    *   [API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY]?: Map<number, PrimaryRegistryEntry>,
    *   [API_PROVIDERS_COUNTER_KEY]?: number
    * }} PrimaryGlobalObject
*/
/** @type {unique symbol} */
const API_PROVIDERS_ID_REGISTRY_KEY = Symbol.for('PrimaryRateLimiter_ClustersMode:apiProviderIdRegistry_Map');
/** @type {unique symbol} */
const API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY = Symbol.for('PrimaryRateLimiter_ClustersMode:apiProviderUniqueNumberRegistry_Map');
/**@type {unique symbol} */
const API_PROVIDERS_COUNTER_KEY = Symbol.for('PrimaryRateLimiter_ClustersMode:apiProvidersCounter');
/**
    * @param {string} apiProviderId
    * @param {PrimaryRegistryEntry} registryEntry
    * @returns {number}
*/
export function registerPrimaryLimiter(apiProviderId, registryEntry) {
    if (!apiProviderId || !registryEntry) {
        throw new RateLimiterError(
            "ERR_MISSING_REGISTRY_ARGUMENTS", 
            "Failed to register PrimaryRateLimiter: Both 'apiProviderId' and 'registryEntry' are strictly required."
        );
    };
    const globalObject = /**@type {PrimaryGlobalObject}*/(globalThis);
    const providerIdLimiterRegistry_Map = globalObject[API_PROVIDERS_ID_REGISTRY_KEY];
    const providerUniqueNumberLimiterRegistry_Map = globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY];
    if (!providerIdLimiterRegistry_Map) {
        if (providerUniqueNumberLimiterRegistry_Map || globalObject[API_PROVIDERS_COUNTER_KEY]) {
            throw new RateLimiterError(
                "ERR_CORRUPT_PRIMARY_REGISTRY", 
                "Fatal registry desynchronization: The string-based ID registry is missing, but the numeric registry or counter already exists in the global scope."
            );
        };
        globalObject[API_PROVIDERS_COUNTER_KEY] = 1;
        globalObject[API_PROVIDERS_ID_REGISTRY_KEY] = new Map([[apiProviderId, registryEntry]]);
        globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY] = new Map([[1, registryEntry]]);
        cluster.on("message", (worker, msg) => {
            if (msg && typeof msg === "object") {
                if ('apiUniqueNumber' in msg && typeof msg.apiUniqueNumber === "number") {
                    const entry = globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY]?.get(msg.apiUniqueNumber);
                    if (entry) entry.handleMessage(worker, msg);
                } else if ('apiProviderId' in msg && typeof msg.apiProviderId === "string") {
                    const entry = globalObject[API_PROVIDERS_ID_REGISTRY_KEY]?.get(msg.apiProviderId);
                    if (entry) entry.handleMessage(worker, msg);
                }
            }
        });
        cluster.on("exit", (worker) => {
            const limitersRegistryMap = globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY];
            if (!limitersRegistryMap) return;
            for (const [, entry] of limitersRegistryMap) {
                entry.handleExit(worker.id);
            }
        });
        return 1;
    } else {
        if (!providerUniqueNumberLimiterRegistry_Map || !globalObject[API_PROVIDERS_COUNTER_KEY]) {
            throw new RateLimiterError(
                "ERR_CORRUPT_PRIMARY_REGISTRY", 
                "Fatal registry desynchronization: The string-based ID registry exists, but the numeric registry or counter is missing from the global scope."
            );
        };
        if (providerIdLimiterRegistry_Map.has(apiProviderId)) {
            throw new RateLimiterError(
                "ERR_DUPLICATE_PRIMARY_LIMITER", 
                `DuplicateRateLimiter: A PrimaryRateLimiter for apiProviderId '${apiProviderId}' has already been instantiated. You must share a single instance across your primary process.`
            );
        }
        providerIdLimiterRegistry_Map.set(apiProviderId, registryEntry);
        providerUniqueNumberLimiterRegistry_Map.set(++globalObject[API_PROVIDERS_COUNTER_KEY], registryEntry);
        return globalObject[API_PROVIDERS_COUNTER_KEY];
    };
}