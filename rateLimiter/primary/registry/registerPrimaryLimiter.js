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
    * @typedef {typeof globalThis & { [API_PROVIDERS_REGISTRY_KEY]?: Map<string, PrimaryRegistryEntry> }} PrimaryGlobalObject
*/
/** @type {unique symbol} */
const API_PROVIDERS_REGISTRY_KEY = Symbol.for('PrimaryRateLimiter_ClustersMode:registryMap');

/**
    * @param {string} apiProviderId
    * @param {PrimaryRegistryEntry} registryEntry
*/
export function registerPrimaryLimiter(apiProviderId, registryEntry) {
    if (!apiProviderId || !registryEntry) {
        throw new RateLimiterError(
            "ERR_MISSING_REGISTRY_ARGUMENTS", 
            "Failed to register PrimaryRateLimiter: Both 'apiProviderId' and 'registryEntry' are strictly required."
        );
    };
    const globalObject = /**@type {PrimaryGlobalObject}*/(globalThis);
    const limitersRegistry = globalObject[API_PROVIDERS_REGISTRY_KEY];
    if (!limitersRegistry) {
        globalObject[API_PROVIDERS_REGISTRY_KEY] = new Map([[apiProviderId, registryEntry]]);
        cluster.on("message", (worker, msg) => {
            if (msg && typeof msg === "object" && 'apiProviderId' in msg && typeof msg.apiProviderId === "string") {
                const entry = globalObject[API_PROVIDERS_REGISTRY_KEY]?.get(msg.apiProviderId);
                if (entry) entry.handleMessage(worker, msg);
            }
        });
        cluster.on("exit", (worker) => {
            const limitersRegistryMap = globalObject[API_PROVIDERS_REGISTRY_KEY];
            if (!limitersRegistryMap) return;
            for (const [, entry] of limitersRegistryMap) {
                entry.handleExit(worker.id);
            }
        });
    } else {
        if (limitersRegistry.has(apiProviderId)) {
            throw new RateLimiterError(
                "ERR_DUPLICATE_PRIMARY_LIMITER", 
                `DuplicateRateLimiter: A PrimaryRateLimiter for apiProviderId '${apiProviderId}' has already been instantiated. You must share a single instance across your primary process.`
            );
        }
        limitersRegistry.set(apiProviderId, registryEntry);
    };
}