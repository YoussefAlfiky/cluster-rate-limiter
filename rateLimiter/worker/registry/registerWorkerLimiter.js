//@ts-check
import { RateLimiterError } from "../../errors/RateLimiterError.js"
import { IPC_SIGNATURE, IPC_MESSAGE_LENGTH, IPC_INDEX } from "../../shared/ipc-protocol.js";

/**
    * @typedef {Object} WorkerRegistryEntry
    * @property {Object} limiter
    * @property {string} config
    * @property {(msg: number[]) => void} handleHotMessage
    * @property {(msg: Object) => void} handleColdMessage
    * 
    * @typedef {typeof globalThis & { 
    *   [API_PROVIDERS_ID_REGISTRY_KEY]?: Map<string, WorkerRegistryEntry>, 
    *   [API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY]?: Map<number, WorkerRegistryEntry>
    * }} WorkerGlobalObject
*/

/** @type {unique symbol} */
const API_PROVIDERS_ID_REGISTRY_KEY = Symbol.for('WorkerRateLimiter_ClustersMode:apiProviderIdRegistry_Map');
/** @type {unique symbol} */
const API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY = Symbol.for('WorkerRateLimiter_ClustersMode:apiProviderUniqueNumberRegistry_Map');

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
    const limitersRegistry = globalObject[API_PROVIDERS_ID_REGISTRY_KEY];
    if (!limitersRegistry) {
        globalObject[API_PROVIDERS_ID_REGISTRY_KEY] = new Map([[apiProviderId, registryEntry]]);
        globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY] = new Map();
        process.on("message", (msg) => {
            // Hot Path (Executes 99.99% of the time)
            if (Array.isArray(msg)) {
                if (msg.length === IPC_MESSAGE_LENGTH && msg[IPC_INDEX.SIGNATURE] === IPC_SIGNATURE) {
                    const apiUniqueNumber = msg[IPC_INDEX.DESTINATION];
                    const entry = globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY]?.get(apiUniqueNumber);
                    if (entry) entry.handleHotMessage(msg);
                }
                return;
            };
            // Cold Path (Boot-time handshakes)
            if (msg && typeof msg === "object") {
                if ('apiProviderId' in msg && typeof msg.apiProviderId === "string") {
                    const entry = globalObject[API_PROVIDERS_ID_REGISTRY_KEY]?.get(msg.apiProviderId);
                    if (entry) entry.handleColdMessage(msg);
                }
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

/**
    * @param {number} apiUniqueNumber
    * @param {WorkerRegistryEntry} registryEntry
*/
export function registerLimiterApiUniqueNumber(apiUniqueNumber, registryEntry) {
    const globalObject = /**@type {WorkerGlobalObject}*/(globalThis);
    const limiterRegistry = globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY];
    if (!limiterRegistry) {
        globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY] = new Map([[apiUniqueNumber, registryEntry]]);
    } else {
        limiterRegistry.set(apiUniqueNumber, registryEntry);
    };
};