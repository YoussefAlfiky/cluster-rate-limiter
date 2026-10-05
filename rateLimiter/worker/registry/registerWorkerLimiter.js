//@ts-check
import { RateLimiterError } from "../../errors/RateLimiterError.js"
import { IPC_SIGNATURE, IPC_MESSAGE_LENGTH, IPC_INDEX } from "../../shared/ipc-protocol.js";

/**
    * @typedef {Object} BaseWorkerLimiterRegistryEntry
    * @property {Object} limiter
    * @property {string} config
    * @property {(msg: Object) => void} handleColdMessage
    * 
    * @typedef {Object} WorkerLimiterRegistryEntry
    * @property {(msg: number[]) => void} handleHotMessage
*/
/**
    * @typedef {typeof globalThis & { 
    *   [API_PROVIDERS_ID_REGISTRY_KEY]?: Map<string, BaseWorkerLimiterRegistryEntry>, 
    *   [API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY]?: Map<number, WorkerLimiterRegistryEntry>
    * }} WorkerGlobalObject
*/

/** @type {unique symbol} */
const API_PROVIDERS_ID_REGISTRY_KEY = Symbol.for('WorkerRateLimiter_ClustersMode:apiProviderIdRegistry_Map');
/** @type {unique symbol} */
const API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY = Symbol.for('WorkerRateLimiter_ClustersMode:apiProviderUniqueNumberRegistry_Map');

const globalObject = /**@type {WorkerGlobalObject}*/(globalThis);


/**
    * @param {string} apiProviderId
    * @param {BaseWorkerLimiterRegistryEntry} registryEntry
*/
export function registerBaseWorkerLimiter(apiProviderId, registryEntry) {
    if (typeof apiProviderId !== "string" || !registryEntry || typeof registryEntry !== "object") {
        throw new RateLimiterError(
            "", 
            ""
        );
    };
    const limitersRegistry = globalObject[API_PROVIDERS_ID_REGISTRY_KEY];
    if (!limitersRegistry) {
        globalObject[API_PROVIDERS_ID_REGISTRY_KEY] = new Map([[apiProviderId, registryEntry]]);
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
    * @param {WorkerLimiterRegistryEntry} registryEntry
*/
export function registerWorkerLimiter(apiUniqueNumber, registryEntry) {
    if (typeof apiUniqueNumber !== "number" || !registryEntry || typeof registryEntry !== "object") {
        throw new RateLimiterError(
            "", 
            ""
        );
    };
    const limiterRegistry = globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY];
    if (!limiterRegistry) {
        globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY] = new Map([[apiUniqueNumber, registryEntry]]);
    } else {
        limiterRegistry.set(apiUniqueNumber, registryEntry);
    };
};