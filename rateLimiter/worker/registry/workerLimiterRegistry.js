//@ts-check
import { RateLimiterError } from "../../errors/RateLimiterError.js"
import { IPC_SIGNATURE, IPC_MESSAGE_LENGTH, IPC_INDEX } from "../../shared/ipc-protocol.js";

/**
    * @typedef {Object} ProviderIdRegistryEntry
    * @property {Object} limiter
    * @property {string} config
    * @property {(msg: Object) => void} handleColdMessage
    * 
    * @typedef {Object} UniqueNumberRegistryEntry
    * @property {(msg: number[]) => void} handleHotMessage
*/
/**
    * @typedef {typeof globalThis & { 
    *   [API_PROVIDERS_ID_REGISTRY_KEY]?: Map<string, ProviderIdRegistryEntry>, 
    *   [API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY]?: Map<number, UniqueNumberRegistryEntry>
    * }} WorkerGlobalObject
*/

/** @type {unique symbol} */
const API_PROVIDERS_ID_REGISTRY_KEY = Symbol.for('WorkerRateLimiter_ClustersMode:apiProviderIdRegistry_Map');
/** @type {unique symbol} */
const API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY = Symbol.for('WorkerRateLimiter_ClustersMode:apiProviderUniqueNumberRegistry_Map');

const globalObject = /**@type {WorkerGlobalObject}*/(globalThis);


/**
    * Registers a rate limiter into the cold-path routing map to receive its boot-time handshake.
    * 
    * **Architecture Note:**
    * This function acts as the global process bootstrapper. The very first time it is invoked, 
    * it safely attaches the singleton `process.on('message')` IPC router for the entire worker, 
    * completely preventing `MaxListenersExceededWarning` memory leaks.
    * 
    * @param {string} apiProviderId The unique string identifier defined by the user (e.g., "stripe-api").
    * @param {ProviderIdRegistryEntry} registryEntry The limiter instance and its handshake callback.
    * @throws {RateLimiterError} If a limiter for this ID is already active, preventing rogue duplicates.
*/
export function registerLimiterByProviderId(apiProviderId, registryEntry) {
    if (
        typeof apiProviderId !== "string" 
        || !registryEntry || typeof registryEntry !== "object" 
        || !registryEntry.limiter || typeof registryEntry.limiter !== 'object' 
        || typeof registryEntry.config !== "string" 
        || typeof registryEntry.handleColdMessage !== "function"
    ) {
        throw new RateLimiterError(
            "ERR_INVALID_REGISTRY_ENTRY", 
            "Failed to register limiter cold path: 'registryEntry' is malformed. It must strictly implement the ProviderIdRegistryEntry interface containing 'limiter' (object), 'config' (string), and 'handleColdMessage' (function)."
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
    * Upgrades a rate limiter into the O(1) hot-path routing map.
    * 
    * **When to call:**
    * This must only be invoked *after* the primary process has acknowledged the limiter 
    * and assigned it a high-performance integer ID (`apiUniqueNumber`).
    * 
    * By mapping the integer directly to the handler, it allows the IPC router to bypass 
    * string comparisons and object parsing for all high-frequency capacity traffic.
    * 
    * **Developer Caution:**
    * Because this function is called inside the `process.on('message')` listener during a handshake, 
    * passing malformed arguments will throw a synchronous error and **crash the entire Node.js worker process**.
    * 
    * @param {number} apiUniqueNumber The V8-optimized integer ID provided by the primary process.
    * @param {UniqueNumberRegistryEntry} registryEntry The hot-path callback for capacity grants.
    * @throws {RateLimiterError} If arguments do not strictly match the `UniqueNumberRegistryEntry` interface.
*/
export function registerLimiterByUniqueNumber(apiUniqueNumber, registryEntry) {
    // Developer Caution: if you pass wrong parameters, this error will crash the entire process
    if (
        typeof apiUniqueNumber !== "number"
        || !registryEntry || typeof registryEntry !== "object" 
        || typeof registryEntry.handleHotMessage !== "function"
    ) {
        throw new RateLimiterError(
            "ERR_INVALID_REGISTRY_ENTRY", 
            "Failed to register limiter hot path: 'registryEntry' is malformed. It must strictly implement the UniqueNumberRegistryEntry interface containing 'handleHotMessage' (function)."
        );
    };
    const limiterRegistry = globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY];
    if (!limiterRegistry) {
        globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY] = new Map([[apiUniqueNumber, registryEntry]]);
    } else {
        limiterRegistry.set(apiUniqueNumber, registryEntry);
    };
};