// @ts-check
import cluster from "node:cluster";
import { RateLimiterError } from "../../errors/RateLimiterError.js";
import { IPC_SIGNATURE, IPC_MESSAGE_LENGTH, IPC_INDEX } from "../../shared/ipc-protocol.js";

/**
    * @typedef {import("node:cluster").Worker} Worker 
    * 
    * @typedef {Object} ProviderIdRegistryEntry
    * @property {Object} limiter
    * @property {string} config
    * @property {(worker: Worker, msg: Object) => void} handleColdMessage
    * 
    * @typedef {Object} UniqueNumberRegistryEntry
    * @property {(worker: Worker, msg: number[]) => void} handleHotMessage
    * @property {(workerId: number) => void} handleExit
    * 
    * @typedef {typeof globalThis & { 
    *   [API_PROVIDERS_ID_REGISTRY_KEY]?: Map<string, ProviderIdRegistryEntry>, 
    *   [API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY]?: Map<number, UniqueNumberRegistryEntry>
    *   [API_PROVIDERS_COUNTER_KEY]?: number
    * }} PrimaryGlobalObject
*/
/** @type {unique symbol} */
const API_PROVIDERS_ID_REGISTRY_KEY = Symbol.for('PrimaryRateLimiter_ClustersMode:apiProviderIdRegistry_Map');
/** @type {unique symbol} */
const API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY = Symbol.for('PrimaryRateLimiter_ClustersMode:apiProviderUniqueNumberRegistry_Map');
/**@type {unique symbol} */
const API_PROVIDERS_COUNTER_KEY = Symbol.for('PrimaryRateLimiter_ClustersMode:apiProvidersCounter');

const globalObject = /**@type {PrimaryGlobalObject}*/(globalThis);

/**
    * Registers a primary rate limiter into the cold-path routing map to handle initial worker handshakes.
    * 
    * **Architecture Note:**
    * This function acts as the global primary bootstrapper. On its first invocation, it safely 
    * attaches the singleton `cluster.on('message')` IPC router and the `cluster.on('exit')` 
    * cleanup handler for the entire primary process, completely preventing event listener leaks.
    * 
    * @param {string} apiProviderId The unique string identifier defined by the user (e.g., "stripe-api").
    * @param {ProviderIdRegistryEntry} registryEntry The limiter instance, config and its cold-path message handler.
    * @throws {RateLimiterError} If arguments are malformed or a limiter for this ID is already active.
*/
export function registerLimiterByProviderId(apiProviderId, registryEntry) {
    if (
        typeof apiProviderId !== "string" || apiProviderId.trim() === ""
        || !registryEntry || typeof registryEntry !== "object" 
        || !registryEntry.limiter || typeof registryEntry.limiter !== "object" 
        || typeof registryEntry.config !== "string" 
        || typeof registryEntry.handleColdMessage !== "function"
    ) {
        throw new RateLimiterError(
            "ERR_INVALID_REGISTRY_ENTRY", 
            "Failed to register primary limiter cold path: 'apiProviderId' must be a non-empty string, and 'registryEntry' must strictly implement the ProviderIdRegistryEntry interface containing 'limiter' (object), 'config' (string), and 'handleColdMessage' (function)."
        );
    };
    const limiterRegistry = globalObject[API_PROVIDERS_ID_REGISTRY_KEY];
    if (!limiterRegistry) {
        globalObject[API_PROVIDERS_ID_REGISTRY_KEY] = new Map([[apiProviderId, registryEntry]]);
        cluster.on("message", (worker, msg) => {
            // Hot Path (Executes 99.99% of the time)
            if (Array.isArray(msg)) {
                if (msg.length === IPC_MESSAGE_LENGTH && msg[IPC_INDEX.SIGNATURE] === IPC_SIGNATURE) {
                    const apiUniqueNumber = msg[IPC_INDEX.DESTINATION];
                    const entry = globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY]?.get(apiUniqueNumber);
                    if (entry) entry.handleHotMessage(worker, msg);
                };
                return;
            }
            // Cold Path (Boot-time handshakes)
            if (msg && typeof msg === "object") {
                if ('apiProviderId' in msg && typeof msg.apiProviderId === "string") {
                    const entry = globalObject[API_PROVIDERS_ID_REGISTRY_KEY]?.get(msg.apiProviderId);
                    if (entry) entry.handleColdMessage(worker, msg);
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
    } else {
        if (limiterRegistry.has(apiProviderId)) {
            throw new RateLimiterError(
                "ERR_DUPLICATE_PRIMARY_LIMITER", 
                `DuplicateRateLimiter: A PrimaryRateLimiter for apiProviderId '${apiProviderId}' has already been instantiated. You must share a single instance across your primary process.`
            );
        }
        limiterRegistry.set(apiProviderId, registryEntry);
    };
};

/**
    * Upgrades a primary rate limiter into the O(1) hot-path routing map.
    * 
    * @param {number} apiUniqueNumber The V8-optimized integer ID generated for this limiter.
    * @param {UniqueNumberRegistryEntry} registryEntry The hot-path capacity handler and worker exit cleanup callback.
    * @throws {RateLimiterError} If arguments are malformed, or if an `apiUniqueNumber` collision occurs.
    * @returns {void}
*/
export function registerLimiterByUniqueNumber(apiUniqueNumber, registryEntry) {
    if (
        !Number.isInteger(apiUniqueNumber) || apiUniqueNumber < 1
        || !registryEntry || typeof registryEntry !== "object" 
        || typeof registryEntry.handleExit !== "function" 
        || typeof registryEntry.handleHotMessage !== "function"
    ) {
        throw new RateLimiterError(
            "ERR_INVALID_REGISTRY_ENTRY", 
            "Failed to register primary limiter hot path: Invalid arguments. 'apiUniqueNumber' must be a positive integer, and 'registryEntry' must strictly implement the UniqueNumberRegistryEntry interface."
        );
    };
    const limiterRegistry = globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY];
    if (!limiterRegistry) {
        globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY] = new Map([[apiUniqueNumber, registryEntry]]);
    } else {
        if (limiterRegistry.has(apiUniqueNumber)) {
            throw new RateLimiterError(
                "ERR_DUPLICATE_UNIQUE_NUMBER", 
                `Collision: A PrimaryRateLimiter for apiUniqueNumber '${apiUniqueNumber}' has already been registered in the fast-path routing map.`
            );
        }
        limiterRegistry.set(apiUniqueNumber, registryEntry);
    };
};

/** @returns {number} */
export function generateApiUniqueNumber() {
    if (!globalObject[API_PROVIDERS_COUNTER_KEY]) {
        // start from 1 to avoid falsy zero => may confuse in condition checks
        globalObject[API_PROVIDERS_COUNTER_KEY] = 1;
    } else {
        globalObject[API_PROVIDERS_COUNTER_KEY]++;
    }
    return globalObject[API_PROVIDERS_COUNTER_KEY];
};


/**
    * Safely evicts a primary rate limiter from the global IPC routing registries.
    * 
    * **Architecture Note:**
    * Used primarily as an internal transaction rollback mechanism. If a Limiter subclass 
    * fails to allocate its memory (e.g., throwing a RangeError on massive queues) after 
    * the base constructor has already registered it, this cleans up the global maps. 
    * This prevents "ghost" limiters from permanently blocking the `apiProviderId` 
    * or leaking memory in the hot-path router.
    * 
    * @param {string} apiProviderId The unique string identifier of the limiter.
    * @param {number} apiUniqueNumber The fast-path integer ID assigned to the limiter.
    * @throws {RateLimiterError} If arguments are malformed or registries are uninitialized.
    * @returns {void}
*/
export function unregisterLimiter(apiProviderId, apiUniqueNumber) {
    if (
        typeof apiProviderId !== "string" || apiProviderId.trim() === ""
        ||
        !Number.isInteger(apiUniqueNumber) || apiUniqueNumber < 1
    ) {
        throw new RateLimiterError(
            "ERR_INVALID_UNREGISTER_ARGS", 
            "Failed to unregister primary limiter: 'apiProviderId' must be a non-empty string and 'apiUniqueNumber' must be a positive integer."
        );
    };
    
    const providerIdLimiterRegistry = globalObject[API_PROVIDERS_ID_REGISTRY_KEY];
    const uniqueNumberLimiterRegistry = globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY];
    
    providerIdLimiterRegistry?.delete(apiProviderId);
    uniqueNumberLimiterRegistry?.delete(apiUniqueNumber);
};