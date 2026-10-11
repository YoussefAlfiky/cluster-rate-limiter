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
        typeof apiProviderId !== "string" 
        || !registryEntry || typeof registryEntry !== "object" 
        || !registryEntry.limiter || typeof registryEntry.limiter !== "object" 
        || typeof registryEntry.config !== "string" 
        || typeof registryEntry.handleColdMessage !== "function"
    ) {
        throw new RateLimiterError(
            "ERR_INVALID_REGISTRY_ENTRY", 
            "Failed to register primary limiter cold path: 'registryEntry' is malformed. It must strictly implement the ProviderIdRegistryEntry interface containing 'limiter' (object), 'config' (string), and 'handleColdMessage' (function)."
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
    * Generates a unique integer ID and upgrades a primary rate limiter into the O(1) hot-path routing map.
    * 
    * **Architecture Note:**
    * This function safely issues a centralized, auto-incrementing integer (`apiUniqueNumber`). 
    * After registering the high-performance mapping, it executes a proactive broadcast to all 
    * currently active cluster workers. This ensures that even if workers boot *before* this 
    * primary limiter, they immediately receive their fast-path authorization.
    * 
    * **Developer Caution:**
    * Because this is internal routing infrastructure, passing malformed arguments intentionally 
    * throws a synchronous error to fail-fast and prevent silent IPC corruption during development.
    * 
    * @param {string} apiProviderId The unique string identifier of the target API.
    * @param {"semaphore" | "duration"} mode The operational rate-limiting strategy.
    * @param {UniqueNumberRegistryEntry} registryEntry The hot-path capacity handler and worker exit cleanup callback.
    * @throws {RateLimiterError} If arguments are malformed, or if registry state corruption is detected.
    * @returns {number} The newly generated, V8-optimized integer ID (`apiUniqueNumber`) for this limiter.
*/
export function registerLimiterByUniqueNumber(apiProviderId, mode, registryEntry) {
    if (
        typeof apiProviderId !== "string" 
        || (mode !== "duration" && mode !== "semaphore") 
        || !registryEntry || typeof registryEntry !== "object" 
        || typeof registryEntry.handleExit !== "function" 
        || typeof registryEntry.handleHotMessage !== "function"
    ) {
        throw new RateLimiterError(
            "ERR_INVALID_REGISTRY_ENTRY", 
            "Failed to register primary limiter hot path: Invalid arguments. 'apiProviderId' must be a string, 'mode' must be 'duration' or 'semaphore', and 'registryEntry' must strictly implement the UniqueNumberRegistryEntry interface ('handleHotMessage' and 'handleExit')."
        );
    };
    let apiUniqueNumber = -1;
    const limiterRegistry = globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY];
    if (!limiterRegistry) {
        if (globalObject[API_PROVIDERS_COUNTER_KEY]) {
            throw new RateLimiterError(
                "ERR_INTERNAL_REGISTRY_CORRUPTION",
                "Fatal state corruption: Primary limiter counter exists, but the unique number registry map is missing."
            );
        };
        apiUniqueNumber = 1;
        globalObject[API_PROVIDERS_UNIQUE_NUMBER_REGISTRY_KEY] = new Map([[apiUniqueNumber, registryEntry]]);
        globalObject[API_PROVIDERS_COUNTER_KEY] = apiUniqueNumber;
    } else {
        if (!globalObject[API_PROVIDERS_COUNTER_KEY]) {
            throw new RateLimiterError(
                "ERR_INTERNAL_REGISTRY_CORRUPTION",
                "Fatal state corruption: Primary limiter unique number registry map exists, but the counter is missing."
            );
        };
        apiUniqueNumber = ++globalObject[API_PROVIDERS_COUNTER_KEY];
        if (limiterRegistry.has(apiUniqueNumber)) {
            throw new RateLimiterError(
                "ERR_DUPLICATE_UNIQUE_NUMBER", 
                `Collision: A PrimaryRateLimiter for apiUniqueNumber '${apiUniqueNumber}' has already been registered in the fast-path routing map.`
            );
        }
        limiterRegistry.set(apiUniqueNumber, registryEntry);
    };
    // Execute Proactive Primary-Workers Handshake
    for (const workerId in cluster.workers) {
        const worker = cluster.workers[workerId];
        if (!worker) continue;
        worker.send({apiProviderId, mode, apiUniqueNumber});
    };
    //
    return apiUniqueNumber;
};