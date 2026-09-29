// @ts-check
import { isPrimary } from "node:cluster";
import { RateLimiterError } from "../../errors/RateLimiterError.js";

/**
    * @typedef {Object} SemaphoreConfig
    * @property {"semaphore"} mode
    * @property {string} apiProviderId
    * @property {number} maxConcurrentRequests
    * 
    * @typedef {Object} DurationConfig
    * @property {"duration"} mode
    * @property {string} apiProviderId
    * @property {number} maxConcurrentRequests
    * @property {number} durationInSeconds
    * 
    * @typedef {SemaphoreConfig | DurationConfig} LimiterConfig
*/

/** @param {LimiterConfig} config */
export function validatePrimaryLimiterConstructorInput(config) {
    if (!config || typeof config !== "object") {
        throw new RateLimiterError("ERR_INVALID_CONFIG", "Configuration object is required.");
    }
    if (!isPrimary) {
        throw new RateLimiterError(
            "ERR_NOT_PRIMARY", 
            "PrimaryRateLimiter_ClustersMode must be instantiated only in the primary cluster process."
        );
    }
    const { mode, apiProviderId, maxConcurrentRequests } = config;
    if (mode !== "semaphore" && mode !== "duration") {
        throw new RateLimiterError(
            "ERR_INVALID_MODE", 
            `Invalid mode: expected 'semaphore' or 'duration', but received '${mode}'.`
        );
    }
    if (typeof apiProviderId !== "string" || apiProviderId.trim() === "") {
        throw new RateLimiterError(
            "ERR_INVALID_API_PROVIDER_ID", 
            `Invalid apiProviderId: expected a non-empty string, but received ${typeof apiProviderId === "string" ? "an empty string" : typeof apiProviderId}.`
        );
    }
    if (!Number.isInteger(maxConcurrentRequests) || maxConcurrentRequests <= 0) {
        throw new RateLimiterError(
            "ERR_INVALID_MAX_CONCURRENT_REQUESTS", 
            "maxConcurrentRequests must be a positive integer."
        );
    }
    if (mode === "duration") {
        const duration = config.durationInSeconds;
        if (typeof duration !== "number" || !Number.isFinite(duration) || duration <= 0) {
            throw new RateLimiterError(
                "ERR_INVALID_DURATION", 
                "InvalidDuration: when mode is duration, durationInSeconds must be a positive numeric value."
            );
        }
    }
};