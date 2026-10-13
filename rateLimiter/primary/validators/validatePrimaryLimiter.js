// @ts-check
import { isPrimary } from "node:cluster";
import { RateLimiterError } from "../../errors/RateLimiterError.js";

/**
    * @param {string} apiProviderId 
    * @param {"semaphore" | "duration"} mode 
    * @param {number} maxConcurrentRequests 
    * @param {number | null} durationInSeconds
*/
export function validatePrimaryLimiter(apiProviderId, mode, maxConcurrentRequests, durationInSeconds) {
    if (!isPrimary) {
        throw new RateLimiterError(
            "ERR_NOT_PRIMARY", 
            "Primary Rate Limiter must be instantiated only in the primary cluster process."
        );
    };
    if (typeof apiProviderId !== "string" || apiProviderId.trim() === "") {
        throw new RateLimiterError(
            "ERR_INVALID_API_PROVIDER_ID", 
            `Invalid apiProviderId: expected a non-empty string, but received ${typeof apiProviderId === "string" ? "an empty string" : typeof apiProviderId}.`
        );
    };
    if (mode !== "semaphore" && mode !== "duration") {
        throw new RateLimiterError(
            "ERR_INVALID_MODE", 
            `Invalid mode: expected 'semaphore' or 'duration', but received '${mode}'.`
        );
    };
    if (!Number.isInteger(maxConcurrentRequests) || maxConcurrentRequests <= 0) {
        throw new RateLimiterError(
            "ERR_INVALID_MAX_CONCURRENT_REQUESTS", 
            "maxConcurrentRequests must be a positive integer."
        );
    }
    if (mode === "duration") {
        if (typeof durationInSeconds !== "number" || !Number.isFinite(durationInSeconds) || durationInSeconds <= 0) {
            throw new RateLimiterError(
                "ERR_INVALID_DURATION", 
                "InvalidDuration: when mode is duration, durationInSeconds must be a positive numeric value."
            );
        }
    };
};