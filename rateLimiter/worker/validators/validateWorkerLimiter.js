//@ts-check
import { isWorker } from "node:cluster";
import { RateLimiterError } from "../../errors/RateLimiterError.js";
/**
    * @typedef {{resolve: (requestId: number) => void, reject: (reason?: unknown) => void, timeout: NodeJS.Timeout}} PendingRequestTrackers 
*/

/**
    * @param {string} apiProviderId 
    * @param {"semaphore" | "duration"} mode 
    * @param {number} permitTimeoutDurationInSeconds 
    * @param {Map<number, PendingRequestTrackers>} pendingRequestsMap
    * @param {( (requestId: number) => void ) | null} onUnclaimedPermit
*/
export function validateWorkerLimiter(apiProviderId, mode, permitTimeoutDurationInSeconds, pendingRequestsMap, onUnclaimedPermit) {
    if (!isWorker) {
        throw new RateLimiterError(
            "ERR_NOT_WORKER", 
            "WorkerRateLimiter_ClustersMode must be instantiated only in the cluster worker process."
        );
    }
    if (typeof apiProviderId !== "string" || apiProviderId.trim() === "") {
        throw new RateLimiterError(
            "ERR_INVALID_API_PROVIDER_ID", 
            `Invalid apiProviderId: expected a non-empty string, but received ${typeof apiProviderId === "string" ? "an empty string" : typeof apiProviderId}.`
        );
    }
    if (mode !== "duration" && mode !== "semaphore") {
        throw new RateLimiterError(
            "ERR_INVALID_MODE", 
            `Invalid mode: expected 'semaphore' or 'duration', but received '${mode}'.`
        );
    }
    if (!Number.isFinite(permitTimeoutDurationInSeconds) || permitTimeoutDurationInSeconds <= 0) {
        throw new RateLimiterError(
            "ERR_INVALID_TIMEOUT_DURATION", 
            `Invalid permitTimeoutDurationInSeconds: must be a strictly positive finite number, but received ${typeof permitTimeoutDurationInSeconds === "number" ? permitTimeoutDurationInSeconds : typeof permitTimeoutDurationInSeconds}.`
        );
    };
    if (!(pendingRequestsMap instanceof Map)) {
        throw new RateLimiterError("ERR_INVALID_PENDING_MAP", "pendingRequests must be a Map.");
    };
    if (mode === "semaphore" && typeof onUnclaimedPermit !== "function") {
        throw new RateLimiterError(
            "ERR_MISSING_UNCLAIMED_PERMIT_CALLBACK", 
            `onUnclaimedPermit must be a valid function when mode is 'semaphore', but received ${typeof onUnclaimedPermit}.`
        );
    };
};