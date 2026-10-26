//@ts-check
import { isWorker } from "node:cluster";
import { RateLimiterError } from "../../errors/RateLimiterError.js";
/**
    * @typedef {{resolve: (requestId: number) => void, reject: (reason?: unknown) => void, timeout: NodeJS.Timeout}} PendingRequestTrackers 
*/
/**
    * @typedef {(apiProviderId: string, requestId: number, err: unknown) => void | Promise<void>} ReleaseFailureCallback
    * @typedef {(err: unknown) => void | Promise<void>} ErrorCallback
    * 
    * @typedef {Object} ReleasePermitListenerBuckets
    * @property {{ on: Set<ReleaseFailureCallback>, once: Set<ReleaseFailureCallback> }} releasePermitFailure
    * @property {{ on: Set<ErrorCallback>, once: Set<ErrorCallback> }} error
*/

/**
    * @param {string} apiProviderId 
    * @param {"semaphore" | "duration"} mode 
    * @param {number} permitTimeoutDurationInSeconds 
*/
export function validateWorkerLimiterMainInput(apiProviderId, mode, permitTimeoutDurationInSeconds) { 
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
};


/**
    * @param {string} apiProviderId 
    * @param {"semaphore" | "duration"} mode 
    * @param {number} permitTimeoutDurationInSeconds 
    * @param {ReleasePermitListenerBuckets | null} releasePermitListenerBuckets
*/
export function validateWorkerLimiter(apiProviderId, mode, permitTimeoutDurationInSeconds, releasePermitListenerBuckets) {
    validateWorkerLimiterMainInput(apiProviderId, mode, permitTimeoutDurationInSeconds);
    if (mode === "semaphore") {
        if (!releasePermitListenerBuckets || typeof releasePermitListenerBuckets !== "object") {
            throw new RateLimiterError(
                "ERR_INVALID_LISTENER_BUCKETS", 
                "Semaphore mode requires 'releasePermitListenerBuckets' to be a valid object."
            );
        };
        const { releasePermitFailure, error } = releasePermitListenerBuckets;
        if (!releasePermitFailure || !error) {
            throw new RateLimiterError(
                "ERR_INVALID_LISTENER_BUCKETS", 
                "'releasePermitListenerBuckets' must contain 'releasePermitFailure' and 'error' properties."
            );
        };
        if (
            !(releasePermitFailure.on instanceof Set)
            || !(releasePermitFailure.once instanceof Set)
            || !(error.on instanceof Set)
            || !(error.once instanceof Set)
        ) {
            throw new RateLimiterError(
                "ERR_INVALID_LISTENER_BUCKETS", 
                "Listener bucket collections ('on' and 'once') must be instances of Set."
            );
        }
    };
};