//@ts-check
import { randomUUID } from "node:crypto";
import {RateLimiterError} from "../../errors/RateLimiterError.js"
/**
    * @typedef {import("node:crypto").UUID} UUID
    * @typedef {{resolve: (requestId: UUID) => void, reject: (reason?: unknown) => void, timeout: NodeJS.Timeout}} PendingRequestTrackers 
*/

/** 
    * @param {string} apiProviderId
    * @param {Map<UUID, PendingRequestTrackers>} pendingRequestsMap
    * @param {number} permitTimeoutDurationMs
    * @returns {Promise<UUID>} 
*/
export function acquirePermit(apiProviderId, pendingRequestsMap, permitTimeoutDurationMs) {
    const requestId = randomUUID();
    return new Promise((resolve, reject) => {
        if (!process.send) {
            return reject(new RateLimiterError(
                "ERR_IPC_CHANNEL_CLOSED", 
                "Cannot acquire permit: The IPC channel is closed. The worker may be disconnected or orphaned."
            ));
        };
        const timeout = setTimeout(() => {
            const pendingRequestTrackers = pendingRequestsMap.get(requestId);
            if (pendingRequestTrackers) {
                const { reject } = pendingRequestTrackers;
                pendingRequestsMap.delete(requestId);
                reject(new RateLimiterError("ERR_PERMIT_TIMEOUT", `Timed out waiting for a permit for ${requestId} request.`));
            };
        }, permitTimeoutDurationMs);
        pendingRequestsMap.set(requestId, {resolve, reject, timeout});
        process.send({apiProviderId, requestId, askForToken: true}, (err) => {
            if (err) {
                pendingRequestsMap.delete(requestId);
                clearTimeout(timeout);
                reject(err);
            };
        });
    });
};