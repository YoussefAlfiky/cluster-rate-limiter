//@ts-check
import {RateLimiterError} from "../../errors/RateLimiterError.js"
import { IPC_SIGNATURE, IPC_OPCODES } from "../../shared/ipc-protocol.js";
/**
    * @typedef {{resolve: (requestId: number) => void, reject: (reason?: unknown) => void, timeout: NodeJS.Timeout}} PendingRequestTrackers 
*/

/** 
    * @param {number} apiUniqueNumber
    * @param {number} requestId
    * @param {Map<number, PendingRequestTrackers>} pendingRequestsMap
    * @param {number} permitTimeoutDurationMs
    * @returns {Promise<number>} 
*/
export function acquirePermit(apiUniqueNumber, requestId, pendingRequestsMap, permitTimeoutDurationMs) {
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
        process.send([IPC_SIGNATURE, apiUniqueNumber, IPC_OPCODES.ACQUIRE, requestId], (err) => {
            if (err) {
                pendingRequestsMap.delete(requestId);
                clearTimeout(timeout);
                reject(err);
            };
        });
    });
};