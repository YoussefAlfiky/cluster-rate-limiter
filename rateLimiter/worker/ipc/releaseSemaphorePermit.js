//@ts-check
import { RateLimiterError } from "../../errors/RateLimiterError.js";
import { triggerEvent } from "../../events/triggerEvent.js"

/**
    * @typedef {(apiProviderId: string, requestId: number, err: unknown) => void | Promise<void>} ReleaseFailureCallback
    * @typedef {(err: unknown) => void | Promise<void>} ErrorCallback
    * 
    * @typedef {{ on: Set<ReleaseFailureCallback>, once: Set<ReleaseFailureCallback> }} ReleaseFailureListeners
    * @typedef {{ on: Set<ErrorCallback>, once: Set<ErrorCallback> }} ErrorListeners
*/
/** 
    * @param {string} apiProviderId
    * @param {number} apiUniqueNumber
    * @param {number} requestId
    * @param {ReleaseFailureListeners} releaseFailureListeners
    * @param {ErrorListeners} errorListeners
    * @returns {Promise<void>}
*/
export function releaseSemaphorePermit(apiProviderId, apiUniqueNumber, requestId, releaseFailureListeners, errorListeners) {
    return new Promise((resolve, reject) => {
        const message = {
            apiUniqueNumber,
            requestId,
            terminateOneRequest: true
        };
        const sendPermitTermination = /** @param {number} retryCount */(retryCount) => {
            if (!process.send) {
                const err = new RateLimiterError(
                    "ERR_IPC_CHANNEL_CLOSED", 
                    "Cannot release permit: The IPC channel is closed. The worker may be disconnected or orphaned."
                );
                triggerEvent(releaseFailureListeners, errorListeners, apiProviderId, requestId, err);
                return reject(err);
            };
            process.send(message, (err) => {
                if (err) {
                    if (retryCount > 3) {
                        triggerEvent(releaseFailureListeners, errorListeners, apiProviderId, requestId, err);
                        reject(err);
                    } else {
                        setTimeout(() => {
                            sendPermitTermination(retryCount + 1);
                        }, 50);
                    };
                } else {
                    resolve();
                };
            });
        };
        sendPermitTermination(0);
    });
};