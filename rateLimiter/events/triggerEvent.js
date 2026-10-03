//@ts-check
/** 
    * @template {unknown[]} A
    * @typedef {(...args: A) => void | Promise<void>} EventCallback 
*/
/**@typedef {(err: unknown) => void | Promise<void>} ErrorCallback*/

/**
    * Safely routes an error to registered error listeners.
    * 
    * If no error listeners are attached, it falls back to emitting a Node.js process warning.
    * 
    * @param {{ on: Set<ErrorCallback>, once: Set<ErrorCallback> }} errorListenersBucket
    * The bucket containing the `on` and `once` error callbacks to execute.
    * 
    * @param {unknown} errToEmit
    * The error instance or rejection reason to pass to the listeners.
    * 
    * @returns {void}
*/
export function emitSafeError(errorListenersBucket, errToEmit) {
    // 1. Snapshot the sets to prevent infinite loops if listeners mutate the sets
    const errorOn = [...errorListenersBucket.on];
    const errorOnce = [...errorListenersBucket.once];
    const hasErrorListeners = errorOn.length > 0 || errorOnce.length > 0;
    // Standard Node.js behavior: Warn if no one is listening to "error"
    if (!hasErrorListeners) {
        process.emitWarning(
            errToEmit instanceof Error ? errToEmit.message : String(errToEmit),
            "RateLimiterWarning",
            "An unhandled background error occurred."
        );
        return;
    }
    // 2. Clear the 'once' bucket BEFORE execution
    if (errorOnce.length > 0) {
        errorListenersBucket.once.clear();
    }
    const execErrorCb = /** @param {ErrorCallback} cb */(cb) => {
        try {
            const result = cb(errToEmit);
            if (result instanceof Promise) {
                result.catch((asyncErr) => {
                    // Prevent recursive loops by strictly emitting a warning here
                    process.emitWarning(
                        asyncErr instanceof Error ? asyncErr.message : String(asyncErr),
                        "RateLimiterWarning",
                        "An async 'error' listener rejected with an exception."
                    );
                });
            }
        } catch (cbErr) {
            // Prevent recursive loops by strictly emitting a warning here
            process.emitWarning(
                cbErr instanceof Error ? cbErr.message : String(cbErr),
                "RateLimiterWarning",
                "An 'error' listener threw a synchronous exception."
            );
        }
    };
    for (const cb of errorOn) execErrorCb(cb);
    for (const cb of errorOnce) execErrorCb(cb);
};

/** 
    * Helper to execute event listeners and catch their errors
    * @template {unknown[]} A
    * @param {EventCallback<A>} cb 
    * @param {{ on: Set<ErrorCallback>, once: Set<ErrorCallback> }} errorListenersBucket
    * @param {A} listenerArguments
*/
function execEventCb(cb, errorListenersBucket, ...listenerArguments) {
    try {
        const result = cb(...listenerArguments);
        if (result instanceof Promise) {
            result.catch((asyncErr) => emitSafeError(errorListenersBucket, asyncErr));
        }
    } catch (syncErr) {
        emitSafeError(errorListenersBucket, syncErr);
    }
};

/**
    * Safely executes all registered listeners for a specific event.
    * 
    * @template {unknown[]} A
    * 
    * @param {{ on: Set<EventCallback<A>>, once: Set<EventCallback<A>> }} eventListenersBucket
    * The target event bucket containing the `on` and `once` callbacks to execute.
    * 
    * @param {{ on: Set<ErrorCallback>, once: Set<ErrorCallback> }} errorListenersBucket
    * The fallback error bucket used to catch and route exceptions thrown by the event callbacks themselves.
    * 
    * @param {A} listenerArguments
    * The arguments to pass to each executed event callback.
    * 
    * @returns {boolean}
    * Returns `true` if at least one listener was registered and executed. Returns `false` 
    * if the event bucket was completely empty
*/
export function triggerEvent(eventListenersBucket, errorListenersBucket, ...listenerArguments) {
    // 1. Snapshot the event sets to prevent iteration mutations
    const eventOn = [...eventListenersBucket.on];
    const eventOnce = [...eventListenersBucket.once];
    // 2. Clear the 'once' bucket BEFORE execution
    if (eventOnce.length === 0) {
        if (eventOn.length === 0) return false;
    } else {
        eventListenersBucket.once.clear();
    }
    // 3. Execute all event listeners safely
    for (const cb of eventOn) execEventCb(cb, errorListenersBucket, ...listenerArguments);
    for (const cb of eventOnce) execEventCb(cb, errorListenersBucket, ...listenerArguments);
    return true;
}