//@ts-check
/** 
    * @template {unknown[]} A
    * @typedef {(...args: A) => void | Promise<void>} EventCallback 
*/
/**@typedef {(err: unknown) => void | Promise<void>} ErrorCallback*/
/**
    * @template {unknown[]} A
    * @param {{ on: Set<EventCallback<A>>, once: Set<EventCallback<A>> }} eventListenersBucket
    * @param {{ on: Set<ErrorCallback>, once: Set<ErrorCallback> }} errorListenersBucket
    * @param {A} listenerArguments
*/
export function triggerEvent(eventListenersBucket, errorListenersBucket, ...listenerArguments) {
    // Helper to safely route errors to the user's `error` event handlers
    const emitSafeError = /** @param {unknown} errToEmit */(errToEmit) => {
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
    // Helper to execute event listeners and catch their errors
    const execEventCb = /** @param {EventCallback<A>} cb */(cb) => {
        try {
            const result = cb(...listenerArguments);
            if (result instanceof Promise) {
                result.catch((asyncErr) => emitSafeError(asyncErr));
            }
        } catch (syncErr) {
            emitSafeError(syncErr);
        }
    };
    // 1. Snapshot the event sets to prevent iteration mutations
    const eventOn = [...eventListenersBucket.on];
    const eventOnce = [...eventListenersBucket.once];
    // 2. Clear the 'once' bucket BEFORE execution
    if (eventOnce.length > 0) {
        eventListenersBucket.once.clear();
    }
    // 3. Execute all event listeners safely
    for (const cb of eventOn) execEventCb(cb);
    for (const cb of eventOnce) execEventCb(cb);
}