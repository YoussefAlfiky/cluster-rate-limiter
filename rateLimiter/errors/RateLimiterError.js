/**
    * Custom Error class for all Rate Limiter exceptions.
    * Allows users to catch errors by `.code` instead of fragile message strings.
*/
export class RateLimiterError extends Error {
    /**
        * @param {string} code 
        * @param {string} message
    */
    constructor(code, message) {
        super(message);
        this.name = "RateLimiterError";
        /** @type {string} */
        this.code = code;
        // Keeps the internal constructor out of the user's stack trace
        if (Error.captureStackTrace) {
            Error.captureStackTrace(this, this.constructor);
        }
    }
}