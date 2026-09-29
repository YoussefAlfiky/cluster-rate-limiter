//@ts-check
import { RateLimiterError } from "../errors/RateLimiterError.js";
/** 
    * @typedef {(...args: any[]) => void | Promise<void>} EventCallback 
    * @typedef {Record<string, { on: Set<EventCallback>, once: Set<EventCallback> }>} EventListenerBuckets
*/

/** @template {EventListenerBuckets} A */
export class EventListenerRegistry {
    /**
        * @readonly
        * @type {A}
    */
    #buckets;
    /** @param {A} buckets */
    constructor(buckets) {
        if (!buckets || typeof buckets !== "object") {
            throw new RateLimiterError("ERR_INVALID_BUCKETS", "EventListenerRegistry requires pre-initialized buckets.");
        };
        this.#buckets = Object.assign(Object.create(null), buckets);
    }
    /** @param {unknown} event */
    #getEventBucket(event) {
        if (typeof event !== "string") {
            throw new RateLimiterError(
                "ERR_INVALID_EVENT_TYPE", 
                `Invalid event type: expected a string, but received ${typeof event}.`
            );
        }
        const bucket = this.#buckets[event];
        if (!bucket) {
            const validEvents = Object.keys(this.#buckets).map(key => `'${key}'`).join(', ');
            throw new RateLimiterError(
                "ERR_INVALID_EVENT", 
                `Invalid event name: expected one of [${validEvents}], but received '${String(event)}'.`
            );
        };
        return bucket;
    };
    /**
        * Adds a persistent listener for the specified event.
        * 
        * @template {keyof A} K
        * @param {K} event 
        * @param {EventCallback} listener 
        * @returns {void}
    */
    on(event, listener) {
        const bucket = this.#getEventBucket(event);
        bucket.on.add(listener);
    };
    /**
        * Adds a one-time listener for the specified event.
        * 
        * @template {keyof A} K
        * @param {K} event 
        * @param {EventCallback} listener 
        * @returns {void}
    */
    once(event, listener) {
        const bucket = this.#getEventBucket(event);
        bucket.once.add(listener);
    };
    /**
        * Removes a specific listener from the specified event.
        * 
        * @template {keyof A} K
        * @param {K} event 
        * @param {EventCallback} listener 
        * @returns {void}
    */
    off(event, listener) {
        // We use the same validation here to throw if the event string is completely invalid
        const bucket = this.#getEventBucket(event);
        bucket.on.delete(listener);
        bucket.once.delete(listener);
    };
    /**
        * Removes all registered event listeners.
        * 
        * @returns {void}
    */
    clearListeners() {
        for (const event of Object.values(this.#buckets)) {
            event.on.clear();
            event.once.clear();
        };
    };
}