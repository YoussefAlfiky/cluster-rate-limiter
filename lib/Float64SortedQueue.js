// @ts-check
export class SortedQueueError extends Error {
    /**
     * @param {"INVALID_VALUE" | "QUEUE_FULL"} code 
     * @param {string | undefined} message 
     */
    constructor(code, message) {
        if (code !== "INVALID_VALUE" && code !== "QUEUE_FULL") {
            throw new TypeError(`Invalid error code: '${code}'. Must be INVALID_VALUE, or QUEUE_FULL.`);
        };
        if (typeof message !== "string" && typeof message !== "undefined") {
            throw new TypeError("Error message must be a string or undefined.");
        };
        super(message);
        this.name = "SortedQueueError";
        this.code = code;
    }
}

export class Float64SortedQueue {
    /** 
        * @readonly
        * @type {number} 
    */
    #capacity = 0 // temp value till init;
    /** 
        * @readonly
        * @type {Float64Array} 
    */
    #values;
    /** @type {number} */
    #headIndex= 0;
    /** @type {number} */
    #tailIndex = 0; // Insertion point physical index
    /** @type {number} */
    #size = 0;
    /** @param {number} capacity The fixed maximum number of items this queue can hold. */
    constructor(capacity) {
        if (!Number.isInteger(capacity) || capacity <= 0) {
            throw new SortedQueueError(
                "INVALID_VALUE",
                "capacity must be a positive safe integer"
            );
        }
        this.#capacity = capacity;
        this.#values = new Float64Array(capacity);
    }
    /** @param {number} item */
    enqueue(item) {
        if (this.#size === this.#capacity) {
            throw new SortedQueueError("QUEUE_FULL", "Cannot enqueue: queue capacity reached");
        }
        // Insert directly into raw memory using the physical tail index
        this.#values[this.#tailIndex] = item;
        this.#tailIndex = (this.#tailIndex + 1) % this.#capacity;
        this.#size++;
    }
    /** @returns {number | undefined} */
    dequeue() {
        if (this.#size === 0) return undefined;
        const item = this.#values[this.#headIndex];
        this.#headIndex = (this.#headIndex + 1) % this.#capacity;
        this.#size--;
        return item;
    }
    clear() {
        this.#headIndex = 0;
        this.#tailIndex = 0;
        this.#size = 0;
    }
    /** @returns {number} */
    get size() {
        return this.#size;
    }
    /** @returns {number | undefined} */
    peek() {
        if (this.#size === 0) return undefined;
        return this.#values[this.#headIndex];
    }
    /** 
        * @param {number} target
        * @returns {number}
    */
    #binarySearchLowerBound(target) {
        if (this.#size === 0) return 0;
        let leftLogicalIndex = 0;
        let rightLogicalIndex = this.#size;
        // Binary search using logical indices mapped to physical ring-buffer indices
        while (leftLogicalIndex < rightLogicalIndex) {
            const midLogicalIndex = leftLogicalIndex + ((rightLogicalIndex - leftLogicalIndex) >> 1);
            const midPhysicalIndex = (this.#headIndex + midLogicalIndex) % this.#capacity;
            if (this.#values[midPhysicalIndex] < target) {
                leftLogicalIndex = midLogicalIndex + 1;
            } else {
                rightLogicalIndex = midLogicalIndex;
            }
        }
        return leftLogicalIndex;
    }
    /** @param {number} target */
    removeBefore(target) {
        if (this.#size === 0) return;
        // The logical lower bound index is exactly the number of items strictly less than the target
        const itemsToRemoveCount = this.#binarySearchLowerBound(target);
        if (itemsToRemoveCount === 0) return; // All items are >= target, nothing to remove
        if (itemsToRemoveCount === this.#size) {
            this.clear(); // All items are < target
            return;
        }
        // Just shift the head index forward by the number of elements removed. Zero object deletion overhead.
        this.#headIndex = (this.#headIndex + itemsToRemoveCount) % this.#capacity;
        this.#size -= itemsToRemoveCount;
    }
}