// @ts-check
export class SortedQueueError extends Error {
    /**
     * @param {"WRONG_ORDER" | "INVALID_VALUE" | "QUEUE_FULL"} code 
     * @param {string | undefined} message 
     */
    constructor(code, message) {
        if (code !== "WRONG_ORDER" && code !== "INVALID_VALUE" && code !== "QUEUE_FULL") {
            throw new TypeError(`Invalid error code: '${code}'. Must be WRONG_ORDER, INVALID_VALUE, or QUEUE_FULL.`);
        };
        if (typeof message !== "string" && typeof message !== "undefined") {
            throw new TypeError("Error message must be a string or undefined.");
        };
        super(message);
        this.name = "SortedQueueError";
        this.code = code;
        Object.setPrototypeOf(this, SortedQueueError.prototype); // keeps instanceof working if compiled to ES5
    }
}

export class Float64SortedQueue {
    /** 
        * @readonly
        * @type {number} 
    */
    #capacity;
    /** @type {number} */
    #headIndex= 0;
    /** @type {number} */
    #tailIndex = 0; // Insertion point physical index
    /** @type {number} */
    #size = 0;
    // Float64Array for hyper-fast, cache-friendly storage of numeric timestamps
    /** 
        * @readonly
        * @type {Float64Array} 
    */
    #values;
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
        if (!Number.isFinite(item)) {
            throw new SortedQueueError("INVALID_VALUE", "Item must be a finite number");
        }
        if (this.#size === this.#capacity) {
            throw new SortedQueueError("QUEUE_FULL", "Cannot enqueue: queue capacity reached");
        }
        if (this.#size > 0) {
            // Calculate the physical index of the most recently inserted item
            const lastPhysicalIndex = (this.#tailIndex - 1 + this.#capacity) % this.#capacity;
            if (item < this.#values[lastPhysicalIndex]) {
                throw new SortedQueueError("WRONG_ORDER", "The order of item passed to enqueue method must be >= previous Item's order");
            }
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
        * @returns {{logicalIndex: number | null, lowerBoundLogicalIndex: number}}
    */
    #binarySearch(target) {
        if (Number.isNaN(target)) throw new SortedQueueError("INVALID_VALUE", "target can't be NaN");
        if (this.#size === 0) return { logicalIndex: null, lowerBoundLogicalIndex: 0 };
        let leftLogicalIndex = 0;
        let rightLogicalIndex = this.#size;
        // Binary search using logical indices mapped to physical ring-buffer indices
        while (leftLogicalIndex < rightLogicalIndex) {
            const midLogicalIndex = leftLogicalIndex + Math.floor((rightLogicalIndex - leftLogicalIndex) / 2);
            const midPhysicalIndex = (this.#headIndex + midLogicalIndex) % this.#capacity;
            const value = this.#values[midPhysicalIndex];
            if (value < target) {
                leftLogicalIndex = midLogicalIndex + 1;
            } else {
                rightLogicalIndex = midLogicalIndex;
            }
        }
        const lowerBoundLogicalIndex = leftLogicalIndex;
        /** @type {number | null} */
        let exactMatchLogicalIndex = null;
        if (lowerBoundLogicalIndex < this.#size) {
            const lowerBoundPhysicalIndex = (this.#headIndex + lowerBoundLogicalIndex) % this.#capacity;
            if (this.#values[lowerBoundPhysicalIndex] === target) {
                exactMatchLogicalIndex = lowerBoundLogicalIndex;
            }
        }
        // Returns logical indices (0 to size)
        return { 
            logicalIndex: exactMatchLogicalIndex, 
            lowerBoundLogicalIndex 
        };
    }
    /** @param {number} target */
    removeBefore(target) {
        if (this.#size === 0) return;
        // The logical lower bound index is exactly the number of items strictly less than the target
        const { lowerBoundLogicalIndex: itemsToRemoveCount } = this.#binarySearch(target);
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