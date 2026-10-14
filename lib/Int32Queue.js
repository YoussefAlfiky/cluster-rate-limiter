// @ts-check
/**
    * A highly optimized, infinitely growing circular queue for 32-bit integers.
    * Instantly recycles dequeued memory slots to guarantee zero dead memory footprint.
*/
export class Int32Queue {
    /** @type {Int32Array} */
    #data;
    #capacity = 0; // temp value till init
    #head = 0;
    #tail = 0;
    #size = 0;
    /** @param {number} [initialCapacity=1024] */
    constructor(initialCapacity = 1024) {
        this.#capacity = initialCapacity;
        this.#data = new Int32Array(initialCapacity);
    }
    /** @param {number} value */
    enqueue(value) {
        if (this.#size === this.#capacity) {
            this.#grow();
        }
        this.#data[this.#tail] = value;
        this.#tail = (this.#tail + 1) % this.#capacity;
        this.#size++;
    }
    /** @returns {number | undefined} */
    dequeue() {
        if (this.#size === 0) return undefined;
        const value = this.#data[this.#head];
        this.#head = (this.#head + 1) % this.#capacity;
        this.#size--;
        return value;
    }
    get size() {
        return this.#size;
    }
    clear() {
        this.#head = 0;
        this.#tail = 0;
        this.#size = 0;
    }
    #grow() {
        const newCapacity = this.#capacity * 2;
        const newData = new Int32Array(newCapacity);
        // C++ level memory copy to unroll the circular buffer into a flat array
        newData.set(this.#data.subarray(this.#head, this.#capacity), 0);
        newData.set(this.#data.subarray(0, this.#tail), this.#capacity - this.#head);
        // set the new state
        this.#data = newData;
        this.#head = 0;
        this.#tail = this.#capacity;
        this.#capacity = newCapacity;
    }
}