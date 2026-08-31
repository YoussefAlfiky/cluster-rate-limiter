// @ts-check

/** @template T */
class QueueNode {
    /** 
        * @param {QueueNode<T> | null} nextNode 
        * @param {NonNullable<T>} data 
    */
    constructor(nextNode, data) {
        if ( nextNode !== null && !(nextNode instanceof QueueNode) ) {
            throw new TypeError("Invalid node reference: nextNode must be instances of QueueNode or null.");
        };
        if (data === undefined || data === null) {
            throw new Error("Invalid item: Queue cannot store undefined or null");
        };
        this.nextNode = nextNode
        this.data = data
    };
};

/** @template T */
export class Queue {
    /** @type {number} */
    #length = 0;
    /** @type {QueueNode<T> | null} */
    #headNode = null;
    /** @type {QueueNode<T> | null} */
    #tailNode = null;
    /** @param {NonNullable<T>} item */
    enqueue(item) {
        const newNode = new QueueNode(null, item);
        if (this.#tailNode) {
            this.#tailNode.nextNode = newNode;
            this.#tailNode = newNode;
        } else {
            this.#headNode = this.#tailNode = newNode;
        };
        this.#length++;
    };
    /** @returns {NonNullable<T> | undefined} */
    dequeue(){
        if (!this.#headNode) return;
        const removedNode = this.#headNode;
        const next = removedNode.nextNode;
        if (next) {
            this.#headNode = next;
        } else {
            this.#headNode = this.#tailNode = null;
        };
        this.#length--;
        return removedNode.data;
    };
    //
    clear() {
        this.#headNode = this.#tailNode = null;
        this.#length = 0;
    };
    /** @returns {number} */
    get size() {
        return this.#length;
    };
    /** @returns {NonNullable<T> | undefined} */
    peek() {
        return this.#headNode?.data;
    };
};