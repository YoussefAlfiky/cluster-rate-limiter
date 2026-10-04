//@ts-check

/**
    * A unique "magic number" signature placed at index 0 of hot-path IPC arrays.
    * 
    * This acts as a strict firewall to prevent cross-library collisions on the shared 
    * Node.js cluster event bus. It guarantees that the router instantly drops foreign 
    * arrays and only processes authentic rate limiter packets.
    * 
    * @constant
    * @type {number}
*/
export const IPC_SIGNATURE = 469262976;

/**
    * A highly unique integer signature to prevent cross-library IPC collisions.
    * @constant
    * @type {number}
*/
export const IPC_MESSAGE_LENGTH = 4;

/**
    * IPC Operation Codes
    * @readonly
    * @enum {number}
*/
export const IPC_OPCODES = {
    ACQUIRE: 0,
    GRANT: 1,
    RELEASE: 2
};

/**
    * Tuple Index Maps (For extreme readability in routers)
    * @readonly
    * @enum {number}
*/
export const IPC_INDEX = {
    SIGNATURE: 0,
    DESTINATION: 1,
    OPCODE: 2,
    PAYLOAD: 3
};