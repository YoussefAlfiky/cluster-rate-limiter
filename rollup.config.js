export default {
    input: "index.js",
    external: [/^node:/], 
    output: [
        {
            file: "dist/index.js",
            format: "es",
        },
        {
            file: "dist/index.cjs",
            format: "cjs",
        }
    ]
};