/*
 * fw/engine.js — server-side re-export of the fwtozza engine.
 * The implementation lives in public/fw/engine.js: the browser can only
 * import files under the web root, while Node can import from anywhere —
 * a single source avoids any drift between the two sides.
 */
export * from "../public/fw/engine.js";
