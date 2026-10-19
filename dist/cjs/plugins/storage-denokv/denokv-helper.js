"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RX_STORAGE_NAME_DENOKV = exports.DENOKV_DOCUMENT_ROOT_PATH = exports.CLEANUP_INDEX = void 0;
exports.commitWithRetry = commitWithRetry;
exports.getDenoGlobal = getDenoGlobal;
exports.getDenoKVIndexName = getDenoKVIndexName;
var _utilsPromise = require("../utils/utils-promise.js");
var RX_STORAGE_NAME_DENOKV = exports.RX_STORAGE_NAME_DENOKV = 'denokv';
function getDenoKVIndexName(index) {
  return index.join('|');
}

/**
 * Used for non-index rows that contain the document data,
 * not just a documentId
 */
var DENOKV_DOCUMENT_ROOT_PATH = exports.DENOKV_DOCUMENT_ROOT_PATH = '||';
var CLEANUP_INDEX = exports.CLEANUP_INDEX = ['_deleted', '_meta.lwt'];

/**
 * Get the global Deno variable from globalThis.Deno
 * so that compiling with plain typescript does not fail.
 * Deno has no way to just "download" the deno typings,
 * so we have to use the "any" type here.
 */
function getDenoGlobal() {
  return globalThis.Deno;
}
async function commitWithRetry(buildTx) {
  var attempt = 0;
  while (true) {
    var tx = buildTx();
    try {
      return await tx.commit();
    } catch (err) {
      var locked = err && String(err.message).includes('database is locked');
      if (locked && attempt < 10) {
        attempt++;
        await (0, _utilsPromise.promiseWait)(10 * attempt);
        continue;
      }
      throw err;
    }
  }
}
//# sourceMappingURL=denokv-helper.js.map