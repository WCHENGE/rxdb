import { promiseWait } from "../utils/utils-promise.js";
export var RX_STORAGE_NAME_DENOKV = 'denokv';
export function getDenoKVIndexName(index) {
  return index.join('|');
}

/**
 * Used for non-index rows that contain the document data,
 * not just a documentId
 */
export var DENOKV_DOCUMENT_ROOT_PATH = '||';
export var CLEANUP_INDEX = ['_deleted', '_meta.lwt'];

/**
 * Get the global Deno variable from globalThis.Deno
 * so that compiling with plain typescript does not fail.
 * Deno has no way to just "download" the deno typings,
 * so we have to use the "any" type here.
 */
export function getDenoGlobal() {
  return globalThis.Deno;
}
export async function commitWithRetry(buildTx) {
  var attempt = 0;
  while (true) {
    var tx = buildTx();
    try {
      return await tx.commit();
    } catch (err) {
      var locked = err && String(err.message).includes('database is locked');
      if (locked && attempt < 10) {
        attempt++;
        await promiseWait(10 * attempt);
        continue;
      }
      throw err;
    }
  }
}
//# sourceMappingURL=denokv-helper.js.map