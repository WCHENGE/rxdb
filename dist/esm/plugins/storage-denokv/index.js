import { ensureRxStorageInstanceParamsAreCorrect } from "../../rx-storage-helper.js";
import { RX_STORAGE_NAME_DENOKV } from "./denokv-helper.js";
import { createDenoKVStorageInstance } from "./rx-storage-instance-denokv.js";
import { RXDB_VERSION } from "../utils/utils-rxdb-version.js";
export class RxStorageDenoKV {
  name = RX_STORAGE_NAME_DENOKV;
  rxdbVersion = RXDB_VERSION;
  constructor(settings) {
    this.settings = settings;
  }
  createStorageInstance(params) {
    ensureRxStorageInstanceParamsAreCorrect(params);
    return createDenoKVStorageInstance(this, params, this.settings);
  }
}
export function getRxStorageDenoKV(settings = {
  consistencyLevel: 'strong'
}) {
  var storage = new RxStorageDenoKV(settings);
  return storage;
}
//# sourceMappingURL=index.js.map