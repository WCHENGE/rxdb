import { ensureRxStorageInstanceParamsAreCorrect } from "../../rx-storage-helper.js";
import { RXDB_VERSION } from "../utils/utils-rxdb-version.js";
import { RX_STORAGE_NAME_LOCALSTORAGE, createLocalstorageStorageInstance } from "./rx-storage-instance-localstorage.js";
export * from "./localstorage-mock.js";
export class RxStorageLocalstorage {
  name = RX_STORAGE_NAME_LOCALSTORAGE;
  rxdbVersion = RXDB_VERSION;
  constructor(settings) {
    this.settings = settings;
  }
  createStorageInstance(params) {
    ensureRxStorageInstanceParamsAreCorrect(params);
    var useSettings = Object.assign({}, this.settings, params.options);
    return createLocalstorageStorageInstance(this, params, useSettings);
  }
}
export function getRxStorageLocalstorage(settings = {}) {
  var storage = new RxStorageLocalstorage(settings);
  return storage;
}
//# sourceMappingURL=index.js.map