import { ensureRxStorageInstanceParamsAreCorrect } from "../../rx-storage-helper.js";
import { RX_STORAGE_NAME_MONGODB } from "./mongodb-helper.js";
import { createMongoDBStorageInstance } from "./rx-storage-instance-mongodb.js";
import { RXDB_VERSION } from "../utils/utils-rxdb-version.js";
export class RxStorageMongoDB {
  name = RX_STORAGE_NAME_MONGODB;
  rxdbVersion = RXDB_VERSION;
  constructor(databaseSettings) {
    this.databaseSettings = databaseSettings;
  }
  createStorageInstance(params) {
    ensureRxStorageInstanceParamsAreCorrect(params);
    return createMongoDBStorageInstance(this, params, this.databaseSettings);
  }
}
export function getRxStorageMongoDB(databaseSettings) {
  var storage = new RxStorageMongoDB(databaseSettings);
  return storage;
}
//# sourceMappingURL=rx-storage-mongodb.js.map