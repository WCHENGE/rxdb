import { MongoClient } from 'mongodb';
import { RXDB_VERSION, flatClone } from "../utils/index.js";
import { getPrimaryFieldOfPrimaryKey } from "../../rx-schema-helper.js";
export var RX_STORAGE_NAME_MONGODB = 'mongodb';
export var MONGO_OPTIONS_DRIVER_INFO = {
  driverInfo: {
    name: 'RxDB',
    version: RXDB_VERSION
  }
};

/**
 * MongoDB uses the _id field by itself (max 12 bytes)
 * so we have to substitute the _id field if
 * it is used in the RxDocType.
 */
export var MONGO_ID_SUBSTITUTE_FIELDNAME = '__id';
export function primarySwapMongoDBQuerySelector(primaryKey, selector) {
  selector = flatClone(selector);
  if (primaryKey !== '_id') {
    return selector;
  }
  if (Array.isArray(selector)) {
    return selector.map(item => primarySwapMongoDBQuerySelector(primaryKey, item));
  } else if (typeof selector === 'object') {
    var ret = {};
    Object.entries(selector).forEach(([k, v]) => {
      if (k === primaryKey) {
        ret._id = v;
      } else {
        if (k.startsWith('$')) {
          ret[k] = primarySwapMongoDBQuerySelector(primaryKey, v);
        } else {
          ret[k] = v;
        }
      }
    });
    return ret;
  } else {
    return selector;
  }
}
export function prepareMongoDBQuery(schema, mutateableQuery) {
  var primaryKey = getPrimaryFieldOfPrimaryKey(schema.primaryKey);
  var preparedQuery = {
    query: mutateableQuery,
    mongoSelector: primarySwapMongoDBQuerySelector(primaryKey, mutateableQuery.selector),
    mongoSort: swapToMongoSort(mutateableQuery.sort)
  };
  return preparedQuery;
}
;
export function swapMongoToRxDoc(docData) {
  docData = flatClone(docData);
  if (docData[MONGO_ID_SUBSTITUTE_FIELDNAME]) {
    var value = docData[MONGO_ID_SUBSTITUTE_FIELDNAME];
    delete docData[MONGO_ID_SUBSTITUTE_FIELDNAME];
    docData._id = value;
  } else {
    delete docData._id;
  }
  return docData;
}
export function swapRxDocToMongo(docData) {
  docData = flatClone(docData);
  if (docData._id) {
    var value = docData._id;
    delete docData._id;
    docData[MONGO_ID_SUBSTITUTE_FIELDNAME] = value;
  }
  return docData;
}
export function swapToMongoSort(sort) {
  var ret = {};
  sort.forEach(sortPart => {
    var [key, direction] = Object.entries(sortPart)[0];
    var mongoKey = key === '_id' ? MONGO_ID_SUBSTITUTE_FIELDNAME : key;
    var mongoDirection = direction === 'asc' ? 1 : -1;
    ret[mongoKey] = mongoDirection;
  });
  return ret;
}
export function getMongoDBIndexName(index) {
  return index.join('|');
}
export var MONGO_CLIENT_CACHE = new Map();
export async function getMongoDBClient(connection) {
  var cached = MONGO_CLIENT_CACHE.get(connection);
  if (!cached) {
    var client = new MongoClient(connection, MONGO_OPTIONS_DRIVER_INFO);
    cached = {
      client,
      refCount: 1,
      promise: client.connect().then(() => client)
    };
    MONGO_CLIENT_CACHE.set(connection, cached);
    return cached.promise;
  }
  cached.refCount++;
  if (cached.closeTimeout) {
    clearTimeout(cached.closeTimeout);
    cached.closeTimeout = undefined;
  }
  return cached.promise;
}
export async function closeMongoDBClient(connection) {
  var cached = MONGO_CLIENT_CACHE.get(connection);
  if (cached) {
    cached.refCount--;
    if (cached.refCount === 0) {
      if (cached.closeTimeout) {
        clearTimeout(cached.closeTimeout);
      }
      cached.closeTimeout = setTimeout(() => {
        var innerCached = MONGO_CLIENT_CACHE.get(connection);
        if (innerCached && innerCached.refCount === 0) {
          MONGO_CLIENT_CACHE.delete(connection);
          innerCached.client.close().catch(() => {});
        }
      }, 2000); // 2 second delay to allow reuse
    }
  }
}
//# sourceMappingURL=mongodb-helper.js.map