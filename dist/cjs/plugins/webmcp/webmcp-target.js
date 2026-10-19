"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getWebMCPTargetFromCollection = getWebMCPTargetFromCollection;
var _index = require("../utils/index.js");
var _index2 = require("../replication/index.js");
var _rxjs = require("rxjs");
var _rxStorageHelper = require("../../rx-storage-helper.js");
/**
 * Builds the target that runs the WebMCP tools directly
 * against a local RxCollection.
 */
function getWebMCPTargetFromCollection(collection) {
  var database = collection.database;
  return {
    databaseName: database.name,
    collectionName: collection.name,
    schemaVersion: collection.schema.version,
    primaryPath: collection.schema.primaryPath,
    jsonSchema: collection.schema.getJsonSchemaWithoutMeta(),
    awaitInSync: async () => {
      var replicationStates = (0, _index.getFromMapOrCreate)(_index2.REPLICATION_STATE_BY_COLLECTION, collection, () => []);
      await Promise.all(replicationStates.map(replicationState => {
        if (!replicationState.isStopped()) {
          return replicationState.awaitInSync();
        }
      }));
    },
    query: async query => {
      var docs = await collection.find(query).exec();
      return docs.map(d => d.toJSON());
    },
    count: async query => {
      return await collection.count(query).exec();
    },
    changesSince: async (limit, checkpoint) => {
      return await (0, _rxStorageHelper.getChangedDocumentsSince)(collection.storageInstance, limit, checkpoint);
    },
    awaitChange: async () => {
      await (0, _rxjs.firstValueFrom)(collection.eventBulks$);
    },
    insert: async document => {
      var doc = await collection.insert(document);
      return doc.toJSON();
    },
    upsert: async document => {
      var doc = await collection.upsert(document);
      return doc.toJSON();
    },
    remove: async id => {
      var doc = await collection.findOne(id).exec();
      if (!doc) {
        return undefined;
      }
      var deletedDoc = await doc.remove();
      return deletedDoc.toJSON();
    },
    onClose: fn => {
      collection.onClose.push(fn);
    }
  };
}
//# sourceMappingURL=webmcp-target.js.map