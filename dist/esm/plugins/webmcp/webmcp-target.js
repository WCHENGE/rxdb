import { getFromMapOrCreate } from "../utils/index.js";
import { REPLICATION_STATE_BY_COLLECTION } from "../replication/index.js";
import { firstValueFrom } from 'rxjs';
import { getChangedDocumentsSince } from "../../rx-storage-helper.js";

/**
 * Builds the target that runs the WebMCP tools directly
 * against a local RxCollection.
 */
export function getWebMCPTargetFromCollection(collection) {
  var database = collection.database;
  return {
    databaseName: database.name,
    collectionName: collection.name,
    schemaVersion: collection.schema.version,
    primaryPath: collection.schema.primaryPath,
    jsonSchema: collection.schema.getJsonSchemaWithoutMeta(),
    awaitInSync: async () => {
      var replicationStates = getFromMapOrCreate(REPLICATION_STATE_BY_COLLECTION, collection, () => []);
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
      return await getChangedDocumentsSince(collection.storageInstance, limit, checkpoint);
    },
    awaitChange: async () => {
      await firstValueFrom(collection.eventBulks$);
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