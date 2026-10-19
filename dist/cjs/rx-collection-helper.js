"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.createRxCollectionStorageInstance = createRxCollectionStorageInstance;
exports.ensureRxCollectionIsNotClosed = ensureRxCollectionIsNotClosed;
exports.fillObjectDataBeforeInsert = fillObjectDataBeforeInsert;
exports.isWriteAllowed = isWriteAllowed;
exports.normalizeInlineAttachments = normalizeInlineAttachments;
exports.removeCollectionStorages = removeCollectionStorages;
var _index = require("./plugins/utils/index.js");
var _rxSchemaHelper = require("./rx-schema-helper.js");
var _hooks = require("./hooks.js");
var _rxDatabaseInternalStore = require("./rx-database-internal-store.js");
var _rxStorageHelper = require("./rx-storage-helper.js");
var _overwritable = require("./overwritable.js");
var _rxError = require("./rx-error.js");
/**
 * fills in the default data.
 * This also clones the data.
 */
function fillObjectDataBeforeInsert(schema, data, skipPrimaryKeyFill = false) {
  data = (0, _index.flatClone)(data);
  data = (0, _rxSchemaHelper.fillObjectWithDefaults)(schema, data);
  if (!skipPrimaryKeyFill && typeof schema.jsonSchema.primaryKey !== 'string') {
    data = (0, _rxSchemaHelper.fillPrimaryKey)(schema.primaryPath, schema.jsonSchema, data);
  }
  /**
   * _meta and _rev are not set here because
   * they are always overwritten by the wrapped storage instance
   * in getWrappedStorageInstance() before the actual write.
   * Skipping them here avoids unnecessary object allocations on the hot path.
   *
   * _deleted and _attachments still need to be initialized here because
   * the wrapped storage does NOT set them, and they are required
   * for the document to be valid before the storage write
   * (e.g. _attachments is checked during attachment normalization in bulkInsert).
   */
  if (!('_deleted' in data)) {
    data._deleted = false;
  }
  if (!('_attachments' in data)) {
    data._attachments = {};
  }
  return data;
}

/**
 * Normalizes inline attachment inputs on a document's _attachments.
 * Accepts an array of { id, type, data } objects (aligned with putAttachment API)
 * and converts to the internal map format { [id]: { type, data, digest, length } }.
 * For each entry where data is a Blob and digest is missing,
 * computes digest via hashFunction and sets length from Blob.size.
 * Already-complete RxAttachmentWriteData entries are left untouched.
 */
async function normalizeInlineAttachments(hashFunction, attachments) {
  // Guard against null/undefined/non-object values
  if (attachments == null || typeof attachments !== 'object') {
    throw (0, _rxError.newRxError)('COL24', {
      data: attachments
    });
  }
  var entries;
  // Only accept array format for inline attachments.
  // An empty object {} (set by fillObjectDataBeforeInsert) is also valid.
  if (Array.isArray(attachments)) {
    var attachmentMap = {};
    for (var att of attachments) {
      if (!att || typeof att.id !== 'string' || att.id.length === 0 || typeof att.type !== 'string' || att.type.length === 0 || !(att.data instanceof Blob)) {
        throw (0, _rxError.newRxError)('AT2', {
          obj: att
        });
      }
      if (Object.prototype.hasOwnProperty.call(attachmentMap, att.id)) {
        throw (0, _rxError.newRxError)('AT3', {
          obj: att
        });
      }
      attachmentMap[att.id] = {
        type: att.type,
        data: att.data
      };
    }
    entries = Object.entries(attachmentMap);
    await Promise.all(entries.map(async ([, att]) => {
      if (att.data instanceof Blob && !att.digest) {
        att.digest = await hashFunction(att.data);
        att.length = att.data.size;
      }
    }));
    return attachmentMap;
  }

  // Empty object from fillObjectDataBeforeInsert — pass through
  if (typeof attachments === 'object' && Object.keys(attachments).length === 0) {
    return attachments;
  }

  // Already-normalized map (from internal paths like bulkUpsert's 409 handler)
  // where entries already have digest/length — pass through
  entries = Object.entries(attachments);
  var allNormalized = entries.every(([, att]) => att.digest);
  if (allNormalized) {
    return attachments;
  }
  throw (0, _rxError.newRxError)('COL24', {
    data: attachments
  });
}

/**
 * Creates the storage instances that are used internally in the collection
 */
async function createRxCollectionStorageInstance(rxDatabase, storageInstanceCreationParams) {
  storageInstanceCreationParams.multiInstance = rxDatabase.multiInstance;
  var storageInstance = await rxDatabase.storage.createStorageInstance(storageInstanceCreationParams);
  return storageInstance;
}

/**
 * Removes the main storage of the collection
 * and all connected storages like the ones from the replication meta etc.
 */
async function removeCollectionStorages(storage, databaseInternalStorage, databaseInstanceToken, databaseName, collectionName, multiInstance, password,
/**
 * If no hash function is provided,
 * we assume that the whole internal store is removed anyway
 * so we do not have to delete the meta documents.
 */
hashFunction) {
  var allCollectionMetaDocs = await (0, _rxDatabaseInternalStore.getAllCollectionDocuments)(databaseInternalStorage);
  var relevantCollectionMetaDocs = allCollectionMetaDocs.filter(metaDoc => metaDoc.data.name === collectionName);
  var removeStorages = [];
  relevantCollectionMetaDocs.forEach(metaDoc => {
    removeStorages.push({
      collectionName: metaDoc.data.name,
      schema: metaDoc.data.schema,
      isCollection: true
    });
    metaDoc.data.connectedStorages.forEach(row => removeStorages.push({
      collectionName: row.collectionName,
      isCollection: false,
      schema: row.schema
    }));
  });

  // ensure uniqueness
  var alreadyAdded = new Set();
  removeStorages = removeStorages.filter(row => {
    var key = row.collectionName + '||' + row.schema.version;
    if (alreadyAdded.has(key)) {
      return false;
    } else {
      alreadyAdded.add(key);
      return true;
    }
  });

  // remove all the storages
  await Promise.all(removeStorages.map(async row => {
    var storageInstance = await storage.createStorageInstance({
      collectionName: row.collectionName,
      databaseInstanceToken,
      databaseName,
      /**
       * multiInstance must be set to true if multiInstance
       * was true on the database
       * so that the storageInstance can inform other
       * instances about being removed.
       */
      multiInstance,
      options: {},
      schema: row.schema,
      password,
      devMode: _overwritable.overwritable.isDevMode()
    });
    await storageInstance.remove();
    if (row.isCollection) {
      await (0, _hooks.runAsyncPluginHooks)('postRemoveRxCollection', {
        storage,
        databaseName: databaseName,
        collectionName
      });
    }
  }));

  // remove the meta documents
  if (hashFunction) {
    var writeRows = relevantCollectionMetaDocs.map(doc => {
      var writeDoc = (0, _rxStorageHelper.flatCloneDocWithMeta)(doc);
      writeDoc._deleted = true;
      writeDoc._meta.lwt = (0, _index.now)();
      writeDoc._rev = (0, _index.createRevision)(databaseInstanceToken, doc);
      return {
        previous: doc,
        document: writeDoc
      };
    });
    await databaseInternalStorage.bulkWrite(writeRows, 'rx-database-remove-collection-all');
  }
}
function ensureRxCollectionIsNotClosed(collection) {
  if (collection.closed) {
    throw (0, _rxError.newRxError)('COL21', {
      collection: collection.name,
      version: collection.schema.version
    });
  }
}

/**
 * Asserts that a write to the given collection is currently allowed.
 * Throws if the collection is closed or if a schema migration is
 * pending or running, in which cases external writes would either
 * fail or conflict with the migration replication.
 */
function isWriteAllowed(collection) {
  if (collection.closed) {
    throw (0, _rxError.newRxError)('COL21', {
      collection: collection.name,
      version: collection.schema.version
    });
  }
  if (collection.migrationInProgress) {
    throw (0, _rxError.newRxError)('COL25', {
      collection: collection.name,
      version: collection.schema.version
    });
  }
}
//# sourceMappingURL=rx-collection-helper.js.map