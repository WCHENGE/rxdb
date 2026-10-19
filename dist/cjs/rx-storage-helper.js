"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RX_DATABASE_LOCAL_DOCS_STORAGE_NAME = exports.RX_COLLECTION_BULK_INSERT_CONTEXT = exports.INTERNAL_STORAGE_NAME = void 0;
exports.attachmentWriteDataToNormalData = attachmentWriteDataToNormalData;
exports.categorizeBulkWriteRows = categorizeBulkWriteRows;
exports.ensureRxStorageInstanceParamsAreCorrect = ensureRxStorageInstanceParamsAreCorrect;
exports.flatCloneDocWithMeta = flatCloneDocWithMeta;
exports.getChangedDocumentsSince = getChangedDocumentsSince;
exports.getChangedDocumentsSinceQuery = getChangedDocumentsSinceQuery;
exports.getSingleDocument = getSingleDocument;
exports.getWrappedStorageInstance = getWrappedStorageInstance;
exports.getWrittenDocumentsFromBulkWriteResponse = getWrittenDocumentsFromBulkWriteResponse;
exports.hasEncryption = hasEncryption;
exports.observeSingle = observeSingle;
exports.randomDelayStorage = randomDelayStorage;
exports.registerMutableWriteContext = registerMutableWriteContext;
exports.stackCheckpoints = stackCheckpoints;
exports.stripAttachmentsDataFromDocument = stripAttachmentsDataFromDocument;
exports.stripAttachmentsDataFromRow = stripAttachmentsDataFromRow;
exports.throwIfIsStorageWriteError = throwIfIsStorageWriteError;
exports.writeSingle = writeSingle;
var _overwritable = require("./overwritable.js");
var _rxError = require("./rx-error.js");
var _rxSchemaHelper = require("./rx-schema-helper.js");
var _index4 = require("./plugins/utils/index.js");
var _rxjs = require("rxjs");
var _rxQueryHelper = require("./rx-query-helper.js");
var _hooks = require("./hooks.js");
/**
 * Helper functions for accessing the RxStorage instances.
 */

var INTERNAL_STORAGE_NAME = exports.INTERNAL_STORAGE_NAME = 'db_internal';
var RX_DATABASE_LOCAL_DOCS_STORAGE_NAME = exports.RX_DATABASE_LOCAL_DOCS_STORAGE_NAME = 'storage_local';

/**
 * Context string used by RxCollection.bulkInsert().
 * Documents written with this context are already cloned
 * by fillObjectDataBeforeInsert(), so the wrapped storage
 * can safely mutate them in place instead of cloning again.
 */
var RX_COLLECTION_BULK_INSERT_CONTEXT = exports.RX_COLLECTION_BULK_INSERT_CONTEXT = 'rx-collection-bulk-insert';

/**
 * Set of bulkWrite context strings whose documents
 * are already cloned by the caller and can be safely
 * mutated in place (skip flatClone in the insert path).
 *
 * Plugins can register additional contexts via
 * registerMutableWriteContext().
 */
var MUTABLE_DOCUMENT_WRITE_CONTEXTS = new Set([RX_COLLECTION_BULK_INSERT_CONTEXT]);

/**
 * Register a bulkWrite context string as "mutable",
 * meaning the caller guarantees that insert documents
 * are already cloned and safe to mutate in place.
 * This allows the wrapped storage to skip a redundant
 * flatClone() call on the insert hot path.
 */
function registerMutableWriteContext(context) {
  MUTABLE_DOCUMENT_WRITE_CONTEXTS.add(context);
}
async function getSingleDocument(storageInstance, documentId) {
  var results = await storageInstance.findDocumentsById([documentId], false);
  var doc = results[0];
  if (doc) {
    return doc;
  } else {
    return undefined;
  }
}

/**
 * Writes a single document,
 * throws RxStorageBulkWriteError on failure
 */
async function writeSingle(instance, writeRow, context) {
  var writeResult = await instance.bulkWrite([writeRow], context);
  if (writeResult.error.length > 0) {
    var error = writeResult.error[0];
    throw error;
  } else {
    var primaryPath = (0, _rxSchemaHelper.getPrimaryFieldOfPrimaryKey)(instance.schema.primaryKey);
    var success = getWrittenDocumentsFromBulkWriteResponse(primaryPath, [writeRow], writeResult);
    var ret = success[0];
    return ret;
  }
}

/**
 * Observe the plain document data of a single document.
 * Do not forget to unsubscribe.
 */
function observeSingle(storageInstance, documentId) {
  var firstFindPromise = getSingleDocument(storageInstance, documentId);
  var ret = storageInstance.changeStream().pipe((0, _rxjs.map)(evBulk => evBulk.events.find(ev => ev.documentId === documentId)), (0, _rxjs.filter)(ev => !!ev), (0, _rxjs.map)(ev => Promise.resolve((0, _index4.ensureNotFalsy)(ev).documentData)), (0, _rxjs.startWith)(firstFindPromise), (0, _rxjs.switchMap)(v => v), (0, _rxjs.filter)(v => !!v));
  return ret;
}

/**
 * Checkpoints must be stackable over another.
 * This is required form some RxStorage implementations
 * like the sharding plugin, where a checkpoint only represents
 * the document state from some, but not all shards.
 */
function stackCheckpoints(checkpoints) {
  return Object.assign({}, ...checkpoints.filter(x => !!x));
}
function throwIfIsStorageWriteError(collection, documentId, writeData, error) {
  if (error) {
    if (error.status === 409) {
      throw (0, _rxError.newRxError)('CONFLICT', {
        collection: collection.name,
        id: documentId,
        writeError: error,
        data: writeData
      });
    } else if (error.status === 422) {
      throw (0, _rxError.newRxError)('VD2', {
        collection: collection.name,
        id: documentId,
        writeError: error,
        data: writeData
      });
    } else {
      throw error;
    }
  }
}

/**
 * Use a counter-based event bulk ID instead of randomToken()
 * for better performance. The prefix ensures uniqueness across instances.
 */
var EVENT_BULK_ID_PREFIX = (0, _index4.randomToken)(10);
var eventBulkCounter = 0;
function nextEventBulkId() {
  return EVENT_BULK_ID_PREFIX + ++eventBulkCounter;
}

/**
 * Analyzes a list of BulkWriteRows and determines
 * which documents must be inserted, updated or deleted
 * and which events must be emitted and which documents cause a conflict
 * and must not be written.
 * Used as helper inside of some RxStorage implementations.
 * @hotPath The performance of this function is critical
 */
function categorizeBulkWriteRows(storageInstance, primaryPath,
/**
 * Current state of the documents
 * inside of the storage. Used to determine
 * which writes cause conflicts.
 * This must be a Map for better performance.
 */
docsInDb,
/**
 * The write rows that are passed to
 * RxStorageInstance().bulkWrite().
 */
bulkWriteRows, context,
/**
 * Used by some storages for better performance.
 * For example when get-by-id and insert/update can run in parallel.
 */
onInsert, onUpdate) {
  var hasAttachments = !!storageInstance.schema.attachments;
  var bulkInsertDocs = [];
  var bulkUpdateDocs = [];
  var errors = [];
  var eventBulkId = nextEventBulkId();
  var eventBulk = {
    id: eventBulkId,
    events: [],
    checkpoint: null,
    context
  };
  var eventBulkEvents = eventBulk.events;
  var attachmentsAdd = [];
  var attachmentsRemove = [];
  var attachmentsUpdate = [];
  var hasDocsInDb = docsInDb.size > 0;
  var newestRow;

  /**
   * @performance is really important in this loop!
   */
  var rowAmount = bulkWriteRows.length;
  for (var rowId = 0; rowId < rowAmount; rowId++) {
    var writeRow = bulkWriteRows[rowId];

    // use these variables to have less property accesses
    var document = writeRow.document;
    var previous = writeRow.previous;
    var docId = document[primaryPath];
    var documentDeleted = document._deleted;
    var previousDeleted = previous && previous._deleted;
    var documentInDb = undefined;
    if (hasDocsInDb) {
      documentInDb = docsInDb.get(docId);
    }
    var attachmentError = void 0;
    if (!documentInDb) {
      /**
       * It is possible to insert already deleted documents,
       * this can happen on replication.
       */
      if (hasAttachments) {
        var atts = document._attachments;
        var attKeys = Object.keys(atts);
        for (var a = 0; a < attKeys.length; a++) {
          var attachmentId = attKeys[a];
          var attachmentData = atts[attachmentId];
          if (!attachmentData.data) {
            attachmentError = {
              documentId: docId,
              isError: true,
              status: 510,
              writeRow,
              attachmentId,
              context
            };
            errors.push(attachmentError);
          } else {
            attachmentsAdd.push({
              documentId: docId,
              attachmentId,
              attachmentData: attachmentData,
              digest: attachmentData.digest
            });
          }
        }
      }
      var insertedRow = void 0;
      if (!attachmentError) {
        var row = hasAttachments ? stripAttachmentsDataFromRow(writeRow) : writeRow;
        insertedRow = row;
        bulkInsertDocs.push(row);
        if (onInsert) {
          onInsert(document);
        }
        newestRow = row;
      }
      if (!documentDeleted) {
        var eventDocData = document;
        if (hasAttachments) {
          eventDocData = insertedRow ? insertedRow.document : stripAttachmentsDataFromDocument(document);
        }
        var event = {
          documentId: docId,
          operation: 'INSERT',
          documentData: eventDocData,
          previousDocumentData: hasAttachments && previous ? stripAttachmentsDataFromDocument(previous) : previous
        };
        eventBulkEvents.push(event);
      }
    } else {
      // update existing document
      var revInDb = documentInDb._rev;

      /**
       * Check for conflict
       */
      if (!previous || !!previous && revInDb !== previous._rev) {
        // is conflict error
        var err = {
          isError: true,
          status: 409,
          documentId: docId,
          writeRow: writeRow,
          documentInDb,
          context
        };
        errors.push(err);
        continue;
      }

      // handle attachments data

      var updatedRow = hasAttachments ? stripAttachmentsDataFromRow(writeRow) : writeRow;
      if (hasAttachments) {
        if (documentDeleted) {
          /**
           * Deleted documents must have cleared all their attachments.
           */
          if (previous) {
            var prevAtts = previous._attachments;
            var prevAttKeys = Object.keys(prevAtts);
            for (var _a = 0; _a < prevAttKeys.length; _a++) {
              var _attachmentId = prevAttKeys[_a];
              attachmentsRemove.push({
                documentId: docId,
                attachmentId: _attachmentId,
                digest: prevAtts[_attachmentId].digest
              });
            }
          }
        } else {
          // first check for errors
          var docAtts = document._attachments;
          var docAttKeys = Object.keys(docAtts);
          for (var _a2 = 0; _a2 < docAttKeys.length; _a2++) {
            var _attachmentId2 = docAttKeys[_a2];
            var _attachmentData = docAtts[_attachmentId2];
            var previousAttachmentData = previous ? previous._attachments[_attachmentId2] : undefined;
            if (!previousAttachmentData && !_attachmentData.data) {
              attachmentError = {
                documentId: docId,
                documentInDb: documentInDb,
                isError: true,
                status: 510,
                writeRow,
                attachmentId: _attachmentId2,
                context
              };
              break;
            }
          }
          if (!attachmentError) {
            for (var _a3 = 0; _a3 < docAttKeys.length; _a3++) {
              var _attachmentId3 = docAttKeys[_a3];
              var _attachmentData2 = docAtts[_attachmentId3];
              var _previousAttachmentData = previous ? previous._attachments[_attachmentId3] : undefined;
              if (!_previousAttachmentData) {
                attachmentsAdd.push({
                  documentId: docId,
                  attachmentId: _attachmentId3,
                  attachmentData: _attachmentData2,
                  digest: _attachmentData2.digest
                });
              } else {
                var newDigest = updatedRow.document._attachments[_attachmentId3].digest;
                if (_attachmentData2.data &&
                /**
                 * Performance shortcut,
                 * do not update the attachment data if it did not change.
                 */
                _previousAttachmentData.digest !== newDigest) {
                  attachmentsUpdate.push({
                    documentId: docId,
                    attachmentId: _attachmentId3,
                    attachmentData: _attachmentData2,
                    digest: _attachmentData2.digest
                  });
                }
              }
            }

            /**
             * Detect attachments that have been removed
             * (present in previous but missing from the new document).
             */
            if (previous) {
              var _prevAtts = previous._attachments;
              var _prevAttKeys = Object.keys(_prevAtts);
              for (var _a4 = 0; _a4 < _prevAttKeys.length; _a4++) {
                var _attachmentId4 = _prevAttKeys[_a4];
                if (!docAtts[_attachmentId4]) {
                  attachmentsRemove.push({
                    documentId: docId,
                    attachmentId: _attachmentId4,
                    digest: _prevAtts[_attachmentId4].digest
                  });
                }
              }
            }
          }
        }
      }
      if (attachmentError) {
        errors.push(attachmentError);
      } else {
        /**
         * updatedRow already has attachments stripped (line above),
         * so push it directly without stripping again.
         */
        bulkUpdateDocs.push(updatedRow);
        if (onUpdate) {
          onUpdate(document);
        }
        newestRow = updatedRow;
      }
      var eventDocumentData = void 0;
      var previousEventDocumentData = null;
      var operation = void 0;
      if (previousDeleted && !documentDeleted) {
        operation = 'INSERT';
        /**
         * Reuse the already-stripped document from updatedRow
         * instead of calling stripAttachmentsDataFromDocument() again.
         */
        eventDocumentData = hasAttachments ? updatedRow.document : document;
      } else if (previous && !previousDeleted && !documentDeleted) {
        operation = 'UPDATE';
        eventDocumentData = hasAttachments ? updatedRow.document : document;
        previousEventDocumentData = previous;
      } else if (documentDeleted) {
        operation = 'DELETE';
        eventDocumentData = (0, _index4.ensureNotFalsy)(document);
        previousEventDocumentData = previous;
      } else {
        throw (0, _rxError.newRxError)('SNH', {
          args: {
            writeRow
          }
        });
      }
      var _event = {
        documentId: docId,
        documentData: eventDocumentData,
        previousDocumentData: previousEventDocumentData,
        operation: operation
      };
      eventBulkEvents.push(_event);
    }
  }
  return {
    bulkInsertDocs,
    bulkUpdateDocs,
    newestRow,
    errors,
    eventBulk,
    attachmentsAdd,
    attachmentsRemove,
    attachmentsUpdate
  };
}
function stripAttachmentsDataFromRow(writeRow) {
  return {
    previous: writeRow.previous,
    document: stripAttachmentsDataFromDocument(writeRow.document)
  };
}

/**
 * Used in custom RxStorage implementations.
 */
function attachmentWriteDataToNormalData(writeData) {
  var data = writeData.data;
  if (!data) {
    return writeData;
  }
  var ret = {
    length: data.size,
    digest: writeData.digest,
    type: writeData.type
  };
  return ret;
}
function stripAttachmentsDataFromDocument(doc) {
  var atts = doc._attachments;
  if (!atts) {
    return doc;
  }

  // Use for..in loop to check for any keys without creating an array via Object.keys()
  var hasAnyAttachment = false;
  for (var key in atts) {
    if (Object.prototype.hasOwnProperty.call(atts, key)) {
      hasAnyAttachment = true;
      break;
    }
  }
  if (!hasAnyAttachment) {
    return doc;
  }
  var useDoc = (0, _index4.flatClone)(doc);
  var destAtts = {};
  var attKeys = Object.keys(atts);
  for (var i = 0; i < attKeys.length; i++) {
    var attachmentId = attKeys[i];
    destAtts[attachmentId] = attachmentWriteDataToNormalData(atts[attachmentId]);
  }
  useDoc._attachments = destAtts;
  return useDoc;
}

/**
 * Flat clone the document data
 * and also the _meta field.
 * Used many times when we want to change the meta
 * during replication etc.
 */
function flatCloneDocWithMeta(doc) {
  return {
    ...doc,
    _meta: {
      ...doc._meta
    }
  };
}
/**
 * Wraps the normal storageInstance of a RxCollection
 * to ensure that all access is properly using the hooks
 * and other data transformations and also ensure that database.lockedRun()
 * is used properly.
 */
function getWrappedStorageInstance(database, storageInstance,
/**
 * The original RxJsonSchema
 * before it was mutated by hooks.
 */
rxJsonSchema) {
  _overwritable.overwritable.deepFreezeWhenDevMode(rxJsonSchema);
  var primaryPath = (0, _rxSchemaHelper.getPrimaryFieldOfPrimaryKey)(storageInstance.schema.primaryKey);
  var ret = {
    originalStorageInstance: storageInstance,
    schema: storageInstance.schema,
    internals: storageInstance.internals,
    collectionName: storageInstance.collectionName,
    databaseName: storageInstance.databaseName,
    options: storageInstance.options,
    async bulkWrite(rows, context) {
      var databaseToken = database.token;
      /**
       * Use the same timestamp for all docs of this rows-set.
       * This improves performance because calling Date.now() inside of the now() function
       * is too costly.
       */
      var time = (0, _index4.now)();
      /**
       * Pre-compute the first revision string for inserts (no previous document).
       * This avoids repeated string concatenation and getHeightOfRevision() calls
       * inside the hot loop.
       */
      var firstRevision = '1-' + databaseToken;
      /**
       * Share a single _meta object for all insert rows in this batch.
       * All inserts in the same bulkWrite share the same timestamp,
       * so we avoid creating a new { lwt: time } object per row.
       * This shared reference is safe because:
       * - All documents in one batch receive identical metadata values.
       * - When a document is later updated, flatCloneDocWithMeta() creates
       *   a new _meta object, so the shared reference is never mutated.
       */
      var insertMeta = {
        lwt: time
      };

      /**
       * When the caller has already cloned the documents (registered
       * via MUTABLE_DOCUMENT_WRITE_CONTEXTS), we can mutate them
       * in place and reuse the input array, avoiding redundant
       * flatClone() and wrapper-object allocations on every insert row.
       */
      var isMutableContext = MUTABLE_DOCUMENT_WRITE_CONTEXTS.has(context);
      var toStorageWriteRows;
      if (isMutableContext) {
        /**
         * Fast path: documents are already cloned by the caller.
         * Set _meta/_rev directly on the document and reuse the
         * input rows array without allocating wrapper objects.
         */
        for (var index = 0; index < rows.length; index++) {
          var document = rows[index].document;
          document._meta = insertMeta;
          document._rev = firstRevision;
        }
        toStorageWriteRows = rows;
      } else {
        toStorageWriteRows = new Array(rows.length);
        for (var _index = 0; _index < rows.length; _index++) {
          var writeRow = rows[_index];
          var previous = writeRow.previous;
          var _document = void 0;
          if (previous) {
            _document = flatCloneDocWithMeta(writeRow.document);
            _document._meta.lwt = time;
            _document._rev = (0, _index4.createRevision)(databaseToken, previous);
          } else {
            /**
             * Insert path: flatClone is required because the input document
             * may be a direct reference to another storage's internal data
             * (e.g., during migration, query results from the old storage are
             * passed directly as insert rows to the new storage).
             *
             * Use a shared insertMeta object instead of allocating { lwt: time }
             * per row, since all inserts in the same batch share the same timestamp.
             */
            _document = (0, _index4.flatClone)(writeRow.document);
            _document._meta = insertMeta;
            _document._rev = firstRevision;
          }
          toStorageWriteRows[_index] = {
            document: _document,
            previous
          };
        }
      }
      if (_hooks.HOOKS.preStorageWrite.length > 0) {
        (0, _hooks.runPluginHooks)('preStorageWrite', {
          storageInstance: this.originalStorageInstance,
          rows: toStorageWriteRows
        });
      }
      var writeResult = await database.lockedRun(() => storageInstance.bulkWrite(toStorageWriteRows, context));

      /**
       * The RxStorageInstance MUST NOT allow to insert already _deleted documents,
       * without sending the previous document version.
       * But for better developer experience, RxDB does allow to re-insert deleted documents.
       * We do this by automatically fixing the conflict errors for that case
       * by running another bulkWrite() and merging the results.
       * @link https://github.com/pubkey/rxdb/pull/3839
      */

      /**
       * Fast path: when there are no errors, skip the wrapper object creation
       * and error filtering to reduce allocations.
       */
      if (writeResult.error.length === 0) {
        BULK_WRITE_ROWS_BY_RESPONSE.set(writeResult, toStorageWriteRows);
        return writeResult;
      }
      var useWriteResult = {
        error: []
      };
      BULK_WRITE_ROWS_BY_RESPONSE.set(useWriteResult, toStorageWriteRows);

      // No need to check writeResult.error.length === 0 here because
      // the fast path above already returns early when there are no errors.
      var reInsertErrors = writeResult.error.filter(error => {
        if (error.status === 409 && !error.writeRow.previous && !error.writeRow.document._deleted && (0, _index4.ensureNotFalsy)(error.documentInDb)._deleted) {
          return true;
        }

        // add the "normal" errors to the parent error array.
        useWriteResult.error.push(error);
        return false;
      });
      if (reInsertErrors.length > 0) {
        var reInsertIds = new Set();
        var reInserts = reInsertErrors.map(error => {
          reInsertIds.add(error.documentId);
          return {
            previous: error.documentInDb,
            document: Object.assign({}, error.writeRow.document, {
              _rev: (0, _index4.createRevision)(database.token, error.documentInDb)
            })
          };
        });
        var subResult = await database.lockedRun(() => storageInstance.bulkWrite(reInserts, context));
        useWriteResult.error = useWriteResult.error.concat(subResult.error);
        var successArray = getWrittenDocumentsFromBulkWriteResponse(primaryPath, toStorageWriteRows, useWriteResult, reInsertIds);
        var subSuccess = getWrittenDocumentsFromBulkWriteResponse(primaryPath, reInserts, subResult);
        successArray.push(...subSuccess);
        return useWriteResult;
      }
      return useWriteResult;
    },
    query(preparedQuery) {
      return database.lockedRun(() => storageInstance.query(preparedQuery));
    },
    count(preparedQuery) {
      return database.lockedRun(() => storageInstance.count(preparedQuery));
    },
    findDocumentsById(ids, deleted) {
      return database.lockedRun(() => storageInstance.findDocumentsById(ids, deleted));
    },
    getAttachmentData(documentId, attachmentId, digest) {
      return database.lockedRun(() => storageInstance.getAttachmentData(documentId, attachmentId, digest));
    },
    getChangedDocumentsSince: !storageInstance.getChangedDocumentsSince ? undefined : (limit, checkpoint) => {
      return database.lockedRun(() => storageInstance.getChangedDocumentsSince((0, _index4.ensureNotFalsy)(limit), checkpoint));
    },
    cleanup(minDeletedTime) {
      return database.lockedRun(() => storageInstance.cleanup(minDeletedTime));
    },
    remove() {
      database.storageInstances.delete(ret);
      return database.lockedRun(() => storageInstance.remove());
    },
    close() {
      database.storageInstances.delete(ret);
      return database.lockedRun(() => storageInstance.close());
    },
    changeStream() {
      return storageInstance.changeStream();
    }
  };
  database.storageInstances.add(ret);
  return ret;
}

/**
 * Each RxStorage implementation should
 * run this method at the first step of createStorageInstance()
 * to ensure that the configuration is correct.
 */
function ensureRxStorageInstanceParamsAreCorrect(params) {
  if (params.schema.keyCompression) {
    throw (0, _rxError.newRxError)('UT5', {
      args: {
        params
      }
    });
  }
  if (hasEncryption(params.schema)) {
    throw (0, _rxError.newRxError)('UT6', {
      args: {
        params
      }
    });
  }
  if (params.schema.attachments && params.schema.attachments.compression) {
    throw (0, _rxError.newRxError)('UT7', {
      args: {
        params
      }
    });
  }
}
function hasEncryption(jsonSchema) {
  if (!!jsonSchema.encrypted && jsonSchema.encrypted.length > 0 || jsonSchema.attachments && jsonSchema.attachments.encrypted) {
    return true;
  } else {
    return false;
  }
}
function getChangedDocumentsSinceQuery(storageInstance, limit, checkpoint) {
  var primaryPath = (0, _rxSchemaHelper.getPrimaryFieldOfPrimaryKey)(storageInstance.schema.primaryKey);
  var sinceLwt = checkpoint ? checkpoint.lwt : _index4.RX_META_LWT_MINIMUM;
  var sinceId = checkpoint ? checkpoint.id : '';
  return (0, _rxQueryHelper.normalizeMangoQuery)(storageInstance.schema, {
    selector: {
      $or: [{
        '_meta.lwt': {
          $gt: sinceLwt
        }
      }, {
        '_meta.lwt': {
          $eq: sinceLwt
        },
        [primaryPath]: {
          $gt: checkpoint ? sinceId : ''
        }
      }],
      // add this hint for better index usage
      '_meta.lwt': {
        $gte: sinceLwt
      }
    },
    sort: [{
      '_meta.lwt': 'asc'
    }, {
      [primaryPath]: 'asc'
    }],
    skip: 0,
    limit
    /**
     * DO NOT SET A SPECIFIC INDEX HERE!
     * The query might be modified by some plugin
     * before sending it to the storage.
     * We can be sure that in the end the query planner
     * will find the best index.
     */
    // index: ['_meta.lwt', primaryPath]
  });
}
async function getChangedDocumentsSince(storageInstance, limit, checkpoint) {
  if (storageInstance.getChangedDocumentsSince) {
    return storageInstance.getChangedDocumentsSince(limit, checkpoint);
  }
  var primaryPath = (0, _rxSchemaHelper.getPrimaryFieldOfPrimaryKey)(storageInstance.schema.primaryKey);
  var query = (0, _rxQueryHelper.prepareQuery)(storageInstance.schema, getChangedDocumentsSinceQuery(storageInstance, limit, checkpoint));
  var result = await storageInstance.query(query);
  var documents = result.documents;
  var lastDoc = (0, _index4.lastOfArray)(documents);
  return {
    documents: documents,
    checkpoint: lastDoc ? {
      id: lastDoc[primaryPath],
      lwt: lastDoc._meta.lwt
    } : checkpoint ? checkpoint : {
      id: '',
      lwt: 0
    }
  };
}
var BULK_WRITE_ROWS_BY_RESPONSE = new WeakMap();
var BULK_WRITE_SUCCESS_MAP = new WeakMap();

/**
 * For better performance, this is done only when accessed
 * because most of the time we do not need the results, only the errors.
 */
function getWrittenDocumentsFromBulkWriteResponse(primaryPath, writeRows, response, reInsertIds) {
  return (0, _index4.getFromMapOrCreate)(BULK_WRITE_SUCCESS_MAP, response, () => {
    var ret = [];
    var realWriteRows = BULK_WRITE_ROWS_BY_RESPONSE.get(response);
    if (!realWriteRows) {
      realWriteRows = writeRows;
    }
    if (response.error.length > 0 || reInsertIds) {
      var errorIds = reInsertIds ? reInsertIds : new Set();
      for (var index = 0; index < response.error.length; index++) {
        var error = response.error[index];
        errorIds.add(error.documentId);
      }
      for (var _index2 = 0; _index2 < realWriteRows.length; _index2++) {
        var doc = realWriteRows[_index2].document;
        if (!errorIds.has(doc[primaryPath])) {
          ret.push(stripAttachmentsDataFromDocument(doc));
        }
      }
    } else {
      // pre-set array size for better performance
      ret.length = writeRows.length - response.error.length;
      for (var _index3 = 0; _index3 < realWriteRows.length; _index3++) {
        var _doc = realWriteRows[_index3].document;
        ret[_index3] = stripAttachmentsDataFromDocument(_doc);
      }
    }
    return ret;
  });
}

/**
 * Wraps the storage and simluates
 * delays. Mostly used in tests.
 */
function randomDelayStorage(input) {
  /**
   * Ensure writes to a delay storage
   * are still correctly run in order.
   */
  var randomDelayStorageWriteQueue = _index4.PROMISE_RESOLVE_TRUE;
  var retStorage = {
    name: 'random-delay-' + input.storage.name,
    rxdbVersion: _index4.RXDB_VERSION,
    async createStorageInstance(params) {
      await (0, _index4.promiseWait)(input.delayTimeBefore());
      var storageInstance = await input.storage.createStorageInstance(params);
      await (0, _index4.promiseWait)(input.delayTimeAfter());
      return {
        databaseName: storageInstance.databaseName,
        internals: storageInstance.internals,
        options: storageInstance.options,
        schema: storageInstance.schema,
        collectionName: storageInstance.collectionName,
        bulkWrite(a, b) {
          randomDelayStorageWriteQueue = randomDelayStorageWriteQueue.then(async () => {
            await (0, _index4.promiseWait)(input.delayTimeBefore());
            var response = await storageInstance.bulkWrite(a, b);
            await (0, _index4.promiseWait)(input.delayTimeAfter());
            return response;
          });
          var ret = randomDelayStorageWriteQueue;
          return ret;
        },
        async findDocumentsById(a, b) {
          await (0, _index4.promiseWait)(input.delayTimeBefore());
          var ret = await storageInstance.findDocumentsById(a, b);
          await (0, _index4.promiseWait)(input.delayTimeAfter());
          return ret;
        },
        async query(a) {
          await (0, _index4.promiseWait)(input.delayTimeBefore());
          var ret = await storageInstance.query(a);
          return ret;
        },
        async count(a) {
          await (0, _index4.promiseWait)(input.delayTimeBefore());
          var ret = await storageInstance.count(a);
          await (0, _index4.promiseWait)(input.delayTimeAfter());
          return ret;
        },
        async getAttachmentData(a, b, c) {
          await (0, _index4.promiseWait)(input.delayTimeBefore());
          var ret = await storageInstance.getAttachmentData(a, b, c);
          await (0, _index4.promiseWait)(input.delayTimeAfter());
          return ret;
        },
        getChangedDocumentsSince: !storageInstance.getChangedDocumentsSince ? undefined : async (a, b) => {
          await (0, _index4.promiseWait)(input.delayTimeBefore());
          var ret = await (0, _index4.ensureNotFalsy)(storageInstance.getChangedDocumentsSince)(a, b);
          await (0, _index4.promiseWait)(input.delayTimeAfter());
          return ret;
        },
        changeStream() {
          return storageInstance.changeStream();
        },
        async cleanup(a) {
          await (0, _index4.promiseWait)(input.delayTimeBefore());
          var ret = await storageInstance.cleanup(a);
          await (0, _index4.promiseWait)(input.delayTimeAfter());
          return ret;
        },
        async close() {
          await (0, _index4.promiseWait)(input.delayTimeBefore());
          var ret = await storageInstance.close();
          await (0, _index4.promiseWait)(input.delayTimeAfter());
          return ret;
        },
        async remove() {
          await (0, _index4.promiseWait)(input.delayTimeBefore());
          var ret = await storageInstance.remove();
          await (0, _index4.promiseWait)(input.delayTimeAfter());
          return ret;
        }
      };
    }
  };
  return retStorage;
}
//# sourceMappingURL=rx-storage-helper.js.map