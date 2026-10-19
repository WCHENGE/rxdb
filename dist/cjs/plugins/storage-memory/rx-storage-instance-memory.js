"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RxStorageInstanceMemory = exports.OPEN_MEMORY_INSTANCES = void 0;
exports.createMemoryStorageInstance = createMemoryStorageInstance;
var _readOnlyError2 = _interopRequireDefault(require("@babel/runtime/helpers/readOnlyError"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _rxjs = require("rxjs");
var _customIndex = require("../../custom-index.js");
var _rxSchemaHelper = require("../../rx-schema-helper.js");
var _rxStorageHelper = require("../../rx-storage-helper.js");
var _index2 = require("../../plugins/utils/index.js");
var _binarySearchBounds = require("./binary-search-bounds.js");
var _memoryHelper = require("./memory-helper.js");
var _memoryIndexes = require("./memory-indexes.js");
var _rxQueryHelper = require("../../rx-query-helper.js");
var _rxError = require("../../rx-error.js");
/**
 * Used in tests to ensure everything
 * is closed correctly
 */
var OPEN_MEMORY_INSTANCES = exports.OPEN_MEMORY_INSTANCES = new Set();
var RxStorageInstanceMemory = exports.RxStorageInstanceMemory = /*#__PURE__*/function () {
  function RxStorageInstanceMemory(storage, databaseName, collectionName, schema, internals, options, settings, devMode) {
    (0, _defineProperty2.default)(this, "primaryPath", void 0);
    (0, _defineProperty2.default)(this, "closed", false);
    /**
     * Used by some plugins and storage wrappers
     * to find out details about the internals of a write operation.
     * For example if you want to know which documents really have been replaced
     * or newly inserted.
     */
    (0, _defineProperty2.default)(this, "categorizedByWriteInput", new WeakMap());
    this.storage = storage;
    this.databaseName = databaseName;
    this.collectionName = collectionName;
    this.schema = schema;
    this.internals = internals;
    this.options = options;
    this.settings = settings;
    this.devMode = devMode;
    OPEN_MEMORY_INSTANCES.add(this);
    this.primaryPath = (0, _rxSchemaHelper.getPrimaryFieldOfPrimaryKey)(this.schema.primaryKey);
  }
  var _proto = RxStorageInstanceMemory.prototype;
  _proto.bulkWrite = function bulkWrite(documentWrites, context) {
    this.ensurePersistence();
    (0, _memoryHelper.ensureNotRemoved)(this);
    var internals = this.internals;
    var documentsById = this.internals.documents;
    var primaryPath = this.primaryPath;
    var categorized = (0, _rxStorageHelper.categorizeBulkWriteRows)(this, primaryPath, documentsById, documentWrites, context);
    var error = categorized.errors;
    /**
     * @performance
     * We have to return a Promise but we do not want to wait
     * one tick, so we directly create the promise
     * which makes it likely to be already resolved later.
     */
    var awaitMe = Promise.resolve({
      error
    });
    this.categorizedByWriteInput.set(documentWrites, categorized);
    this.internals.ensurePersistenceTask = categorized;
    if (!this.internals.ensurePersistenceIdlePromise) {
      this.internals.ensurePersistenceIdlePromise = (0, _index2.requestIdlePromiseNoQueue)().then(() => {
        this.internals.ensurePersistenceIdlePromise = undefined;
        this.ensurePersistence();
      });
    }

    /**
     * Important: The events must be emitted AFTER the persistence
     * task has been added.
     */
    if (categorized.eventBulk.events.length > 0) {
      var lastState = (0, _index2.ensureNotFalsy)(categorized.newestRow).document;
      categorized.eventBulk.checkpoint = {
        id: lastState[primaryPath],
        lwt: lastState._meta.lwt
      };
      internals.changes$.next(categorized.eventBulk);
    }
    return awaitMe;
  }

  /**
   * Instead of directly inserting the documents into all indexes,
   * we do it lazy in the background. This gives the application time
   * to directly work with the write-result and to do stuff like rendering DOM
   * notes and processing RxDB queries.
   * Then in some later time, or just before the next read/write,
   * it is ensured that the indexes have been written.
   */;
  _proto.ensurePersistence = function ensurePersistence() {
    if (!this.internals.ensurePersistenceTask) {
      return;
    }
    var internals = this.internals;
    var primaryPath = this.primaryPath;
    var categorized = this.internals.ensurePersistenceTask;
    this.internals.ensurePersistenceTask = undefined;

    /**
     * Do inserts/updates
     * @performance Use cached byIndexArray instead of Object.values()
     */
    var stateByIndex = internals.byIndexArray;

    /**
     * @performance Use batch insert for bulk inserts to avoid
     * repeated Array.splice() calls which are O(n) each.
     * Instead, batch-compute index entries, sort them,
     * and merge into existing sorted arrays.
     */
    var bulkInsertDocs = categorized.bulkInsertDocs;
    if (bulkInsertDocs.length > 0) {
      (0, _memoryHelper.bulkInsertToState)(primaryPath, internals, stateByIndex, bulkInsertDocs);
    }
    var bulkUpdateDocs = categorized.bulkUpdateDocs;
    for (var i = 0; i < bulkUpdateDocs.length; ++i) {
      var writeRow = bulkUpdateDocs[i];
      var doc = writeRow.document;
      var docId = doc[primaryPath];
      /**
       * @performance
       * Pass writeRow.previous directly as the old document state
       * instead of re-looking it up from the documents Map.
       * This is safe because categorizeBulkWriteRows already verified
       * that previous._rev matches the document in the Map (conflict check).
       */
      (0, _memoryHelper.putWriteRowToState)(docId, internals, stateByIndex, doc, writeRow.previous);
    }

    /**
     * Handle attachments
     */
    if (this.schema.attachments) {
      var attachmentsMap = internals.attachments;
      categorized.attachmentsAdd.forEach(attachment => {
        attachmentsMap.set((0, _memoryHelper.attachmentMapKey)(attachment.documentId, attachment.attachmentId), {
          writeData: attachment.attachmentData,
          digest: attachment.digest
        });
      });
      if (this.schema.attachments) {
        categorized.attachmentsUpdate.forEach(attachment => {
          attachmentsMap.set((0, _memoryHelper.attachmentMapKey)(attachment.documentId, attachment.attachmentId), {
            writeData: attachment.attachmentData,
            digest: attachment.digest
          });
        });
        categorized.attachmentsRemove.forEach(attachment => {
          attachmentsMap.delete((0, _memoryHelper.attachmentMapKey)(attachment.documentId, attachment.attachmentId));
        });
      }
    }
  };
  _proto.findDocumentsById = function findDocumentsById(docIds, withDeleted) {
    this.ensurePersistence();
    var documentsById = this.internals.documents;
    var ret = [];
    if (documentsById.size === 0) {
      return Promise.resolve(ret);
    }
    /**
     * @performance
     * Split into two paths to avoid checking withDeleted on every iteration.
     */
    if (withDeleted) {
      for (var i = 0; i < docIds.length; ++i) {
        var docInDb = documentsById.get(docIds[i]);
        if (docInDb) {
          ret.push(docInDb);
        }
      }
    } else {
      for (var _i = 0; _i < docIds.length; ++_i) {
        var _docInDb = documentsById.get(docIds[_i]);
        if (_docInDb && !_docInDb._deleted) {
          ret.push(_docInDb);
        }
      }
    }
    return Promise.resolve(ret);
  };
  _proto.query = function query(preparedQuery) {
    this.ensurePersistence();
    var queryPlan = preparedQuery.queryPlan;
    var query = preparedQuery.query;
    var skip = query.skip ? query.skip : 0;
    /**
     * Use typeof so an explicit `limit: 0` from the mango query is
     * honored. The previous truthy check treated `0` as "no limit"
     * and returned all matching documents.
     */
    var limit = typeof query.limit === 'number' ? query.limit : Infinity;
    var skipPlusLimit = skip + limit;
    var queryMatcher = false;
    if (!queryPlan.selectorSatisfiedByIndex) {
      queryMatcher = (0, _rxQueryHelper.getQueryMatcher)(this.schema, preparedQuery.query);
    }
    var queryPlanFields = queryPlan.index;
    var mustManuallyResort = !queryPlan.sortSatisfiedByIndex;
    var index = queryPlanFields;
    var lowerBound = queryPlan.startKeys;
    var lowerBoundString = (0, _customIndex.getStartIndexStringFromLowerBound)(this.schema, index, lowerBound);
    var upperBound = queryPlan.endKeys;
    var upperBoundString = (0, _customIndex.getStartIndexStringFromUpperBound)(this.schema, index, upperBound);
    var indexName = (0, _memoryIndexes.getMemoryIndexName)(index);
    if (!this.internals.byIndex[indexName]) {
      throw new Error('index does not exist ' + indexName);
    }
    var docsWithIndex = this.internals.byIndex[indexName].docsWithIndex;

    /**
     * @performance Use string-specialized binary search to avoid
     * temporary array allocations on every query.
     */
    var indexOfLower = queryPlan.inclusiveStart ? (0, _binarySearchBounds.boundGEByIndexString)(docsWithIndex, lowerBoundString) : (0, _binarySearchBounds.boundGTByIndexString)(docsWithIndex, lowerBoundString);
    var indexOfUpper = queryPlan.inclusiveEnd ? (0, _binarySearchBounds.boundLEByIndexString)(docsWithIndex, upperBoundString) : (0, _binarySearchBounds.boundLTByIndexString)(docsWithIndex, upperBoundString);
    var rows = [];

    /**
     * @performance
     * If the selector is satisfied by the index,
     * we can extract all documents in the range without
     * running a per-document queryMatcher check.
     * This is a common case for queries like find-by-query
     * where the selector is empty or fully covered by the index.
     */
    if (!queryMatcher) {
      var rangeLength = indexOfUpper - indexOfLower + 1;
      if (rangeLength > 0) {
        var extractLength = mustManuallyResort ? rangeLength : Math.min(rangeLength, skipPlusLimit);
        rows = new Array(extractLength);
        for (var i = 0; i < extractLength; i++) {
          rows[i] = docsWithIndex[indexOfLower + i][1];
        }
      }
    } else {
      var done = false;
      while (!done) {
        var currentRow = docsWithIndex[indexOfLower];
        if (!currentRow || indexOfLower > indexOfUpper) {
          break;
        }
        var currentDoc = currentRow[1];
        if (queryMatcher(currentDoc)) {
          rows.push(currentDoc);
        }
        if (rows.length >= skipPlusLimit && !mustManuallyResort) {
          done = true;
        }
        indexOfLower++;
      }
    }
    if (mustManuallyResort) {
      var sortComparator = (0, _rxQueryHelper.getSortComparator)(this.schema, preparedQuery.query);
      rows = rows.sort(sortComparator);
    }

    // apply skip and limit boundaries.
    if (skip !== 0 || rows.length > skipPlusLimit) {
      rows = rows.slice(skip, skipPlusLimit);
    }
    return Promise.resolve({
      documents: rows
    });
  };
  _proto.count = function count(preparedQuery) {
    this.ensurePersistence();
    var queryPlan = preparedQuery.queryPlan;

    /**
     * @performance
     * If the selector is satisfied by the index,
     * we can compute the count directly from the index range
     * without extracting document data into an array.
     * Uses string-specialized binary search to avoid allocations.
     */
    if (queryPlan.selectorSatisfiedByIndex) {
      var _queryPlanFields = queryPlan.index;
      var _index = _queryPlanFields;
      var _lowerBound = queryPlan.startKeys;
      var _lowerBoundString = (0, _customIndex.getStartIndexStringFromLowerBound)(this.schema, _index, _lowerBound);
      var _upperBound = queryPlan.endKeys;
      var _upperBoundString = (0, _customIndex.getStartIndexStringFromUpperBound)(this.schema, _index, _upperBound);
      var _indexName = (0, _memoryIndexes.getMemoryIndexName)(_index);
      if (!this.internals.byIndex[_indexName]) {
        throw (0, _rxError.newRxError)('SNH', {
          args: {
            indexName: _indexName
          }
        });
      }
      var _docsWithIndex = this.internals.byIndex[_indexName].docsWithIndex;
      var _indexOfLower = queryPlan.inclusiveStart ? (0, _binarySearchBounds.boundGEByIndexString)(_docsWithIndex, _lowerBoundString) : (0, _binarySearchBounds.boundGTByIndexString)(_docsWithIndex, _lowerBoundString);
      var _indexOfUpper = queryPlan.inclusiveEnd ? (0, _binarySearchBounds.boundLEByIndexString)(_docsWithIndex, _upperBoundString) : (0, _binarySearchBounds.boundLTByIndexString)(_docsWithIndex, _upperBoundString);
      var _count = Math.max(0, _indexOfUpper - _indexOfLower + 1);
      return Promise.resolve({
        count: _count,
        mode: 'fast'
      });
    }
    var queryMatcher = (0, _rxQueryHelper.getQueryMatcher)(this.schema, preparedQuery.query);
    var queryPlanFields = queryPlan.index;
    var index = queryPlanFields;
    var lowerBound = queryPlan.startKeys;
    var lowerBoundString = (0, _customIndex.getStartIndexStringFromLowerBound)(this.schema, index, lowerBound);
    var upperBound = queryPlan.endKeys;
    var upperBoundString = (0, _customIndex.getStartIndexStringFromUpperBound)(this.schema, index, upperBound);
    var indexName = (0, _memoryIndexes.getMemoryIndexName)(index);
    if (!this.internals.byIndex[indexName]) {
      throw (0, _rxError.newRxError)('SNH', {
        args: {
          indexName
        }
      });
    }
    var docsWithIndex = this.internals.byIndex[indexName].docsWithIndex;
    var indexOfLower = queryPlan.inclusiveStart ? (0, _binarySearchBounds.boundGEByIndexString)(docsWithIndex, lowerBoundString) : (0, _binarySearchBounds.boundGTByIndexString)(docsWithIndex, lowerBoundString);
    var indexOfUpper = queryPlan.inclusiveEnd ? (0, _binarySearchBounds.boundLEByIndexString)(docsWithIndex, upperBoundString) : (0, _binarySearchBounds.boundLTByIndexString)(docsWithIndex, upperBoundString);
    var count = 0;
    while (indexOfLower <= indexOfUpper) {
      var currentRow = docsWithIndex[indexOfLower];
      if (!currentRow) {
        break;
      }
      if (queryMatcher(currentRow[1])) {
        count++;
      }
      indexOfLower++;
    }
    return Promise.resolve({
      count,
      mode: 'fast'
    });
  };
  _proto.cleanup = function cleanup(minimumDeletedTime) {
    this.ensurePersistence();
    var maxDeletionTime = (0, _index2.now)() - minimumDeletedTime;
    var index = ['_deleted', '_meta.lwt', this.primaryPath];
    var indexName = (0, _memoryIndexes.getMemoryIndexName)(index);
    var docsWithIndex = this.internals.byIndex[indexName].docsWithIndex;
    var lowerBoundString = (0, _customIndex.getStartIndexStringFromLowerBound)(this.schema, index, [true, 0, '']);
    var indexOfLower = (0, _binarySearchBounds.boundGTByIndexString)(docsWithIndex, lowerBoundString);
    var done = false;
    while (!done) {
      var currentDoc = docsWithIndex[indexOfLower];
      if (!currentDoc || currentDoc[1]._meta.lwt > maxDeletionTime) {
        done = true;
      } else {
        (0, _memoryHelper.removeDocFromState)(this.primaryPath, this.schema, this.internals, currentDoc[1]);
        /**
         * Do NOT increment indexOfLower after removal.
         * removeDocFromState() splices the element out of the array,
         * so the next element shifts into the current position.
         * Incrementing would skip it.
         */
      }
    }
    return _index2.PROMISE_RESOLVE_TRUE;
  };
  _proto.getAttachmentData = function getAttachmentData(documentId, attachmentId, digest) {
    this.ensurePersistence();
    (0, _memoryHelper.ensureNotRemoved)(this);
    var key = (0, _memoryHelper.attachmentMapKey)(documentId, attachmentId);
    var data = this.internals.attachments.get(key);
    if (!digest || !data || data.digest !== digest) {
      throw new Error('attachment does not exist: ' + key);
    }
    return Promise.resolve(data.writeData.data);
  };
  _proto.changeStream = function changeStream() {
    (0, _memoryHelper.ensureNotRemoved)(this);
    return this.internals.changes$.asObservable();
  };
  _proto.remove = async function remove() {
    if (this.closed) {
      throw new Error('closed');
    }
    this.ensurePersistence();
    (0, _memoryHelper.ensureNotRemoved)(this);
    this.internals.removed = true;
    this.storage.collectionStates.delete((0, _memoryHelper.getMemoryCollectionKey)(this.databaseName, this.collectionName, this.schema.version));
    await this.close();
  };
  _proto.close = function close() {
    OPEN_MEMORY_INSTANCES.delete(this);
    this.ensurePersistence();
    if (this.closed) {
      return _index2.PROMISE_RESOLVE_VOID;
    }
    this.closed = true;
    this.internals.refCount = this.internals.refCount - 1;
    return _index2.PROMISE_RESOLVE_VOID;
  };
  return RxStorageInstanceMemory;
}();
function createMemoryStorageInstance(storage, params, settings) {
  var collectionKey = (0, _memoryHelper.getMemoryCollectionKey)(params.databaseName, params.collectionName, params.schema.version);
  var internals = storage.collectionStates.get(collectionKey);
  if (!internals) {
    internals = {
      id: (0, _index2.randomToken)(5),
      schema: params.schema,
      removed: false,
      refCount: 1,
      documents: new Map(),
      attachments: params.schema.attachments ? new Map() : undefined,
      byIndex: {},
      byIndexArray: [],
      changes$: new _rxjs.Subject()
    };
    (0, _memoryIndexes.addIndexesToInternalsState)(internals, params.schema);
    storage.collectionStates.set(collectionKey, internals);
  } else {
    /**
     * Ensure that the storage was not already
     * created with a different schema.
     * This is very important because if this check
     * does not exist here, we have hard-to-debug problems
     * downstream.
     */
    if (params.devMode && !(0, _index2.deepEqual)(internals.schema, params.schema)) {
      throw new Error('storage was already created with a different schema');
    }
    internals.refCount = internals.refCount + 1;
  }
  var instance = new RxStorageInstanceMemory(storage, params.databaseName, params.collectionName, params.schema, internals, params.options, settings, params.devMode);
  return Promise.resolve(instance);
}
//# sourceMappingURL=rx-storage-instance-memory.js.map