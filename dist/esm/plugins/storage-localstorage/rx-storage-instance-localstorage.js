import { Subject } from 'rxjs';
import { PROMISE_RESOLVE_TRUE, PROMISE_RESOLVE_VOID, arrayBufferToBase64, ensureNotFalsy, now, toArray } from "../utils/index.js";
import { categorizeBulkWriteRows } from "../../rx-storage-helper.js";
import { getPrimaryFieldOfPrimaryKey } from "../../rx-schema-helper.js";
import { getQueryMatcher, getSortComparator } from "../../rx-query-helper.js";
import { newRxError } from "../../rx-error.js";
import { getIndexableStringMonad, getStartIndexStringFromLowerBound, getStartIndexStringFromUpperBound } from "../../custom-index.js";
import { pushAtSortPosition } from 'array-push-at-sort-position';
import { boundEQ, boundGE, boundGT, boundLE, boundLT } from "../storage-memory/binary-search-bounds.js";
export var RX_STORAGE_NAME_LOCALSTORAGE = 'localstorage';

// index-string to doc-id mapped

/**
 * StorageEvents are not sent to the same
 * browser tab where they were created.
 * This makes it hard to write unit tests
 * so we redistribute the events here instead.
 */
export var storageEventStream$ = new Subject();
var storageEventStreamObservable = storageEventStream$.asObservable();
var storageEventStreamSubscribed = false;
export function getStorageEventStream() {
  if (!storageEventStreamSubscribed && typeof window !== 'undefined') {
    storageEventStreamSubscribed = true;
    window.addEventListener('storage', ev => {
      if (!ev.key) {
        return;
      }
      storageEventStream$.next({
        fromStorageEvent: true,
        key: ev.key,
        newValue: ev.newValue
      });
    });
  }
  return storageEventStreamObservable;
}
var instanceId = 0;
export class RxStorageInstanceLocalstorage {
  primaryPath;

  /**
   * Under this key the whole state
   * will be stored as stringified json
   * inside of the localstorage.
   */
  docsKey;
  attachmentsKey;
  changestreamStorageKey;
  indexesKey;
  changeStreamSub;
  changes$ = new Subject();
  closed;
  localStorage;
  removed = false;
  instanceId = instanceId++;
  constructor(storage, databaseName, collectionName, schema, internals, options, settings, multiInstance, databaseInstanceToken) {
    this.storage = storage;
    this.databaseName = databaseName;
    this.collectionName = collectionName;
    this.schema = schema;
    this.internals = internals;
    this.options = options;
    this.settings = settings;
    this.multiInstance = multiInstance;
    this.databaseInstanceToken = databaseInstanceToken;
    this.localStorage = settings.localStorage ? settings.localStorage : typeof window !== 'undefined' ? window.localStorage : undefined;
    this.primaryPath = getPrimaryFieldOfPrimaryKey(this.schema.primaryKey);
    this.docsKey = 'RxDB-ls-doc-' + this.databaseName + '--' + this.collectionName + '--' + this.schema.version;
    this.changestreamStorageKey = 'RxDB-ls-changes-' + this.databaseName + '--' + this.collectionName + '--' + this.schema.version;
    this.indexesKey = 'RxDB-ls-idx-' + this.databaseName + '--' + this.collectionName + '--' + this.schema.version;
    this.attachmentsKey = 'RxDB-ls-attachment-' + this.databaseName + '--' + this.collectionName + '--' + this.schema.version;
    this.changeStreamSub = getStorageEventStream().subscribe(ev => {
      if (ev.key !== this.changestreamStorageKey || !ev.newValue || ev.fromStorageEvent && ev.databaseInstanceToken === this.databaseInstanceToken) {
        return;
      }
      var latestChanges = JSON.parse(ev.newValue);
      if (ev.fromStorageEvent && latestChanges.databaseInstanceToken === this.databaseInstanceToken) {
        return;
      }
      this.changes$.next(latestChanges.eventBulk);
    });
  }
  getDoc(docId) {
    var docString = this.localStorage.getItem(this.docsKey + '-' + docId);
    if (docString) {
      return JSON.parse(docString);
    }
  }
  setDoc(doc) {
    var docId = doc[this.primaryPath];
    this.localStorage.setItem(this.docsKey + '-' + docId, JSON.stringify(doc));
  }
  getIndex(index) {
    var indexString = this.localStorage.getItem(this.indexesKey + getIndexName(index));
    if (!indexString) {
      return [];
    } else {
      return JSON.parse(indexString);
    }
  }
  setIndex(index, value) {
    this.localStorage.setItem(this.indexesKey + getIndexName(index), JSON.stringify(value));
  }
  async bulkWrite(documentWrites, context) {
    var ret = {
      error: []
    };

    /**
     * Pre-convert all Blob attachment data to base64 BEFORE
     * calling categorizeBulkWriteRows. localStorage has no transactions
     * and is non-async, so all writes after the conflict check
     * must be synchronous to avoid interleaving.
     */
    var attachmentBase64Map = new Map();
    await Promise.all(documentWrites.map(async row => {
      if (row.document._attachments) {
        await Promise.all(Object.values(row.document._attachments).map(async att => {
          if (att.data instanceof Blob) {
            var ab = await att.data.arrayBuffer();
            attachmentBase64Map.set(att.data, arrayBufferToBase64(ab));
          }
        }));
      }
    }));
    var docsInDb = new Map();
    documentWrites.forEach(row => {
      var docId = row.document[this.primaryPath];
      var doc = this.getDoc(docId);
      if (doc) {
        docsInDb.set(docId, doc);
      }
    });
    var categorized = categorizeBulkWriteRows(this, this.primaryPath, docsInDb, documentWrites, context);
    ret.error = categorized.errors;
    var indexValues = Object.values(this.internals.indexes).map(idx => {
      return this.getIndex(idx.index);
    });
    [categorized.bulkInsertDocs, categorized.bulkUpdateDocs].forEach(rows => {
      rows.forEach(row => {
        // write new document data
        this.setDoc(row.document);

        // update the indexes
        var docId = row.document[this.primaryPath];
        Object.values(this.internals.indexes).forEach((idx, i) => {
          var indexValue = indexValues[i];
          var newIndexString = idx.getIndexableString(row.document);
          var insertPosition = pushAtSortPosition(indexValue, [newIndexString, docId], sortByIndexStringComparator, 0);
          if (row.previous) {
            var previousIndexString = idx.getIndexableString(row.previous);
            if (previousIndexString === newIndexString) {
              /**
               * Performance shortcut.
               * If index was not changed -> The old doc must be before or after the new one.
               */
              var prev = indexValue[insertPosition - 1];
              if (prev && prev[1] === docId) {
                indexValue.splice(insertPosition - 1, 1);
              } else {
                var next = indexValue[insertPosition + 1];
                if (next[1] === docId) {
                  indexValue.splice(insertPosition + 1, 1);
                } else {
                  throw newRxError('SNH', {
                    document: row.document,
                    args: {
                      insertPosition,
                      indexValue,
                      row,
                      idx
                    }
                  });
                }
              }
            } else {
              /**
               * Index changed, we must search for the old one and remove it.
               */
              var indexBefore = boundEQ(indexValue, [previousIndexString], compareDocsWithIndex);
              indexValue.splice(indexBefore, 1);
            }
          }
        });
      });
    });
    indexValues.forEach((indexValue, i) => {
      var index = Object.values(this.internals.indexes);
      this.setIndex(index[i].index, indexValue);
    });

    // attachments — use pre-converted base64 data (synchronous)
    categorized.attachmentsAdd.forEach(attachment => {
      var base64 = ensureNotFalsy(attachmentBase64Map.get(attachment.attachmentData.data));
      var mimeType = attachment.attachmentData.type || 'application/octet-stream';
      this.localStorage.setItem(this.attachmentsKey + '-' + attachment.documentId + '||' + attachment.attachmentId, 'data:' + mimeType + ';base64,' + base64);
    });
    categorized.attachmentsUpdate.forEach(attachment => {
      var base64 = ensureNotFalsy(attachmentBase64Map.get(attachment.attachmentData.data));
      var mimeType = attachment.attachmentData.type || 'application/octet-stream';
      this.localStorage.setItem(this.attachmentsKey + '-' + attachment.documentId + '||' + attachment.attachmentId, 'data:' + mimeType + ';base64,' + base64);
    });
    categorized.attachmentsRemove.forEach(attachment => {
      this.localStorage.removeItem(this.attachmentsKey + '-' + attachment.documentId + '||' + attachment.attachmentId);
    });
    if (categorized.eventBulk.events.length > 0) {
      var lastState = ensureNotFalsy(categorized.newestRow).document;
      categorized.eventBulk.checkpoint = {
        id: lastState[this.primaryPath],
        lwt: lastState._meta.lwt
      };
      var storageItemData = {
        databaseInstanceToken: this.databaseInstanceToken,
        eventBulk: categorized.eventBulk
      };
      var itemString = JSON.stringify(storageItemData);
      this.localStorage.setItem(this.changestreamStorageKey, itemString);
      storageEventStream$.next({
        fromStorageEvent: false,
        key: this.changestreamStorageKey,
        newValue: itemString,
        databaseInstanceToken: this.databaseInstanceToken
      });
    }
    return Promise.resolve(ret);
  }
  async findDocumentsById(docIds, withDeleted) {
    var ret = [];
    docIds.forEach(docId => {
      var doc = this.getDoc(docId);
      if (doc) {
        if (withDeleted || !doc._deleted) {
          ret.push(doc);
        }
      }
    });
    return ret;
  }
  query(preparedQuery) {
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
      queryMatcher = getQueryMatcher(this.schema, preparedQuery.query);
    }
    var queryPlanFields = queryPlan.index;
    var mustManuallyResort = !queryPlan.sortSatisfiedByIndex;
    var index = queryPlanFields;
    var lowerBound = queryPlan.startKeys;
    var lowerBoundString = getStartIndexStringFromLowerBound(this.schema, index, lowerBound);
    var upperBound = queryPlan.endKeys;
    upperBound = upperBound;
    var upperBoundString = getStartIndexStringFromUpperBound(this.schema, index, upperBound);
    var docsWithIndex = this.getIndex(index);
    var indexOfLower = (queryPlan.inclusiveStart ? boundGE : boundGT)(docsWithIndex, [lowerBoundString], compareDocsWithIndex);
    var indexOfUpper = (queryPlan.inclusiveEnd ? boundLE : boundLT)(docsWithIndex, [upperBoundString], compareDocsWithIndex);
    var rows = [];
    var done = false;
    while (!done) {
      var currentRow = docsWithIndex[indexOfLower];
      if (!currentRow || indexOfLower > indexOfUpper) {
        break;
      }
      var docId = currentRow[1];
      var currentDoc = ensureNotFalsy(this.getDoc(docId));
      if (!queryMatcher || queryMatcher(currentDoc)) {
        rows.push(currentDoc);
      }
      if (rows.length >= skipPlusLimit && !mustManuallyResort) {
        done = true;
      }
      indexOfLower++;
    }
    if (mustManuallyResort) {
      var sortComparator = getSortComparator(this.schema, preparedQuery.query);
      rows = rows.sort(sortComparator);
    }

    // apply skip and limit boundaries.
    rows = rows.slice(skip, skipPlusLimit);
    return Promise.resolve({
      documents: rows
    });
  }

  /**
   * Returns the same count as query().documents.length
   * to fulfill the RxStorageInstance interface contract.
   */
  async count(preparedQuery) {
    var result = await this.query(preparedQuery);
    return {
      count: result.documents.length,
      mode: 'fast'
    };
  }
  changeStream() {
    return this.changes$.asObservable();
  }
  cleanup(minimumDeletedTime) {
    var _this = this;
    var maxDeletionTime = now() - minimumDeletedTime;
    var indexValue = this.getIndex(CLEANUP_INDEX);
    var lowerBoundString = getStartIndexStringFromLowerBound(this.schema, CLEANUP_INDEX, [true, 0, '']);
    var indexOfLower = boundGT(indexValue, [lowerBoundString], compareDocsWithIndex);
    var indexValues = Object.values(this.internals.indexes).map(idx => {
      return this.getIndex(idx.index);
    });
    var done = false;
    var _loop = function () {
      var currentIndexRow = indexValue[indexOfLower];
      if (!currentIndexRow) {
        return 1; // break
      }
      var currentDocId = currentIndexRow[1];
      var currentDoc = ensureNotFalsy(_this.getDoc(currentDocId));
      if (currentDoc._meta.lwt > maxDeletionTime) {
        done = true;
      } else {
        _this.localStorage.removeItem(_this.docsKey + '-' + currentDocId);
        Object.values(_this.internals.indexes).forEach((idx, i) => {
          var indexValue = indexValues[i];
          var indexString = idx.getIndexableString(currentDoc);
          var indexBefore = boundEQ(indexValue, [indexString], compareDocsWithIndex);
          indexValue.splice(indexBefore, 1);
        });
        indexOfLower++;
      }
    };
    while (!done) {
      if (_loop()) break;
    }
    indexValues.forEach((indexValue, i) => {
      var index = Object.values(this.internals.indexes);
      this.setIndex(index[i].index, indexValue);
    });
    return PROMISE_RESOLVE_TRUE;
  }
  async getAttachmentData(documentId, attachmentId) {
    var stored = ensureNotFalsy(this.localStorage.getItem(this.attachmentsKey + '-' + documentId + '||' + attachmentId));
    // Stored value is a data URL like "data:<mime>;base64,<payload>".
    // Fall back to application/octet-stream for legacy bare base64 payloads.
    var dataUrl = stored;
    if (!stored.startsWith('data:')) {
      dataUrl = 'data:application/octet-stream;base64,' + stored;
    }
    var response = await fetch(dataUrl);
    return response.blob();
  }
  remove() {
    ensureNotRemoved(this);
    this.removed = true;

    // delete changes
    this.changeStreamSub.unsubscribe();
    this.localStorage.removeItem(this.changestreamStorageKey);

    // delete documents and their attachments
    var firstIndex = Object.values(this.internals.indexes)[0];
    var indexedDocs = this.getIndex(firstIndex.index);
    indexedDocs.forEach(row => {
      var docId = row[1];
      var doc = this.getDoc(docId);
      this.localStorage.removeItem(this.docsKey + '-' + docId);
      if (doc && doc._attachments) {
        Object.keys(doc._attachments).forEach(attachmentId => {
          this.localStorage.removeItem(this.attachmentsKey + '-' + docId + '||' + attachmentId);
        });
      }
    });

    // delete indexes
    Object.values(this.internals.indexes).forEach(idx => {
      this.localStorage.removeItem(this.indexesKey + idx.indexName);
    });
    return PROMISE_RESOLVE_VOID;
  }
  close() {
    this.changeStreamSub.unsubscribe();
    this.removed = true;
    if (this.closed) {
      return this.closed;
    }
    this.closed = (async () => {
      this.changes$.complete();
      this.localStorage.removeItem(this.changestreamStorageKey);
    })();
    return this.closed;
  }
}
export async function createLocalstorageStorageInstance(storage, params, settings) {
  var primaryPath = getPrimaryFieldOfPrimaryKey(params.schema.primaryKey);
  var useIndexes = params.schema.indexes ? params.schema.indexes.slice(0) : [];
  useIndexes.push([primaryPath]);
  var useIndexesFinal = useIndexes.map(index => {
    var indexAr = toArray(index);
    return indexAr;
  });
  useIndexesFinal.push(CLEANUP_INDEX);
  var indexes = {};
  useIndexesFinal.forEach((indexAr, indexId) => {
    var indexName = getIndexName(indexAr);
    indexes[indexName] = {
      indexId: '|' + indexId + '|',
      indexName,
      getIndexableString: getIndexableStringMonad(params.schema, indexAr),
      index: indexAr
    };
  });
  var internals = {
    indexes
  };
  var instance = new RxStorageInstanceLocalstorage(storage, params.databaseName, params.collectionName, params.schema, internals, params.options, settings, params.multiInstance, params.databaseInstanceToken);
  return instance;
}
export function getIndexName(index) {
  return index.join('|');
}
export var CLEANUP_INDEX = ['_deleted', '_meta.lwt'];
function sortByIndexStringComparator(a, b) {
  if (a[0] < b[0]) {
    return -1;
  } else {
    return 1;
  }
}
function compareDocsWithIndex(a, b) {
  var indexStringA = a[0];
  var indexStringB = b[0];
  if (indexStringA < indexStringB) {
    return -1;
  } else if (indexStringA === indexStringB) {
    return 0;
  } else {
    return 1;
  }
}
function ensureNotRemoved(instance) {
  if (instance.removed) {
    throw new Error('removed');
  }
}
//# sourceMappingURL=rx-storage-instance-localstorage.js.map