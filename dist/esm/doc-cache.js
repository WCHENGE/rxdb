import { getFromMapOrThrow, getHeightOfRevision, overwriteGetterForCaching, requestIdlePromiseNoQueue } from "./plugins/utils/index.js";
import { overwritable } from "./overwritable.js";

/**
 * Because we have to create many cache items,
 * we use an array instead of an object with properties
 * for better performance and less memory usage.
 * @link https://stackoverflow.com/questions/17295056/array-vs-object-efficiency-in-javascript
 */

/**
 * @link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/FinalizationRegistry
 */

/**
 * The DocumentCache stores RxDocument objects
 * by their primary key and revision.
 * This is useful on client side applications where
 * it is not known how much memory can be used, so
 * we de-duplicate RxDocument states to save memory.
 * To not fill up the memory with old document states, the DocumentCache
 * only contains weak references to the RxDocuments themself.
 * @link https://caniuse.com/?search=weakref
 */
export class DocumentCache {
  cacheItemByDocId = new Map();

  /**
   * Process stuff lazy to not block the CPU
   * on critical paths.
   */
  tasks = new Set();

  /**
   * Some JavaScript runtimes like QuickJS,
   * so not have a FinalizationRegistry or WeakRef.
   * Therefore we need a workaround which might waste a lot of memory,
   * but at least works.
   */
  registry = typeof FinalizationRegistry === 'function' ? new FinalizationRegistry(docMeta => {
    var docId = docMeta.docId;
    var cacheItem = this.cacheItemByDocId.get(docId);
    if (cacheItem) {
      var byRev = cacheItem[0];
      var weakRef = byRev.get(docMeta.rev);
      /**
       * Only remove entries whose RxDocument is garbage collected.
       * An entry might already have been replaced by another document state
       * which must not be removed.
       */
      if (weakRef && !weakRef.deref()) {
        byRev.delete(docMeta.rev);
      }
      var byRevAndLwt = cacheItem[2];
      if (byRevAndLwt) {
        var key = revAndLwtKey(docMeta.rev, docMeta.lwt);
        var otherWeakRef = byRevAndLwt.get(key);
        if (otherWeakRef && !otherWeakRef.deref()) {
          byRevAndLwt.delete(key);
        }
        if (byRevAndLwt.size === 0) {
          cacheItem[2] = undefined;
        }
      }
      if (byRev.size === 0 && !cacheItem[2]) {
        /**
         * No state of the document is cached anymore,
         * so we can clean up.
         */
        this.cacheItemByDocId.delete(docId);
      }
    }
  }) : undefined;
  constructor(primaryPath, changes$,
  /**
   * A method that can create a RxDocument by the given document data.
   */
  documentCreator) {
    this.primaryPath = primaryPath;
    this.changes$ = changes$;
    this.documentCreator = documentCreator;
    changes$.subscribe(events => {
      this.tasks.add(() => {
        var cacheItemByDocId = this.cacheItemByDocId;
        for (var index = 0; index < events.length; index++) {
          var event = events[index];
          var cacheItem = cacheItemByDocId.get(event.documentId);
          if (cacheItem) {
            var documentData = event.documentData;
            if (!documentData) {
              documentData = event.previousDocumentData;
            }
            /**
             * In multiInstance mode the changeStream merges events
             * from the own writes together with the events from other
             * instances that arrive over the BroadcastChannel.
             * These events can arrive out of order, so an older document
             * state might be received after a newer one was already processed.
             * We must not downgrade the cached latest document state in that case,
             * otherwise two instances can end up with diverging "latest" states
             * that never converge.
             * @link https://github.com/pubkey/rxdb/issues/8609
             */
            var currentLatest = cacheItem[1];
            if (!currentLatest || isNewerDocumentState(documentData, currentLatest)) {
              cacheItem[1] = documentData;
            }
          }
        }
      });
      if (this.tasks.size <= 1) {
        requestIdlePromiseNoQueue().then(() => {
          this.processTasks();
        });
      }
    });
  }
  processTasks() {
    if (this.tasks.size === 0) {
      return;
    }
    this.tasks.forEach(task => task());
    this.tasks.clear();
  }

  /**
   * Get the RxDocument from the cache
   * and create a new one if not exits before.
   * @overwrites itself with the actual function
   * because this is @performance relevant.
   * It is called on each document row for each write and read.
   */
  get getCachedRxDocuments() {
    var fn = getCachedRxDocumentMonad(this);
    return overwriteGetterForCaching(this, 'getCachedRxDocuments', fn);
  }
  get getCachedRxDocument() {
    var fn = getCachedRxDocumentSingle(this);
    return overwriteGetterForCaching(this, 'getCachedRxDocument', fn);
  }

  /**
   * Throws if not exists
   */
  getLatestDocumentData(docId) {
    this.processTasks();
    var cacheItem = getFromMapOrThrow(this.cacheItemByDocId, docId);
    return cacheItem[1];
  }
  getLatestDocumentDataIfExists(docId) {
    this.processTasks();
    var cacheItem = this.cacheItemByDocId.get(docId);
    if (cacheItem) {
      return cacheItem[1];
    }
  }
}

/**
 * Determines if the given document state has to overwrite
 * the currently cached latest state of that document.
 */
function isNewerDocumentState(checkState, currentLatest) {
  /**
   * When the cleanup has purged the tombstone of a deleted document,
   * a write to the same primary key starts a new revision chain at height 1.
   * That state has a lower revision height than the cached tombstone
   * while still being the newer state, so here we compare by the write time
   * instead of by the revision.
   */
  if (currentLatest._deleted && !checkState._deleted && checkState._meta.lwt > currentLatest._meta.lwt) {
    return true;
  }
  return !isOlderDocumentState(checkState._rev, currentLatest._rev);
}

/**
 * Determines if the document state of checkRev is older than the one of currentRev.
 * Compares by revision height first and falls back to a deterministic
 * comparison of the full revision string so that all instances agree
 * on the same winner when the heights are equal.
 * Equal revisions are not considered older so that the cached reference
 * still gets refreshed, which keeps the previous behavior intact.
 */
function isOlderDocumentState(checkRev, currentRev) {
  if (checkRev === currentRev) {
    return false;
  }
  var checkHeight = getHeightOfRevision(checkRev);
  var currentHeight = getHeightOfRevision(currentRev);
  if (checkHeight !== currentHeight) {
    return checkHeight < currentHeight;
  }
  return checkRev < currentRev;
}
function revAndLwtKey(rev, lwt) {
  return rev + '|' + lwt;
}

/**
 * Returns the cached RxDocument of a document state
 * that has the same revision as the one in the revision Map
 * but a different _meta.lwt.
 */
function getFromOtherStates(cacheItem, rev, lwt) {
  var byRevAndLwt = cacheItem[2];
  if (!byRevAndLwt) {
    return undefined;
  }
  var weakRef = byRevAndLwt.get(revAndLwtKey(rev, lwt));
  return weakRef ? weakRef.deref() : undefined;
}
function addToCacheItem(cacheItem, rev, lwt, rxDocument) {
  var byRev = cacheItem[0];
  var existingWeakRef = byRev.get(rev);
  var existing = existingWeakRef ? existingWeakRef.deref() : undefined;
  if (existing && existing._data._meta.lwt !== lwt) {
    /**
     * Another state with the same revision is still in use,
     * so it must not be replaced.
     */
    var byRevAndLwt = cacheItem[2];
    if (!byRevAndLwt) {
      byRevAndLwt = new Map();
      cacheItem[2] = byRevAndLwt;
    }
    byRevAndLwt.set(revAndLwtKey(rev, lwt), createWeakRefWithFallback(rxDocument));
  } else {
    byRev.set(rev, createWeakRefWithFallback(rxDocument));
  }
}

/**
 * @hotPath Dedicated single-document function that avoids array allocations.
 * Used by getCachedRxDocument which is called from many call sites.
 */
function getCachedRxDocumentSingle(docCache) {
  var primaryPath = docCache.primaryPath;
  var cacheItemByDocId = docCache.cacheItemByDocId;
  var registry = docCache.registry;
  var deepFreezeWhenDevMode = overwritable.deepFreezeWhenDevMode;
  var documentCreator = docCache.documentCreator;
  return docData => {
    var docId = docData[primaryPath];
    var rev = docData._rev;
    var lwt = docData._meta.lwt;
    var cacheItem = cacheItemByDocId.get(docId);
    if (!cacheItem) {
      docData = deepFreezeWhenDevMode(docData);
      var newDoc = documentCreator(docData);
      var newByRev = new Map();
      newByRev.set(rev, createWeakRefWithFallback(newDoc));
      cacheItemByDocId.set(docId, [newByRev, docData]);
      if (registry) {
        docCache.tasks.add(() => {
          registry.register(newDoc, {
            docId: newDoc.primary,
            rev,
            lwt
          });
        });
        if (docCache.tasks.size <= 1) {
          requestIdlePromiseNoQueue().then(() => {
            docCache.processTasks();
          });
        }
      }
      return newDoc;
    }
    var cachedRxDocumentWeakRef = cacheItem[0].get(rev);
    var cachedRxDocument = cachedRxDocumentWeakRef ? cachedRxDocumentWeakRef.deref() : undefined;
    if ((!cachedRxDocument || cachedRxDocument._data._meta.lwt !== lwt) && !(cachedRxDocument = getFromOtherStates(cacheItem, rev, lwt))) {
      docData = deepFreezeWhenDevMode(docData);
      cachedRxDocument = documentCreator(docData);
      addToCacheItem(cacheItem, rev, lwt, cachedRxDocument);
      if (registry) {
        var registeredDoc = cachedRxDocument;
        docCache.tasks.add(() => {
          registry.register(registeredDoc, {
            docId: registeredDoc.primary,
            rev,
            lwt
          });
        });
        if (docCache.tasks.size <= 1) {
          requestIdlePromiseNoQueue().then(() => {
            docCache.processTasks();
          });
        }
      }
    }
    return cachedRxDocument;
  };
}

/**
 * This function is called very very often.
 * @hotPath This is one of the most important methods for performance.
 * It is used in many places to transform the raw document data into RxDocuments.
 */
function getCachedRxDocumentMonad(docCache) {
  var primaryPath = docCache.primaryPath;
  var cacheItemByDocId = docCache.cacheItemByDocId;
  var registry = docCache.registry;
  var deepFreezeWhenDevMode = overwritable.deepFreezeWhenDevMode;
  var documentCreator = docCache.documentCreator;
  var fn = docsData => {
    var ret = new Array(docsData.length);
    var registryTasks;
    for (var index = 0; index < docsData.length; index++) {
      var docData = docsData[index];
      var docId = docData[primaryPath];
      var rev = docData._rev;
      var lwt = docData._meta.lwt;
      var cacheItem = cacheItemByDocId.get(docId);
      if (!cacheItem) {
        /**
         * New document - no need for WeakRef lookup.
         * Create cache item directly.
         */
        docData = deepFreezeWhenDevMode(docData);
        var cachedRxDocument = documentCreator(docData);
        var byRev = new Map();
        byRev.set(rev, createWeakRefWithFallback(cachedRxDocument));
        cacheItemByDocId.set(docId, [byRev, docData]);
        ret[index] = cachedRxDocument;
        if (registry) {
          if (!registryTasks) {
            registryTasks = [];
          }
          registryTasks.push({
            doc: cachedRxDocument,
            rev,
            lwt
          });
        }
      } else {
        var cachedRxDocumentWeakRef = cacheItem[0].get(rev);
        var _cachedRxDocument = cachedRxDocumentWeakRef ? cachedRxDocumentWeakRef.deref() : undefined;
        if ((!_cachedRxDocument || _cachedRxDocument._data._meta.lwt !== lwt) && !(_cachedRxDocument = getFromOtherStates(cacheItem, rev, lwt))) {
          docData = deepFreezeWhenDevMode(docData);
          _cachedRxDocument = documentCreator(docData);
          addToCacheItem(cacheItem, rev, lwt, _cachedRxDocument);
          if (registry) {
            if (!registryTasks) {
              registryTasks = [];
            }
            registryTasks.push({
              doc: _cachedRxDocument,
              rev,
              lwt
            });
          }
        }
        ret[index] = _cachedRxDocument;
      }
    }
    if (registryTasks && registry) {
      /**
       * Calling registry.register() has shown to have
       * really bad performance. So we add the cached documents
       * lazily.
       */
      var tasks = registryTasks;
      docCache.tasks.add(() => {
        for (var _index = 0; _index < tasks.length; _index++) {
          var task = tasks[_index];
          registry.register(task.doc, {
            docId: task.doc.primary,
            rev: task.rev,
            lwt: task.lwt
          });
        }
      });
      if (docCache.tasks.size <= 1) {
        requestIdlePromiseNoQueue().then(() => {
          docCache.processTasks();
        });
      }
    }
    return ret;
  };
  return fn;
}
export function mapDocumentsDataToCacheDocs(docCache, docsData) {
  var getCachedRxDocuments = docCache.getCachedRxDocuments;
  return getCachedRxDocuments(docsData);
}

/**
 * Fallback for JavaScript runtimes that do not support WeakRef.
 * The fallback will keep the items in cache forever,
 * but at least works.
 */
var HAS_WEAK_REF = typeof WeakRef === 'function';
var createWeakRefWithFallback = HAS_WEAK_REF ? createWeakRef : createWeakRefFallback;
function createWeakRef(obj) {
  return new WeakRef(obj);
}
function createWeakRefFallback(obj) {
  return {
    deref() {
      return obj;
    }
  };
}
//# sourceMappingURL=doc-cache.js.map