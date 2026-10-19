"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RxQueryBase = void 0;
exports._getDefaultQuery = _getDefaultQuery;
exports.createRxQuery = createRxQuery;
exports.isFindOneByIdQuery = isFindOneByIdQuery;
exports.isRxQuery = isRxQuery;
exports.queryCollection = queryCollection;
exports.tunnelQueryCache = tunnelQueryCache;
var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _rxjs = require("rxjs");
var _operators = require("rxjs/operators");
var _index = require("./plugins/utils/index.js");
var _rxError = require("./rx-error.js");
var _hooks = require("./hooks.js");
var _eventReduce = require("./event-reduce.js");
var _queryCache = require("./query-cache.js");
var _rxQueryHelper = require("./rx-query-helper.js");
var _rxQuerySingleResult = require("./rx-query-single-result.js");
var _queryCount = 0;
var newQueryID = function () {
  return ++_queryCount;
};

/**
 * Counter for _lastEnsureEqual.
 * We only need ordering and zero-check for cache replacement,
 * so a counter is cheaper than Date.now().
 */
var _ensureEqualCount = 0;
var RxQueryBase = exports.RxQueryBase = /*#__PURE__*/function () {
  function RxQueryBase(op, mangoQuery, collection,
  // used by some plugins
  other = {}) {
    (0, _defineProperty2.default)(this, "id", newQueryID());
    /**
     * Some stats then are used for debugging and cache replacement policies
     */
    (0, _defineProperty2.default)(this, "_execOverDatabaseCount", 0);
    /**
     * @performance
     * Use Date.now() instead of now() for creation time.
     * The monotonic uniqueness guarantee of now() is not needed here
     * since _creationTime is only used by the cache replacement policy
     * for rough lifetime comparisons.
     */
    (0, _defineProperty2.default)(this, "_creationTime", Date.now());
    // used in the query-cache to determine if the RxQuery can be cleaned up.
    // 0 means never executed. Updated to an incrementing counter on each _ensureEqual call.
    (0, _defineProperty2.default)(this, "_lastEnsureEqual", 0);
    (0, _defineProperty2.default)(this, "uncached", false);
    // used to count the subscribers to the query
    // Lazy-initialized to avoid BehaviorSubject overhead for .exec()-only queries
    (0, _defineProperty2.default)(this, "_refCount$", null);
    (0, _defineProperty2.default)(this, "isFindOneByIdQuery", void 0);
    /**
     * Contains the current result state
     * or null if query has not run yet.
     */
    (0, _defineProperty2.default)(this, "_result", null);
    // stores the changeEvent-number of the last handled change-event
    (0, _defineProperty2.default)(this, "_latestChangeEvent", -1);
    /**
     * ensures that the exec-runs
     * are not run in parallel
     */
    (0, _defineProperty2.default)(this, "_ensureEqualQueue", _index.PROMISE_RESOLVE_FALSE);
    /**
     * Returns an observable that emits the results
     * This should behave like an rxjs-BehaviorSubject which means:
     * - Emit the current result-set on subscribe
     * - Emit the new result-set when an RxChangeEvent comes in
     * - Do not emit anything before the first result-set was created (no null)
     */
    (0, _defineProperty2.default)(this, "_$", void 0);
    this.op = op;
    this.mangoQuery = mangoQuery;
    this.collection = collection;
    this.other = other;
    if (!mangoQuery) {
      this.mangoQuery = _getDefaultQuery();
    }

    /**
     * @performance
     * isFindOneByIdQuery is only used by queryCollection()
     * which is not called for 'count' queries.
     * Skip the check for count queries to avoid unnecessary work.
     */
    if (op === 'count') {
      this.isFindOneByIdQuery = false;
    } else {
      this.isFindOneByIdQuery = isFindOneByIdQuery(this.collection.schema.primaryPath, mangoQuery);
    }
  }
  var _proto = RxQueryBase.prototype;
  /**
   * set the new result-data as result-docs of the query
   * @param newResultData json-docs that were received from the storage
   */
  _proto._setResultData = function _setResultData(newResultData) {
    if (typeof newResultData === 'undefined') {
      throw (0, _rxError.newRxError)('QU18', {
        database: this.collection.database.name,
        collection: this.collection.name
      });
    }
    if (typeof newResultData === 'number') {
      this._result = new _rxQuerySingleResult.RxQuerySingleResult(this, [], newResultData);
      return;
    } else if (newResultData instanceof Map) {
      newResultData = Array.from(newResultData.values());
    }
    var newQueryResult = new _rxQuerySingleResult.RxQuerySingleResult(this, newResultData, newResultData.length);
    this._result = newQueryResult;
  }

  /**
   * executes the query on the database
   * @return results-array with document-data
   */;
  _proto._execOverDatabase = async function _execOverDatabase(rerunCount = 0) {
    this._execOverDatabaseCount = this._execOverDatabaseCount + 1;
    var result;

    /**
     * Snapshot the counter before running the query.
     * If a write commits during the query, the counter will be higher
     * when we check it below, and we need to signal re-evaluation.
     */
    var counterBefore = this.collection._changeEventBuffer.getCounter();
    if (this.op === 'findByIds') {
      var ids = (0, _index.ensureNotFalsy)(this.mangoQuery.selector)[this.collection.schema.primaryPath].$in;
      var docsData = [];
      var mustBeQueried = [];
      // first try to fill from docCache
      for (var i = 0; i < ids.length; i++) {
        var id = ids[i];
        var docData = this.collection._docCache.getLatestDocumentDataIfExists(id);
        if (docData) {
          if (!docData._deleted) {
            docsData.push(docData);
          }
        } else {
          mustBeQueried.push(id);
        }
      }
      // everything which was not in docCache must be fetched from the storage
      if (mustBeQueried.length > 0) {
        var docs = await this.collection.storageInstance.findDocumentsById(mustBeQueried, false);
        for (var _i = 0; _i < docs.length; _i++) {
          docsData.push(docs[_i]);
        }
      }
      result = {
        result: docsData,
        counter: this.collection._changeEventBuffer.getCounter()
      };
    } else if (this.op === 'count') {
      var preparedQuery = this.getPreparedQuery();
      var countResult = await this.collection.storageInstance.count(preparedQuery);
      if (countResult.mode === 'slow' && !this.collection.database.allowSlowCount) {
        throw (0, _rxError.newRxError)('QU14', {
          collection: this.collection,
          queryObj: this.mangoQuery
        });
      } else {
        result = {
          result: countResult.count,
          counter: this.collection._changeEventBuffer.getCounter()
        };
      }
    } else {
      var queryResult = await queryCollection(this);
      result = {
        result: queryResult.docs,
        counter: queryResult.counter
      };
    }

    /**
     * If a write committed concurrently during our query, our result may be
     * stale: the write's change event was already buffered so getCounter()
     * inside queryCollection() advanced _latestChangeEvent past the event,
     * which would cause subsequent _ensureEqual() calls to see
     * _isResultsInSync()=true and skip re-evaluation entirely.
     *
     * Re-run the query when a write commits concurrently. We still yield
     * with a fixed wait so non-async storages do not monopolize the event
     * loop, but we avoid the old growing backoff that could delay
     * convergence more and more under write pressure.
     *
     * @link https://github.com/pubkey/rxdb/issues/7067
     */
    if (this.collection._changeEventBuffer.getCounter() !== counterBefore) {
      await (0, _index.promiseWait)(20);
      return this._execOverDatabase(rerunCount + 1);
    }
    return result;
  }

  /**
   * Execute the query
   * To have an easier implementations,
   * just subscribe and use the first result
   */;
  _proto.exec = async function exec(throwIfMissing) {
    if (throwIfMissing && this.op !== 'findOne') {
      throw (0, _rxError.newRxError)('QU9', {
        collection: this.collection.name,
        query: this.mangoQuery,
        op: this.op
      });
    }

    /**
     * run _ensureEqual() here,
     * this will make sure that errors in the query which throw inside of the RxStorage,
     * will be thrown at this execution context and not in the background.
     */
    await _ensureEqual(this);
    var useResult = (0, _index.ensureNotFalsy)(this._result);
    return useResult.getValue(throwIfMissing);
  }

  /**
   * Returns the normalized query.
   * Caches the result so that multiple calls to
   * queryMatcher, toString() and getPreparedQuery()
   * do not have to run the normalization again.
   * @overwrites itself with the actual value.
   */;
  /**
   * returns a string that is used for equal-comparisons
   * @overwrites itself with the actual value
   */
  _proto.toString = function toString() {
    /**
     * For findByIds queries, build the cache key directly from the IDs
     * to avoid the expensive normalizeMangoQuery + sortObject + JSON.stringify.
     * The selector structure is guaranteed by findByIds() which always creates
     * { [primaryPath]: { $in: ids } }
     *
     * @performance The ids are not sorted because sorting
     * is expensive for many ids. Also the order of the ids
     * defines the iteration order of the result Map, so queries
     * with a different id order must not share the same cache key.
     */
    var value;
    if (this.op === 'findByIds') {
      var ids = this.mangoQuery.selector[this.collection.schema.primaryPath].$in;
      value = '|findByIds|' + JSON.stringify(ids);
    } else {
      var stringObj = (0, _index.sortObject)({
        op: this.op,
        query: this.normalizedQuery,
        other: this.other
      }, true);
      value = JSON.stringify(stringObj);
    }
    this.toString = () => value;
    return value;
  }

  /**
   * returns the prepared query
   * which can be sent to the storage instance to query for documents.
   * @overwrites itself with the actual value.
   */;
  _proto.getPreparedQuery = function getPreparedQuery() {
    var hookInput = {
      rxQuery: this,
      // can be mutated by the hooks so we have to deep clone first.
      mangoQuery: (0, _index.clone)(this.normalizedQuery)
    };
    hookInput.mangoQuery.selector._deleted = {
      $eq: false
    };
    if (hookInput.mangoQuery.index) {
      hookInput.mangoQuery.index.unshift('_deleted');
    }
    (0, _hooks.runPluginHooks)('prePrepareQuery', hookInput);
    var value = (0, _rxQueryHelper.prepareQuery)(this.collection.schema.jsonSchema, hookInput.mangoQuery);
    this.getPreparedQuery = () => value;
    return value;
  }

  /**
   * returns true if the document matches the query,
   * does not use the 'skip' and 'limit'
   */;
  _proto.doesDocumentDataMatch = function doesDocumentDataMatch(docData) {
    // if doc is deleted, it cannot match
    if (docData._deleted) {
      return false;
    }
    return this.queryMatcher(docData);
  }

  /**
   * deletes all found documents
   * @return promise with deleted documents
   */;
  _proto.remove = async function remove(throwIfMissing) {
    if (throwIfMissing && this.op !== 'findOne') {
      throw (0, _rxError.newRxError)('QU9', {
        collection: this.collection.name,
        query: this.mangoQuery,
        op: this.op
      });
    }
    var docs = await this.exec();
    if (Array.isArray(docs)) {
      var result = await this.collection.bulkRemove(docs);
      if (result.error.length > 0) {
        throw (0, _rxError.rxStorageWriteErrorToRxError)(result.error[0]);
      } else {
        return result.success;
      }
    } else if (docs instanceof Map) {
      var docsArray = [...docs.values()];
      var _result = await this.collection.bulkRemove(docsArray);
      if (_result.error.length > 0) {
        throw (0, _rxError.rxStorageWriteErrorToRxError)(_result.error[0]);
      }
      var resultMap = new Map();
      _result.success.forEach(doc => resultMap.set(doc.primary, doc));
      return resultMap;
    } else {
      // findOne() can return null when no document matches
      if (!docs) {
        if (throwIfMissing) {
          throw (0, _rxError.newRxError)('QU10', {
            collection: this.collection.name,
            query: this.mangoQuery,
            op: this.op
          });
        }
        return null;
      }
      return docs.remove();
    }
  };
  _proto.incrementalRemove = function incrementalRemove() {
    return (0, _rxQueryHelper.runQueryUpdateFunction)(this.asRxQuery, doc => doc.incrementalRemove());
  }

  /**
   * helper function to transform RxQueryBase to RxQuery type
   */;
  /**
   * updates all found documents
   * @overwritten by plugin (optional)
   */
  _proto.update = function update(_updateObj) {
    throw (0, _index.pluginMissing)('update');
  };
  _proto.patch = function patch(_patch) {
    return (0, _rxQueryHelper.runQueryUpdateFunction)(this.asRxQuery, doc => doc.patch(_patch));
  };
  _proto.incrementalPatch = function incrementalPatch(patch) {
    return (0, _rxQueryHelper.runQueryUpdateFunction)(this.asRxQuery, doc => doc.incrementalPatch(patch));
  };
  _proto.modify = function modify(mutationFunction) {
    return (0, _rxQueryHelper.runQueryUpdateFunction)(this.asRxQuery, doc => doc.modify(mutationFunction));
  };
  _proto.incrementalModify = function incrementalModify(mutationFunction) {
    return (0, _rxQueryHelper.runQueryUpdateFunction)(this.asRxQuery, doc => doc.incrementalModify(mutationFunction));
  }

  // we only set some methods of query-builder here
  // because the others depend on these ones
;
  _proto.where = function where(_queryObj) {
    throw (0, _index.pluginMissing)('query-builder');
  };
  _proto.sort = function sort(_params) {
    throw (0, _index.pluginMissing)('query-builder');
  };
  _proto.skip = function skip(_amount) {
    throw (0, _index.pluginMissing)('query-builder');
  };
  _proto.limit = function limit(_amount) {
    throw (0, _index.pluginMissing)('query-builder');
  };
  return (0, _createClass2.default)(RxQueryBase, [{
    key: "refCount$",
    get: function () {
      if (!this._refCount$) {
        this._refCount$ = new _rxjs.BehaviorSubject(null);
      }
      return this._refCount$;
    }
  }, {
    key: "$",
    get: function () {
      if (!this._$) {
        var changeEventBulks$ = this.collection.eventBulks$.pipe(
        /**
         * Performance shortcut.
         * Changes to local documents are not relevant for the query.
         */
        (0, _operators.filter)(bulk => !bulk.isLocal));
        var liveQueryUpdateThrottleTime = getLiveQueryUpdateThrottleTime(this.collection);
        var updateTrigger$ = liveQueryUpdateThrottleTime > 0 ? (0, _rxjs.merge)((0, _rxjs.of)(null), changeEventBulks$.pipe((0, _operators.auditTime)(liveQueryUpdateThrottleTime))) : changeEventBulks$.pipe(
        /**
         * Start once to ensure the querying also starts
         * when there where no changes.
         */
        (0, _operators.startWith)(null));
        var results$ = updateTrigger$.pipe(
        /**
         * Ensure query results are up to date.
         * When enabled, liveQueryUpdateThrottleTime groups change-triggered
         * reruns before this point so downstream throttling is not too late.
         */
        (0, _operators.mergeMap)(() => _ensureEqual(this)),
        // use the current result set, written by _ensureEqual().
        (0, _operators.map)(() => this._result),
        // do not run stuff above for each new subscriber, only once.
        (0, _operators.shareReplay)(_index.RXJS_SHARE_REPLAY_DEFAULTS),
        // do not proceed if result set has not changed.
        (0, _operators.distinctUntilChanged)((prev, curr) => {
          if (prev && prev.time === (0, _index.ensureNotFalsy)(curr).time) {
            return true;
          } else {
            return false;
          }
        }), (0, _operators.filter)(result => !!result),
        /**
         * Map the result set to a single RxDocument or an array,
         * depending on query type
         */
        (0, _operators.map)(result => {
          return (0, _index.ensureNotFalsy)(result).getValue();
        }));
        this._$ = (0, _rxjs.merge)(results$,
        /**
         * Also add the refCount$ to the query observable
         * to allow us to count the amount of subscribers.
         */
        this.refCount$.pipe((0, _operators.filter)(() => false)));
      }
      return this._$;
    }
  }, {
    key: "$$",
    get: function () {
      var reactivity = this.collection.database.getReactivityFactory();
      return reactivity.fromObservable(this.$, undefined, this.collection.database);
    }
  }, {
    key: "normalizedQuery",
    get: function () {
      return (0, _index.overwriteGetterForCaching)(this, 'normalizedQuery', (0, _rxQueryHelper.normalizeMangoQuery)(this.collection.schema.jsonSchema, this.mangoQuery, this.op === 'count'));
    }

    /**
     * cached call to get the queryMatcher
     * @overwrites itself with the actual value
     */
  }, {
    key: "queryMatcher",
    get: function () {
      var schema = this.collection.schema.jsonSchema;
      return (0, _index.overwriteGetterForCaching)(this, 'queryMatcher', (0, _rxQueryHelper.getQueryMatcher)(schema, this.normalizedQuery));
    }
  }, {
    key: "asRxQuery",
    get: function () {
      return this;
    }
  }]);
}();
function _getDefaultQuery() {
  return {
    selector: {}
  };
}
function getLiveQueryUpdateThrottleTime(collection) {
  if (typeof collection.liveQueryUpdateThrottleTime === 'number') {
    return collection.liveQueryUpdateThrottleTime;
  }
  return collection.database.liveQueryUpdateThrottleTime;
}

/**
 * run this query through the QueryCache
 */
function tunnelQueryCache(rxQuery) {
  return rxQuery.collection._queryCache.getByQuery(rxQuery);
}
function createRxQuery(op, queryObj, collection, other) {
  (0, _hooks.runPluginHooks)('preCreateRxQuery', {
    op,
    queryObj,
    collection,
    other
  });
  var ret = new RxQueryBase(op, queryObj, collection, other);

  // ensure when created with same params, only one is created
  ret = tunnelQueryCache(ret);
  (0, _queryCache.triggerCacheReplacement)(collection);
  return ret;
}

/**
 * Check if the current results-state is in sync with the database
 * which means that no write event happened since the last run.
 * @return false if not which means it should re-execute
 */
function _isResultsInSync(rxQuery) {
  var currentLatestEventNumber = rxQuery.asRxQuery.collection._changeEventBuffer.getCounter();
  if (rxQuery._latestChangeEvent >= currentLatestEventNumber) {
    return true;
  } else {
    return false;
  }
}

/**
 * wraps __ensureEqual()
 * to ensure it does not run in parallel
 * @return true if has changed, false if not
 *
 * @performance
 * Avoid async wrapper when awaitBeforeReads is empty (common case).
 * This eliminates one unnecessary Promise allocation per query execution.
 */
function _ensureEqual(rxQuery) {
  if (rxQuery.collection.awaitBeforeReads.size > 0) {
    return Promise.all(Array.from(rxQuery.collection.awaitBeforeReads).map(fn => fn())).then(() => {
      rxQuery._ensureEqualQueue = rxQuery._ensureEqualQueue.then(() => __ensureEqual(rxQuery));
      return rxQuery._ensureEqualQueue;
    });
  }
  rxQuery._ensureEqualQueue = rxQuery._ensureEqualQueue.then(() => __ensureEqual(rxQuery));
  return rxQuery._ensureEqualQueue;
}

/**
 * ensures that the results of this query is equal to the results which a query over the database would give
 * @return true if results have changed
 */
function __ensureEqual(rxQuery) {
  /**
   * @performance
   * Use a counter instead of Date.now() since _lastEnsureEqual
   * is only used by the cache replacement policy for sorting queries
   * by last usage and zero-check, not for time-based comparison.
   */
  rxQuery._lastEnsureEqual = ++_ensureEqualCount;

  /**
   * Optimisation shortcuts
   */
  if (
  // db is closed
  rxQuery.collection.database.closed ||
  // nothing happened since last run
  _isResultsInSync(rxQuery)) {
    return _index.PROMISE_RESOLVE_FALSE;
  }
  var ret = false;
  var mustReExec = false; // if this becomes true, a whole execution over the database is made
  if (rxQuery._latestChangeEvent === -1) {
    // have not executed yet -> must run
    mustReExec = true;
  }

  /**
   * try to use EventReduce to calculate the new results
   */
  if (!mustReExec) {
    var missedChangeEvents = rxQuery.asRxQuery.collection._changeEventBuffer.getFrom(rxQuery._latestChangeEvent + 1);
    if (missedChangeEvents === null) {
      // changeEventBuffer is of bounds -> we must re-execute over the database
      mustReExec = true;
    } else {
      rxQuery._latestChangeEvent = rxQuery.asRxQuery.collection._changeEventBuffer.getCounter();
      var runChangeEvents = rxQuery.asRxQuery.collection._changeEventBuffer.reduceByLastOfDoc(missedChangeEvents);
      if (rxQuery.op === 'count') {
        // 'count' query
        var previousCount = (0, _index.ensureNotFalsy)(rxQuery._result).count;
        var newCount = previousCount;
        runChangeEvents.forEach(cE => {
          var didMatchBefore = cE.previousDocumentData && rxQuery.doesDocumentDataMatch(cE.previousDocumentData);
          var doesMatchNow = rxQuery.doesDocumentDataMatch(cE.documentData);
          if (!didMatchBefore && doesMatchNow) {
            newCount++;
          }
          if (didMatchBefore && !doesMatchNow) {
            newCount--;
          }
        });
        if (newCount !== previousCount) {
          ret = true; // true because results changed
          rxQuery._setResultData(newCount);
        }
      } else {
        // 'find' or 'findOne' query
        var eventReduceResult = (0, _eventReduce.calculateNewResults)(rxQuery, runChangeEvents);
        if (eventReduceResult.runFullQueryAgain) {
          // could not calculate the new results, execute must be done
          mustReExec = true;
        } else {
          var positiveResult = eventReduceResult;
          if (positiveResult.changed) {
            // we got the new results, we do not have to re-execute, mustReExec stays false
            ret = true; // true because results changed
            rxQuery._setResultData(positiveResult.newResults);
          }
        }
      }
    }
  }

  // oh no we have to re-execute the whole query over the database
  if (mustReExec) {
    return rxQuery._execOverDatabase().then(result => {
      var newResultData = result.result;

      /**
       * The RxStorage is defined to always first emit events and then return
       * on bulkWrite() calls. So here we have to use the counter AFTER the execOverDatabase()
       * has been run, not the one from before.
       */
      rxQuery._latestChangeEvent = result.counter;

      // A count query needs a different has-changed check.
      if (typeof newResultData === 'number') {
        if (!rxQuery._result || newResultData !== rxQuery._result.count) {
          ret = true;
          rxQuery._setResultData(newResultData);
        }
        return ret;
      }
      if (!rxQuery._result || !(0, _index.areRxDocumentArraysEqual)(rxQuery.collection.schema.primaryPath, newResultData, rxQuery._result.docsData)) {
        ret = true; // true because results changed
        rxQuery._setResultData(newResultData);
      }
      return ret;
    });
  }
  return Promise.resolve(ret); // true if results have changed
}

/**
 * Runs the query over the storage instance
 * of the collection.
 * Does some optimizations to ensure findById is used
 * when specific queries are used.
 */
async function queryCollection(rxQuery) {
  var docs = [];
  var collection = rxQuery.collection;

  /**
   * Optimizations shortcut.
   * If query is find-one-document-by-id,
   * then we do not have to use the slow query() method
   * but instead can use findDocumentsById()
   */
  if (rxQuery.isFindOneByIdQuery) {
    var selector = rxQuery.mangoQuery.selector;
    var primaryPath = collection.schema.primaryPath;
    // isFindOneByIdQuery guarantees the primary key is in the selector
    var primarySelectorValue = selector ? selector[primaryPath] : undefined;

    // Check if there are extra operators on the primary key selector (e.g. $ne alongside $in)
    var hasExtraOperators = typeof primarySelectorValue === 'object' && primarySelectorValue !== null && Object.keys(primarySelectorValue).length > 1;

    // Check if there are selectors OTHER than the primary key
    var hasOtherSelectors = selector ? Object.keys(selector).length > 1 : false;

    // Normalize single ID to array and de-duplicate to avoid returning the same document multiple times
    var docIdArray = Array.isArray(rxQuery.isFindOneByIdQuery) ? rxQuery.isFindOneByIdQuery : [rxQuery.isFindOneByIdQuery];
    var docIds = Array.from(new Set(docIdArray));

    // Separate cache hits from storage misses
    var cacheMisses = [];
    docIds.forEach(docId => {
      var docData = rxQuery.collection._docCache.getLatestDocumentDataIfExists(docId);
      if (docData && !docData._deleted) {
        docs.push(docData);
      } else if (!docData) {
        // Only fetch from storage if not in cache
        cacheMisses.push(docId);
      }
      // If found but deleted, skip entirely (no refetch)
    });

    // Fetch only cache misses from storage
    if (cacheMisses.length > 0) {
      var docsFromStorage = await collection.storageInstance.findDocumentsById(cacheMisses, false);
      docs = docs.concat(docsFromStorage);
    }

    // Apply query matcher if there are extra operators or other selectors
    if (hasExtraOperators || hasOtherSelectors) {
      docs = docs.filter(doc => rxQuery.queryMatcher(doc));
    }

    /**
     * The findDocumentsById() fast-path also does not apply `skip`/`limit`/`sort`.
     * To keep behavior consistent with storageInstance.query(), we must
     * apply them after queryMatcher for both find() and findOne() queries.
     */
    // Apply sorting for both find and findOne
    if (docs.length > 1) {
      var preparedQuery = rxQuery.getPreparedQuery();
      var sortComparator = (0, _rxQueryHelper.getSortComparator)(collection.schema.jsonSchema, preparedQuery.query);
      docs = docs.sort(sortComparator);
    }

    // Apply skip for both find and findOne
    var skip = typeof rxQuery.mangoQuery.skip === 'number' && rxQuery.mangoQuery.skip > 0 ? rxQuery.mangoQuery.skip : 0;
    if (skip > 0) {
      docs = docs.slice(skip);
    }

    // Apply limit for both find and findOne
    var limitIsNumber = typeof rxQuery.mangoQuery.limit === 'number' && rxQuery.mangoQuery.limit > 0;
    if (limitIsNumber) {
      var limit = rxQuery.mangoQuery.limit;
      docs = docs.slice(0, limit);
    }
  } else {
    var _preparedQuery = rxQuery.getPreparedQuery();
    var queryResult = await collection.storageInstance.query(_preparedQuery);
    docs = queryResult.documents;
  }
  return {
    docs,
    counter: collection._changeEventBuffer.getCounter()
  };
}

/**
 * Returns true if the given query
 * selects documents by primary key using $eq or $in.
 * Used to optimize performance: these queries use get-by-id
 * instead of a full index scan. Additional operators beyond
 * $eq/$in are handled via the queryMatcher after fetching.
 * Skip, limit, and sort are also applied after fetching.
 * Returns false if no such optimization is possible.
 * Returns the document id (string) or ids (string[]) otherwise.
 */
function isFindOneByIdQuery(primaryPath, query) {
  // primary key constraint can coexist with other selectors, skip, limit, and sort
  // The optimization will fetch by ID, then apply queryMatcher, sort, skip, and limit
  // Use hasOwnProperty to avoid prototype pollution from user-controlled input
  if (query.selector && Object.prototype.hasOwnProperty.call(query.selector, primaryPath)) {
    var value = query.selector[primaryPath];
    if (typeof value === 'string') {
      return value;
    } else if (typeof value.$eq === 'string') {
      return value.$eq;
    }

    // same with $in string arrays
    if (Array.isArray(value.$in) &&
    // must only contain strings
    !value.$in.find(r => typeof r !== 'string')) {
      return value.$in;
    }
  }
  return false;
}
function isRxQuery(obj) {
  return obj instanceof RxQueryBase;
}
//# sourceMappingURL=rx-query.js.map