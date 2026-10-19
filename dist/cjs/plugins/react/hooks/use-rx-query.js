"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useRxQuery = useRxQuery;
exports.useRxQueryBase = useRxQueryBase;
var _react = require("react");
var _rxCollection = require("../../../rx-collection.js");
var _useRxCollection = require("./use-rx-collection.js");
var _rxError = require("../../../rx-error.js");
/**
 * React hook to query an RxDB collection with Mango queries.
 *
 * @param {UseRxQueryOptions<RxDocumentType, OrmMethods, StaticMethods, InstanceCreationOptions, Reactivity>} options - Options for the query.
 * @param {string|RxCollection} options.collection - The collection name or instance to query.
 * @param {MangoQuery<RxDocumentType>} options.query - The Mango query to execute.
 * @param {boolean} [options.live] - Whether to subscribe to live query updates.
 *
 * @returns {UseRxQueryResult<RxDocumentType, OrmMethods>} The query result, loading state, and error.
 */
function useRxQueryBase({
  collection,
  query,
  live
}) {
  var [results, setResults] = (0, _react.useState)([]);
  var [loading, setLoading] = (0, _react.useState)(true);
  var [error, setError] = (0, _react.useState)(null);
  var dbCollection;
  if (typeof collection === 'string') {
    dbCollection = (0, _useRxCollection.useRxCollection)(collection);
  } else {
    if (!(0, _rxCollection.isRxCollection)(collection)) {
      throw (0, _rxError.newRxError)('R3', {
        collection
      });
    }
    dbCollection = collection;
  }
  var emitResults = res => {
    setResults(res);
    if (loading) {
      setLoading(false);
    }
  };
  var emitError = e => {
    setError(e.message);
    if (loading) {
      setLoading(false);
    }
  };
  var runQuery = (0, _react.useCallback)(() => {
    if (dbCollection == null) {
      return;
    }
    setError(null);
    setLoading(true);
    var rxQuery = dbCollection.find(query);
    if (live) {
      var subscription = rxQuery.$.subscribe({
        next: res => emitResults(res),
        error: err => emitError(err)
      });
      return () => {
        subscription.unsubscribe();
      };
    } else {
      rxQuery.exec().then(res => emitResults(res), e => emitError(e));
    }
  }, [dbCollection, query]);
  (0, _react.useEffect)(() => {
    if (collection == null) {
      return;
    }
    var cleanup = runQuery();
    if (typeof cleanup === 'function') {
      return cleanup;
    }
  }, [runQuery]);
  return {
    results,
    loading,
    error
  };
}
function useRxQuery({
  collection,
  query
}) {
  return useRxQueryBase({
    collection,
    query,
    live: false
  });
}
//# sourceMappingURL=use-rx-query.js.map