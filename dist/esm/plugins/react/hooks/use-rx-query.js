import { useCallback, useEffect, useState } from 'react';
import { isRxCollection } from "../../../rx-collection.js";
import { useRxCollection } from "./use-rx-collection.js";
import { newRxError } from "../../../rx-error.js";
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
export function useRxQueryBase({
  collection,
  query,
  live
}) {
  var [results, setResults] = useState([]);
  var [loading, setLoading] = useState(true);
  var [error, setError] = useState(null);
  var dbCollection;
  if (typeof collection === 'string') {
    dbCollection = useRxCollection(collection);
  } else {
    if (!isRxCollection(collection)) {
      throw newRxError('R3', {
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
  var runQuery = useCallback(() => {
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
  useEffect(() => {
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
export function useRxQuery({
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