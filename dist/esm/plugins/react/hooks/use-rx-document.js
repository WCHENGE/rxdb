import { useState, useEffect } from 'react';
import { isRxCollection } from "../../../rx-collection.js";
import { newRxError } from "../../../rx-error.js";
/**
 * React hook to subscribe to a single RxDB document by its primary key.
 * Returns the document with live updates when it changes.
 *
 * @param collection - The RxCollection instance to query from.
 * @param primaryKey - The primary key value of the document to subscribe to.
 * @returns The document, loading state, and error.
 */
export function useRxDocument(collection, primaryKey) {
  var [result, setResult] = useState(null);
  var [loading, setLoading] = useState(Boolean(collection) && primaryKey !== undefined);
  var [error, setError] = useState(null);
  useEffect(() => {
    if (!collection || primaryKey === undefined) {
      setResult(null);
      return;
    }
    if (!isRxCollection(collection)) {
      throw newRxError('R3', {
        collection
      });
    }
    setLoading(true);
    setError(null);
    var subscription = collection.findOne(primaryKey).$.subscribe({
      next: doc => {
        setResult(doc);
        setLoading(false);
      },
      error: err => {
        setError(err.message);
        setLoading(false);
      }
    });
    return () => {
      subscription.unsubscribe();
    };
  }, [collection, primaryKey]);
  return {
    result,
    loading,
    error
  };
}
//# sourceMappingURL=use-rx-document.js.map