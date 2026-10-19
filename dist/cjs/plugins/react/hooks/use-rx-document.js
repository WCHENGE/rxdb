"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useRxDocument = useRxDocument;
var _react = require("react");
var _rxCollection = require("../../../rx-collection.js");
var _rxError = require("../../../rx-error.js");
/**
 * React hook to subscribe to a single RxDB document by its primary key.
 * Returns the document with live updates when it changes.
 *
 * @param collection - The RxCollection instance to query from.
 * @param primaryKey - The primary key value of the document to subscribe to.
 * @returns The document, loading state, and error.
 */
function useRxDocument(collection, primaryKey) {
  var [result, setResult] = (0, _react.useState)(null);
  var [loading, setLoading] = (0, _react.useState)(Boolean(collection) && primaryKey !== undefined);
  var [error, setError] = (0, _react.useState)(null);
  (0, _react.useEffect)(() => {
    if (!collection || primaryKey === undefined) {
      setResult(null);
      return;
    }
    if (!(0, _rxCollection.isRxCollection)(collection)) {
      throw (0, _rxError.newRxError)('R3', {
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