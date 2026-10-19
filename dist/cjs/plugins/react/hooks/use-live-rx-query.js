"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useLiveRxQuery = useLiveRxQuery;
var _useRxQuery = require("./use-rx-query.js");
/**
 * React hook to query an RxDB collection with Mango queries.
 *
 * @param {UseRxQueryOptions<RxDocumentType, OrmMethods, StaticMethods, InstanceCreationOptions, Reactivity>} options - Options for the query.
 * @param {string|RxCollection} options.collection - The collection name or instance to query.
 * @param {MangoQuery<RxDocumentType>} options.query - The Mango query to execute.
 *
 * @returns {UseRxQueryResult<RxDocumentType, OrmMethods>} The query result, loading state, and error.
 */
function useLiveRxQuery({
  collection,
  query
}) {
  return (0, _useRxQuery.useRxQueryBase)({
    collection,
    query,
    live: true
  });
}
//# sourceMappingURL=use-live-rx-query.js.map