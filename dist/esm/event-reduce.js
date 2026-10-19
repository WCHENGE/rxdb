import { calculateActionName, runAction } from 'event-reduce-js';
import { rxChangeEventToEventReduceChangeEvent } from "./rx-change-event.js";
import { clone, ensureNotFalsy, getFromMapOrCreate } from "./plugins/utils/index.js";
import { getQueryMatcher, getSortComparator, normalizeMangoQuery } from "./rx-query-helper.js";
export function getSortFieldsOfQuery(primaryKey, query) {
  if (!query.sort || query.sort.length === 0) {
    return [primaryKey];
  } else {
    return query.sort.map(part => Object.keys(part)[0]);
  }
}
export var RXQUERY_QUERY_PARAMS_CACHE = new WeakMap();
export function getQueryParams(rxQuery) {
  return getFromMapOrCreate(RXQUERY_QUERY_PARAMS_CACHE, rxQuery, () => {
    var collection = rxQuery.collection;
    var normalizedMangoQuery = normalizeMangoQuery(collection.storageInstance.schema, clone(rxQuery.mangoQuery));
    var primaryKey = collection.schema.primaryPath;

    /**
     * Create a custom sort comparator
     * that uses the hooks to ensure
     * we send for example compressed documents to be sorted by compressed queries.
     *
     * @performance
     * Avoid creating intermediate wrapper objects on every comparison call.
     * The sortComparator and queryMatcher are called directly.
     */
    var useSortComparator = getSortComparator(collection.schema.jsonSchema, normalizedMangoQuery);

    /**
     * Create a custom query matcher
     * that uses the hooks to ensure
     * we send for example compressed documents to match compressed queries.
     */
    var useQueryMatcher = getQueryMatcher(collection.schema.jsonSchema, normalizedMangoQuery);
    var ret = {
      primaryKey: rxQuery.collection.schema.primaryPath,
      skip: normalizedMangoQuery.skip,
      limit: normalizedMangoQuery.limit,
      sortFields: getSortFieldsOfQuery(primaryKey, normalizedMangoQuery),
      sortComparator: useSortComparator,
      queryMatcher: useQueryMatcher
    };
    return ret;
  });
}
export function calculateNewResults(rxQuery, rxChangeEvents) {
  if (!rxQuery.collection.database.eventReduce) {
    return {
      runFullQueryAgain: true
    };
  }
  var queryParams = getQueryParams(rxQuery);
  var previousResults = ensureNotFalsy(rxQuery._result).docsData.slice(0);
  /**
   * Copy the map to avoid mutating the cached docsDataMap on the result object.
   * runAction() modifies the map in-place (adds/removes entries),
   * which would corrupt the cached map if a later event triggers runFullQueryAgain
   * and the full re-exec returns the same results (keeping the old result object).
   * On subsequent event-reduce calls, the corrupted map would cause incorrect
   * results because insertAtSortPosition checks keyDocumentMap.has(docId)
   * to decide whether to skip insertion.
   */
  var previousResultsMap = new Map(ensureNotFalsy(rxQuery._result).docsDataMap);
  var changed = false;
  var eventReduceEvents = [];
  for (var index = 0; index < rxChangeEvents.length; index++) {
    var cE = rxChangeEvents[index];
    var eventReduceEvent = rxChangeEventToEventReduceChangeEvent(cE);
    if (eventReduceEvent) {
      eventReduceEvents.push(eventReduceEvent);
    }
  }
  var foundNonOptimizeable = eventReduceEvents.find(eventReduceEvent => {
    var stateResolveFunctionInput = {
      queryParams,
      changeEvent: eventReduceEvent,
      previousResults,
      keyDocumentMap: previousResultsMap
    };
    var actionName = calculateActionName(stateResolveFunctionInput);
    if (actionName === 'runFullQueryAgain') {
      return true;
    } else if (actionName !== 'doNothing') {
      changed = true;
      runAction(actionName, queryParams, eventReduceEvent, previousResults, previousResultsMap);
      return false;
    }
  });
  if (foundNonOptimizeable) {
    return {
      runFullQueryAgain: true
    };
  } else {
    return {
      runFullQueryAgain: false,
      changed,
      newResults: previousResults
    };
  }
}
//# sourceMappingURL=event-reduce.js.map