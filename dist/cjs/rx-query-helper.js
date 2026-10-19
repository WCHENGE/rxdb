"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getQueryMatcher = getQueryMatcher;
exports.getSortComparator = getSortComparator;
exports.normalizeMangoQuery = normalizeMangoQuery;
exports.prepareQuery = prepareQuery;
exports.runQueryUpdateFunction = runQueryUpdateFunction;
var _queryPlanner = require("./query-planner.js");
var _rxSchemaHelper = require("./rx-schema-helper.js");
var _index = require("./plugins/utils/index.js");
var _util = require("mingo/util");
var _rxError = require("./rx-error.js");
var _rxQueryMingo = require("./rx-query-mingo.js");
/**
 * Normalize the query to ensure we have all fields set
 * and queries that represent the same query logic are detected as equal by the caching.
 */
function normalizeMangoQuery(schema, mangoQuery,
/**
 * @performance
 * Count queries do not need sort order.
 * Skipping the sort computation avoids iterating over schema indexes
 * and comparing fields, which improves count query performance.
 */
skipSort) {
  var primaryKey = (0, _rxSchemaHelper.getPrimaryFieldOfPrimaryKey)(schema.primaryKey);
  var normalizedMangoQuery = (0, _index.clone)(mangoQuery);
  if (typeof normalizedMangoQuery.skip !== 'number') {
    normalizedMangoQuery.skip = 0;
  }
  if (!normalizedMangoQuery.selector) {
    normalizedMangoQuery.selector = {};
  } else {
    normalizedMangoQuery.selector = normalizedMangoQuery.selector;
    /**
     * In mango query, it is possible to have an
     * equals comparison by directly assigning a value
     * to a property, without the '$eq' operator.
     * Like:
     * selector: {
     *   foo: 'bar'
     * }
     * For normalization, we have to normalize this
     * so our checks can perform properly.
     *
     * This works recursively for nested queries that
     * contain multiple selectors via $and, $or, $nor or $not.
     */
    normalizeQuerySelectorShorthands(normalizedMangoQuery.selector);
  }

  /**
   * Ensure that if an index is specified,
   * the primaryKey is inside of it.
   */
  if (normalizedMangoQuery.index) {
    var indexAr = (0, _index.toArray)(normalizedMangoQuery.index);
    if (!indexAr.includes(primaryKey)) {
      indexAr.push(primaryKey);
    }
    normalizedMangoQuery.index = indexAr;
  }

  /**
   * To ensure a deterministic sorting,
   * we have to ensure the primary key is always part
   * of the sort query.
   * Primary sorting is added as last sort parameter,
   * similar to how we add the primary key to indexes that do not have it.
   *
   */
  if (skipSort && !normalizedMangoQuery.sort) {
    /**
     * @performance
     * Count queries do not need sorted results.
     * Use a simple primary key sort to avoid the expensive
     * index-matching sort computation.
     */
    normalizedMangoQuery.sort = [{
      [primaryKey]: 'asc'
    }];
  } else if (!normalizedMangoQuery.sort) {
    /**
     * If no sort is given at all,
     * we can assume that the user does not care about sort order at al.
     *
     * we cannot just use the primary key as sort parameter
     * because it would likely cause the query to run over the primary key index
     * which has a bad performance in most cases.
     */
    if (normalizedMangoQuery.index) {
      normalizedMangoQuery.sort = normalizedMangoQuery.index.map(field => {
        return {
          [field]: 'asc'
        };
      });
    } else {
      /**
       * Find the index that best matches the fields with the logical operators
       */
      if (schema.indexes) {
        var fieldsWithLogicalOperator = new Set();
        Object.entries(normalizedMangoQuery.selector).forEach(([field, matcher]) => {
          var hasLogical;
          if (typeof matcher === 'object' && matcher !== null) {
            hasLogical = !!Object.keys(matcher).find(operator => _queryPlanner.LOGICAL_OPERATORS.has(operator));
          } else {
            hasLogical = true;
          }
          if (hasLogical) {
            fieldsWithLogicalOperator.add(field);
          }
        });
        var currentFieldsAmount = -1;
        var currentBestIndexForSort;
        schema.indexes.forEach(index => {
          var useIndex = (0, _index.isMaybeReadonlyArray)(index) ? index : [index];
          var firstWrongIndex = useIndex.findIndex(indexField => !fieldsWithLogicalOperator.has(indexField));
          var matchingFieldCount = firstWrongIndex === -1 ? useIndex.length : firstWrongIndex;
          if (matchingFieldCount > 0 && matchingFieldCount > currentFieldsAmount) {
            currentFieldsAmount = matchingFieldCount;
            currentBestIndexForSort = useIndex;
          }
        });
        if (currentBestIndexForSort) {
          normalizedMangoQuery.sort = currentBestIndexForSort.map(field => {
            return {
              [field]: 'asc'
            };
          });
        }
      }

      /**
       * If no good index was found as default sort-order,
       * just use the first index of the schema.
       * If no index is in the schema, use the default-index which
       * is created by RxDB ONLY if there is no other index defined.
       */
      if (!normalizedMangoQuery.sort) {
        if (schema.indexes && schema.indexes.length > 0) {
          var firstIndex = schema.indexes[0];
          var useIndex = (0, _index.isMaybeReadonlyArray)(firstIndex) ? firstIndex : [firstIndex];
          normalizedMangoQuery.sort = useIndex.map(field => ({
            [field]: 'asc'
          }));
        } else {
          normalizedMangoQuery.sort = [{
            [primaryKey]: 'asc'
          }];
        }
      }
    }
  } else {
    var isPrimaryInSort = normalizedMangoQuery.sort.find(p => (0, _index.firstPropertyNameOfObject)(p) === primaryKey);
    if (!isPrimaryInSort) {
      normalizedMangoQuery.sort = normalizedMangoQuery.sort.slice(0);
      normalizedMangoQuery.sort.push({
        [primaryKey]: 'asc'
      });
    }
  }
  return normalizedMangoQuery;
}

/**
 * Returns the sort-comparator,
 * which is able to sort documents in the same way
 * a query over the db would do.
 */
function getSortComparator(_schema, query) {
  if (!query.sort) {
    throw (0, _rxError.newRxError)('SNH', {
      query
    });
  }
  var sortParts = [];
  query.sort.forEach(sortBlock => {
    var key = Object.keys(sortBlock)[0];
    var direction = Object.values(sortBlock)[0];
    sortParts.push({
      key,
      direction,
      getValueFn: (0, _index.objectPathMonad)(key)
    });
  });
  var fun = (a, b) => {
    for (var i = 0; i < sortParts.length; ++i) {
      var sortPart = sortParts[i];
      var valueA = sortPart.getValueFn(a);
      var valueB = sortPart.getValueFn(b);
      if (valueA !== valueB) {
        /**
         * @performance
         * Use a fast inline comparison for common types (string, number)
         * instead of the more general mingoSortComparator.
         * Mingo's compare does extra type checks that are unnecessary
         * when both values share the same primitive type.
         */
        var dirMultiplier = sortPart.direction === 'asc' ? 1 : -1;
        var typeA = typeof valueA;
        if (typeA === typeof valueB && (typeA === 'string' || typeA === 'number')) {
          return (valueA < valueB ? -1 : 1) * dirMultiplier;
        }
        var ret = sortPart.direction === 'asc' ? (0, _util.compare)(valueA, valueB) : (0, _util.compare)(valueB, valueA);
        return ret;
      }
    }
  };
  return fun;
}

/**
 * Returns a function
 * that can be used to check if a document
 * matches the query.
 */
function getQueryMatcher(_schema, query) {
  if (!query.sort) {
    throw (0, _rxError.newRxError)('SNH', {
      query
    });
  }
  var mingoQuery = (0, _rxQueryMingo.getMingoQuery)(query.selector);
  var fun = doc => {
    return mingoQuery.test(doc);
  };
  return fun;
}
async function runQueryUpdateFunction(rxQuery, fn) {
  var docs = await rxQuery.exec();
  if (!docs) {
    // only findOne() queries can return null
    return null;
  }
  if (Array.isArray(docs)) {
    return Promise.all(docs.map(doc => fn(doc)));
  } else if (docs instanceof Map) {
    var resultMap = new Map();
    await Promise.all([...docs.entries()].map(async ([key, doc]) => {
      var updatedDoc = await fn(doc);
      resultMap.set(key, updatedDoc);
    }));
    return resultMap;
  } else {
    // via findOne()
    var result = await fn(docs);
    return result;
  }
}

/**
 * Normalizes selector shorthand values recursively.
 * Converts `{field: value}` to `{field: {$eq: value}}`
 * and recurses into $and, $or, $nor arrays and $not objects.
 */
var SELECTOR_ARRAY_OPERATORS = new Set(['$and', '$or', '$nor']);
var SELECTOR_OBJECT_OPERATORS = new Set(['$not']);
function shouldNormalizeElemMatchSelector(elemMatch) {
  var keys = Object.keys(elemMatch);
  if (keys.length === 0) {
    return false;
  }
  /**
   * If at least one key is not an operator,
   * it is a nested selector object and shorthand values
   * should be normalized.
   */
  if (keys.some(key => !key.startsWith('$'))) {
    return true;
  }
  /**
   * If all keys are operators, only recurse for selector operators.
   * This avoids breaking operator payloads like
   * { $regex: 'x', $options: 'i' } or { $eq: 'foo' }.
   */
  return keys.some(key => SELECTOR_ARRAY_OPERATORS.has(key) || SELECTOR_OBJECT_OPERATORS.has(key));
}
function normalizeQuerySelectorShorthands(selector) {
  Object.entries(selector).forEach(([field, matcher]) => {
    if (typeof matcher !== 'object' || matcher === null) {
      selector[field] = {
        $eq: matcher
      };
    } else if (SELECTOR_ARRAY_OPERATORS.has(field) && Array.isArray(matcher)) {
      matcher.forEach(subSelector => normalizeQuerySelectorShorthands(subSelector));
    } else if (field === '$not' && typeof matcher === 'object') {
      normalizeQuerySelectorShorthands(matcher);
    } else if (!field.startsWith('$') && typeof matcher === 'object') {
      /**
       * Recurse into field-level operator objects to normalize
       * sub-selectors like $elemMatch which contain nested selectors.
       */
      var matcherObj = matcher;
      if (matcherObj.$elemMatch && typeof matcherObj.$elemMatch === 'object' && shouldNormalizeElemMatchSelector(matcherObj.$elemMatch)) {
        normalizeQuerySelectorShorthands(matcherObj.$elemMatch);
      }
    }
  });
}

/**
 * @returns a format of the query that can be used with the storage
 * when calling RxStorageInstance().query()
 */
function prepareQuery(schema, mutateableQuery) {
  if (!mutateableQuery.sort) {
    throw (0, _rxError.newRxError)('SNH', {
      query: mutateableQuery
    });
  }

  /**
   * Store the query plan together with the
   * prepared query to save performance.
   */
  var queryPlan = (0, _queryPlanner.getQueryPlan)(schema, mutateableQuery);
  return {
    query: mutateableQuery,
    queryPlan
  };
}
//# sourceMappingURL=rx-query-helper.js.map