"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.UPPER_BOUND_LOGICAL_OPERATORS = exports.LOWER_BOUND_LOGICAL_OPERATORS = exports.LOGICAL_OPERATORS = exports.INDEX_MIN = exports.INDEX_MAX = void 0;
exports.getInQueryRangeOpts = getInQueryRangeOpts;
exports.getMatcherQueryOpts = getMatcherQueryOpts;
exports.getQueryPlan = getQueryPlan;
exports.isSelectorSatisfiedByIndex = isSelectorSatisfiedByIndex;
exports.rateQueryPlan = rateQueryPlan;
var _index = require("./plugins/utils/index.js");
var _rxError = require("./rx-error.js");
var _rxSchemaHelper = require("./rx-schema-helper.js");
var INDEX_MAX = exports.INDEX_MAX = String.fromCharCode(65535);

/**
 * Do not use -Infinity here because it would be
 * transformed to null on JSON.stringify() which can break things
 * when the query plan is sent to the storage as json.
 * @link https://stackoverflow.com/a/16644751
 * Notice that for IndexedDB IDBKeyRange we have
 * to transform the value back to -Infinity
 * before we can use it in IDBKeyRange.bound.
 */
var INDEX_MIN = exports.INDEX_MIN = Number.MIN_SAFE_INTEGER;

/**
 * Returns the query plan which contains
 * information about how to run the query
 * and which indexes to use.
 *
 * This is used in some storage like Memory, dexie.js and IndexedDB.
 */
function getQueryPlan(schema, query) {
  var selector = query.selector;
  var indexes = schema.indexes ? schema.indexes.slice(0) : [];
  if (query.index) {
    indexes = [query.index];
  }

  /**
   * Most storages do not support descending indexes
   * so having a 'desc' in the sorting, means we always have to re-sort the results.
   */
  var hasDescSorting = !!query.sort.find(sortField => Object.values(sortField)[0] === 'desc');

  /**
   * Some fields can be part of the selector while not being relevant for sorting
   * because their selector operators specify that in all cases all matching docs
   * would have the same value.
   * For example the boolean field _deleted or enum fields.
   * Same goes for string and number fields that are matched with $eq
   * to a string or number value.
   * @performance Not having these fields in the compare strings
   * makes it possible to use the index order as sort order
   * so that the storage does not have to re-sort the results.
   */
  var sortIrrelevevantFields = new Set();
  Object.keys(selector).forEach(fieldName => {
    var schemaPart = (0, _rxSchemaHelper.getSchemaByObjectPath)(schema, fieldName);
    var fieldSelector = selector[fieldName];
    if (!schemaPart || !Object.prototype.hasOwnProperty.call(fieldSelector, '$eq')) {
      return;
    }
    if (schemaPart.type === 'boolean' || schemaPart.enum) {
      sortIrrelevevantFields.add(fieldName);
    } else if (schemaPart.type === 'string' || schemaPart.type === 'number' || schemaPart.type === 'integer') {
      var eqValueType = typeof fieldSelector.$eq;
      if (eqValueType === 'string' || eqValueType === 'number') {
        sortIrrelevevantFields.add(fieldName);
      }
    }
  });
  var optimalSortIndex = query.sort.map(sortField => Object.keys(sortField)[0]);
  var optimalSortIndexCompareString = optimalSortIndex.filter(f => !sortIrrelevevantFields.has(f)).join(',');
  var currentBestQuality = -1;
  var currentBestQueryPlan;

  /**
   * Calculate one query plan for each index
   * and then test which of the plans is best.
   */
  indexes.forEach(index => {
    var inclusiveEnd = true;
    var inclusiveStart = true;
    var opts = index.map(indexField => {
      var matcher = selector[indexField];
      var operators = matcher ? Object.keys(matcher) : [];
      var matcherOpts = {};
      if (!matcher || !operators.length) {
        var startKey = inclusiveStart ? INDEX_MIN : INDEX_MAX;
        matcherOpts = {
          startKey,
          endKey: inclusiveEnd ? INDEX_MAX : INDEX_MIN,
          inclusiveStart: true,
          inclusiveEnd: true
        };
      } else {
        operators.forEach(operator => {
          if (LOGICAL_OPERATORS.has(operator)) {
            var operatorValue = matcher[operator];
            var partialOpts = getMatcherQueryOpts(operator, operatorValue);
            matcherOpts = Object.assign(matcherOpts, partialOpts);
          } else if (operator === '$in') {
            var _partialOpts = getInQueryRangeOpts(matcher[operator]);
            if (_partialOpts) {
              matcherOpts = Object.assign(matcherOpts, _partialOpts);
            }
          }
        });
      }

      // fill missing attributes
      if (typeof matcherOpts.startKey === 'undefined') {
        matcherOpts.startKey = INDEX_MIN;
      }
      if (typeof matcherOpts.endKey === 'undefined') {
        matcherOpts.endKey = INDEX_MAX;
      }
      if (typeof matcherOpts.inclusiveStart === 'undefined') {
        matcherOpts.inclusiveStart = true;
      }
      if (typeof matcherOpts.inclusiveEnd === 'undefined') {
        matcherOpts.inclusiveEnd = true;
      }

      /**
       * When a numeric bound falls outside the schema minimum/maximum,
       * getNumberIndexString() clamps it to the boundary value.
       * An exclusive bound clamped to the same string as a boundary
       * document would incorrectly exclude that document, so we
       * promote it to inclusive here.
       */
      var schemaPart = (0, _rxSchemaHelper.getSchemaByObjectPath)(schema, indexField);
      if (schemaPart && (schemaPart.type === 'number' || schemaPart.type === 'integer')) {
        if (!matcherOpts.inclusiveStart && typeof matcherOpts.startKey === 'number' && typeof schemaPart.minimum === 'number' && matcherOpts.startKey < schemaPart.minimum) {
          matcherOpts.inclusiveStart = true;
        }
        if (!matcherOpts.inclusiveEnd && typeof matcherOpts.endKey === 'number' && typeof schemaPart.maximum === 'number' && matcherOpts.endKey > schemaPart.maximum) {
          matcherOpts.inclusiveEnd = true;
        }
      }
      if (inclusiveStart && !matcherOpts.inclusiveStart) {
        inclusiveStart = false;
      }
      if (inclusiveEnd && !matcherOpts.inclusiveEnd) {
        inclusiveEnd = false;
      }
      return matcherOpts;
    });
    var startKeys = opts.map(opt => opt.startKey);
    var endKeys = opts.map(opt => opt.endKey);

    /**
     * Compute the index compare string once per index,
     * not inside the queryPlan object literal, to avoid
     * creating a filtered array and joining on every iteration.
     */
    var indexCompareString;
    if (sortIrrelevevantFields.size === 0) {
      indexCompareString = index.join(',');
    } else {
      indexCompareString = index.filter(f => !sortIrrelevevantFields.has(f)).join(',');
    }
    var queryPlan = {
      index,
      startKeys,
      endKeys,
      inclusiveEnd,
      inclusiveStart,
      sortSatisfiedByIndex: !hasDescSorting && optimalSortIndexCompareString === indexCompareString,
      selectorSatisfiedByIndex: isSelectorSatisfiedByIndex(index, query.selector, startKeys, endKeys)
    };
    var quality = rateQueryPlan(schema, query, queryPlan);
    if (quality >= currentBestQuality || query.index) {
      currentBestQuality = quality;
      currentBestQueryPlan = queryPlan;
    }
  });

  /**
   * In all cases and index must be found
   */
  if (!currentBestQueryPlan) {
    throw (0, _rxError.newRxError)('SNH', {
      query
    });
  }
  return currentBestQueryPlan;
}
var LOGICAL_OPERATORS = exports.LOGICAL_OPERATORS = new Set(['$eq', '$gt', '$gte', '$lt', '$lte']);
var LOWER_BOUND_LOGICAL_OPERATORS = exports.LOWER_BOUND_LOGICAL_OPERATORS = new Set(['$eq', '$gt', '$gte']);
var UPPER_BOUND_LOGICAL_OPERATORS = exports.UPPER_BOUND_LOGICAL_OPERATORS = new Set(['$eq', '$lt', '$lte']);
function isSelectorSatisfiedByIndex(index, selector, startKeys, endKeys) {
  /**
   * Not satisfied if contains $and or $or operations.
   */
  if (selector.$and || selector.$or) {
    return false;
  }

  /**
   * Check all selector entries in a single pass:
   * - Ensure all fields are in the index
   * - Ensure all operators are logical
   * - Track lower/upper bound operators
   */
  var selectorEntries = Object.entries(selector);
  var lowerOperatorFieldNames = new Set();
  var upperOperatorFieldNames = new Set();
  var hasNonEqLowerBound = false;
  var hasNonEqUpperBound = false;
  for (var [fieldName, operation] of selectorEntries) {
    if (!index.includes(fieldName)) {
      return false;
    }
    var operationKeys = Object.keys(operation);
    var lowerLogicOpCount = 0;
    var lastLowerLogicOp = void 0;
    var upperLogicOpCount = 0;
    var lastUpperLogicOp = void 0;
    for (var op of operationKeys) {
      if (!LOGICAL_OPERATORS.has(op)) {
        return false;
      }
      if (LOWER_BOUND_LOGICAL_OPERATORS.has(op)) {
        lowerLogicOpCount++;
        lastLowerLogicOp = op;
      }
      if (UPPER_BOUND_LOGICAL_OPERATORS.has(op)) {
        upperLogicOpCount++;
        lastUpperLogicOp = op;
      }
    }

    // If more than one logic op on the same field per bound direction, we have to selector-match.
    if (lowerLogicOpCount > 1 || upperLogicOpCount > 1) {
      return false;
    }
    if (lastLowerLogicOp) {
      lowerOperatorFieldNames.add(fieldName);
    }
    if (lastLowerLogicOp !== '$eq') {
      if (hasNonEqLowerBound) {
        return false;
      }
      hasNonEqLowerBound = true;
    }
    if (lastUpperLogicOp) {
      upperOperatorFieldNames.add(fieldName);
    }
    if (lastUpperLogicOp !== '$eq') {
      if (hasNonEqUpperBound) {
        return false;
      }
      hasNonEqUpperBound = true;
    }
  }

  /**
   * If the index contains a non-relevant field between
   * the relevant fields, then the index is not satisfying.
   */
  var i = 0;
  for (var _fieldName of index) {
    for (var set of [lowerOperatorFieldNames, upperOperatorFieldNames]) {
      if (!set.has(_fieldName) && set.size > 0) {
        return false;
      }
      set.delete(_fieldName);
    }
    var startKey = startKeys[i];
    var endKey = endKeys[i];
    if (startKey !== endKey && lowerOperatorFieldNames.size > 0 && upperOperatorFieldNames.size > 0) {
      return false;
    }
    i++;
  }
  return true;
}

/**
 * $in can use an index by scanning the range between the
 * smallest and the largest of the given values.
 * That range can contain non-matching documents, so the
 * selector is never satisfied by the index alone and the
 * query matcher must still filter the results.
 * Only homogeneous arrays of strings or finite numbers are
 * used because min/max is not defined across mixed types.
 * @link https://github.com/pubkey/rxdb/issues/8631
 */
function getInQueryRangeOpts(operatorValue) {
  if (!Array.isArray(operatorValue) || operatorValue.length === 0) {
    return undefined;
  }
  var valueType = typeof operatorValue[0];
  if (valueType !== 'string' && valueType !== 'number') {
    return undefined;
  }
  var min = operatorValue[0];
  var max = operatorValue[0];
  for (var i = 0; i < operatorValue.length; i++) {
    var value = operatorValue[i];
    if (typeof value !== valueType || valueType === 'number' && !isFinite(value)) {
      return undefined;
    }
    if (value < min) {
      min = value;
    }
    if (value > max) {
      max = value;
    }
  }
  return {
    startKey: min,
    endKey: max,
    inclusiveStart: true,
    inclusiveEnd: true
  };
}
function getMatcherQueryOpts(operator, operatorValue) {
  switch (operator) {
    case '$eq':
      return {
        startKey: operatorValue,
        endKey: operatorValue,
        inclusiveEnd: true,
        inclusiveStart: true
      };
    case '$lte':
      return {
        endKey: operatorValue,
        inclusiveEnd: true
      };
    case '$gte':
      return {
        startKey: operatorValue,
        inclusiveStart: true
      };
    case '$lt':
      return {
        endKey: operatorValue,
        inclusiveEnd: false
      };
    case '$gt':
      return {
        startKey: operatorValue,
        inclusiveStart: false
      };
    default:
      throw (0, _rxError.newRxError)('SNH');
  }
}

/**
 * Returns a number that determines the quality of the query plan.
 * Higher number means better query plan.
 */
function rateQueryPlan(schema, query, queryPlan) {
  var quality = 0;
  var addQuality = value => {
    if (value > 0) {
      quality = quality + value;
    }
  };
  var pointsPerMatchingKey = 10;
  var nonMinKeyCount = (0, _index.countUntilNotMatching)(queryPlan.startKeys, keyValue => keyValue !== INDEX_MIN && keyValue !== INDEX_MAX);
  addQuality(nonMinKeyCount * pointsPerMatchingKey);
  var nonMaxKeyCount = (0, _index.countUntilNotMatching)(queryPlan.endKeys, keyValue => keyValue !== INDEX_MAX && keyValue !== INDEX_MIN);
  addQuality(nonMaxKeyCount * pointsPerMatchingKey);
  var equalKeyCount = (0, _index.countUntilNotMatching)(queryPlan.startKeys, (keyValue, idx) => {
    if (keyValue === queryPlan.endKeys[idx]) {
      return true;
    } else {
      return false;
    }
  });
  addQuality(equalKeyCount * pointsPerMatchingKey * 1.5);
  var pointsIfNoReSortMustBeDone = queryPlan.sortSatisfiedByIndex ? 5 : 0;
  addQuality(pointsIfNoReSortMustBeDone);
  return quality;
}
//# sourceMappingURL=query-planner.js.map