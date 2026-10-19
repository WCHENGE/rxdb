"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.changeIndexableStringByOneQuantum = changeIndexableStringByOneQuantum;
exports.getIndexMeta = getIndexMeta;
exports.getIndexStringLength = getIndexStringLength;
exports.getIndexableStringMonad = getIndexableStringMonad;
exports.getNumberIndexString = getNumberIndexString;
exports.getPrimaryKeyFromIndexableString = getPrimaryKeyFromIndexableString;
exports.getStartIndexStringFromLowerBound = getStartIndexStringFromLowerBound;
exports.getStartIndexStringFromUpperBound = getStartIndexStringFromUpperBound;
exports.getStringLengthOfIndexNumber = getStringLengthOfIndexNumber;
var _rxSchemaHelper = require("./rx-schema-helper.js");
var _index = require("./plugins/utils/index.js");
var _queryPlanner = require("./query-planner.js");
var _rxError = require("./rx-error.js");
/**
 * For some RxStorage implementations,
 * we need to use our custom crafted indexes
 * so we can easily iterate over them. And sort plain arrays of document data.
 *
 * We really often have to craft an index string for a given document.
 * Performance of everything in this file is very important
 * which is why the code sometimes looks strange.
 * Run performance tests before and after you touch anything here!
 */

/**
 * Prepare all relevant information
 * outside of the returned function
 * from getIndexableStringMonad()
 * to save performance when the returned
 * function is called many times.
 */

function getIndexMeta(schema, index) {
  var fieldNameProperties = index.map(fieldName => {
    var schemaPart = (0, _rxSchemaHelper.getSchemaByObjectPath)(schema, fieldName);
    if (!schemaPart) {
      throw (0, _rxError.newRxError)('CI1', {
        fieldName
      });
    }
    var type = schemaPart.type;
    var parsedLengths;
    if (type === 'number' || type === 'integer') {
      parsedLengths = getStringLengthOfIndexNumber(schemaPart);
    }
    var getValue = (0, _index.objectPathMonad)(fieldName);
    var maxLength = schemaPart.maxLength ? schemaPart.maxLength : 0;
    var getIndexStringPart;
    if (type === 'string') {
      getIndexStringPart = docData => {
        var fieldValue = getValue(docData);
        if (!fieldValue) {
          fieldValue = '';
        }
        return fieldValue.padEnd(maxLength, ' ');
      };
    } else if (type === 'boolean') {
      getIndexStringPart = docData => {
        var fieldValue = getValue(docData);
        return fieldValue ? '1' : '0';
      };
    } else {
      // number
      /**
       * @performance
       * Inline the number index string generation to avoid
       * function call overhead and redundant boundary checks.
       * Document data in the hot path is assumed to be valid.
       */
      var pLengths = parsedLengths;
      var pMin = pLengths.minimum;
      var pMax = pLengths.maximum;
      var pRoundedMin = pLengths.roundedMinimum;
      var pNonDecimals = pLengths.nonDecimals;
      var pDecimals = pLengths.decimals;
      var pMultiplier = pLengths.multiplier;
      if (pDecimals === 0) {
        getIndexStringPart = docData => {
          var fieldValue = getValue(docData);
          if (typeof fieldValue === 'undefined') {
            fieldValue = 0;
          }
          if (fieldValue < pMin) {
            fieldValue = pMin;
          }
          if (fieldValue > pMax) {
            fieldValue = pMax;
          }
          return (Math.floor(fieldValue) - pRoundedMin).toString().padStart(pNonDecimals, '0');
        };
      } else {
        getIndexStringPart = docData => {
          var fieldValue = getValue(docData);
          if (typeof fieldValue === 'undefined') {
            fieldValue = 0;
          }
          if (fieldValue < pMin) {
            fieldValue = pMin;
          }
          if (fieldValue > pMax) {
            fieldValue = pMax;
          }
          var flooredValue = Math.floor(fieldValue);
          var shifted = Math.min(Math.round((fieldValue - flooredValue) * pMultiplier), pMultiplier - 1);
          var str = (flooredValue - pRoundedMin).toString().padStart(pNonDecimals, '0');
          return str + shifted.toString().padStart(pDecimals, '0');
        };
      }
    }
    var ret = {
      fieldName,
      schemaPart,
      parsedLengths,
      getValue,
      getIndexStringPart
    };
    return ret;
  });
  return fieldNameProperties;
}

/**
 * Crafts an indexable string that can be used
 * to check if a document would be sorted below or above
 * another documents, dependent on the index values.
 * @monad for better performance
 *
 * IMPORTANT: Performance is really important here
 * which is why we code so 'strange'.
 * Always run performance tests when you want to
 * change something in this method.
 */
function getIndexableStringMonad(schema, index) {
  var fieldNameProperties = getIndexMeta(schema, index);
  var fieldNamePropertiesAmount = fieldNameProperties.length;
  var indexPartsFunctions = fieldNameProperties.map(r => r.getIndexStringPart);

  /**
   * @hotPath Performance of this function is very critical!
   * Specialize for common field counts to avoid loop overhead.
   */
  if (fieldNamePropertiesAmount === 1) {
    return indexPartsFunctions[0];
  }
  if (fieldNamePropertiesAmount === 2) {
    var fn0 = indexPartsFunctions[0];
    var fn1 = indexPartsFunctions[1];
    return docData => fn0(docData) + fn1(docData);
  }
  if (fieldNamePropertiesAmount === 3) {
    var _fn = indexPartsFunctions[0];
    var _fn2 = indexPartsFunctions[1];
    var fn2 = indexPartsFunctions[2];
    return docData => _fn(docData) + _fn2(docData) + fn2(docData);
  }
  var ret = function (docData) {
    var str = '';
    for (var i = 0; i < fieldNamePropertiesAmount; ++i) {
      str += indexPartsFunctions[i](docData);
    }
    return str;
  };
  return ret;
}
function getStringLengthOfIndexNumber(schemaPart) {
  var minimum = Math.floor(schemaPart.minimum);
  var maximum = Math.ceil(schemaPart.maximum);
  var multipleOf = schemaPart.multipleOf;
  var valueSpan = maximum - minimum;
  var nonDecimals = valueSpan.toString().length;
  var multipleOfParts = multipleOf.toString().split('.');
  var decimals = 0;
  if (multipleOfParts.length > 1) {
    decimals = multipleOfParts[1].length;
  }
  return {
    minimum,
    maximum,
    nonDecimals,
    decimals,
    roundedMinimum: minimum,
    multiplier: Math.pow(10, decimals)
  };
}
function getIndexStringLength(schema, index) {
  var fieldNameProperties = getIndexMeta(schema, index);
  var length = 0;
  fieldNameProperties.forEach(props => {
    var schemaPart = props.schemaPart;
    var type = schemaPart.type;
    if (type === 'string') {
      length += schemaPart.maxLength;
    } else if (type === 'boolean') {
      length += 1;
    } else {
      var parsedLengths = props.parsedLengths;
      length = length + parsedLengths.nonDecimals + parsedLengths.decimals;
    }
  });
  return length;
}
function getPrimaryKeyFromIndexableString(indexableString, primaryKeyLength) {
  var paddedPrimaryKey = indexableString.slice(primaryKeyLength * -1);
  // we can safely trim here because the primary key is not allowed to start or end with a space char.
  var primaryKey = paddedPrimaryKey.trim();
  return primaryKey;
}
function getNumberIndexString(parsedLengths, fieldValue) {
  /**
   * Ensure that the given value is in the boundaries
   * of the schema, otherwise it would create a broken index string.
   * This can happen for example if you have a minimum of 0
   * and run a query like
   * selector {
   *  numField: { $gt: -1000 }
   * }
   */
  if (typeof fieldValue === 'undefined') {
    fieldValue = 0;
  }
  if (fieldValue < parsedLengths.minimum) {
    fieldValue = parsedLengths.minimum;
  }
  if (fieldValue > parsedLengths.maximum) {
    fieldValue = parsedLengths.maximum;
  }
  var nonDecimalsValueAsString = (Math.floor(fieldValue) - parsedLengths.roundedMinimum).toString();
  var str = nonDecimalsValueAsString.padStart(parsedLengths.nonDecimals, '0');
  if (parsedLengths.decimals > 0) {
    /**
     * @performance
     * Use math to extract decimal digits instead of toString().split('.')
     * which creates intermediate strings and arrays.
     * multiplier is pre-computed in ParsedLengths to avoid Math.pow() per call.
     */
    var multiplier = parsedLengths.multiplier;
    var shifted = Math.min(Math.round((fieldValue - Math.floor(fieldValue)) * multiplier), multiplier - 1);
    var decimalPart = shifted.toString();
    str += decimalPart.padStart(parsedLengths.decimals, '0');
  }
  return str;
}
function getStartIndexStringFromLowerBound(schema, index, lowerBound) {
  var str = '';
  index.forEach((fieldName, idx) => {
    var schemaPart = (0, _rxSchemaHelper.getSchemaByObjectPath)(schema, fieldName);
    var bound = lowerBound[idx];
    var type = schemaPart.type;
    switch (type) {
      case 'string':
        var maxLength = (0, _index.ensureNotFalsy)(schemaPart.maxLength, 'maxLength not set');
        if (typeof bound === 'string') {
          if (bound === _queryPlanner.INDEX_MAX) {
            str += ''.padEnd(maxLength, _queryPlanner.INDEX_MAX);
          } else {
            str += bound.padEnd(maxLength, ' ');
          }
        } else {
          /**
           * Use the null character (\x00) as the absolute minimum
           * instead of space (\x20). Strings can contain characters
           * with codepoints below space (e.g. \t = 9, \n = 10)
           * which would sort before space-padded bounds and be
           * silently excluded from query results.
           */
          str += ''.padEnd(maxLength, '\x00');
        }
        break;
      case 'boolean':
        if (bound === null) {
          str += '0';
        } else if (bound === _queryPlanner.INDEX_MIN) {
          str += '0';
        } else if (bound === _queryPlanner.INDEX_MAX) {
          str += '1';
        } else {
          var boolToStr = bound ? '1' : '0';
          str += boolToStr;
        }
        break;
      case 'number':
      case 'integer':
        var parsedLengths = getStringLengthOfIndexNumber(schemaPart);
        if (bound === null || bound === _queryPlanner.INDEX_MIN) {
          var fillChar = '0';
          str += fillChar.repeat(parsedLengths.nonDecimals + parsedLengths.decimals);
        } else if (bound === _queryPlanner.INDEX_MAX) {
          str += getNumberIndexString(parsedLengths, parsedLengths.maximum);
        } else {
          var add = getNumberIndexString(parsedLengths, bound);
          str += add;
        }
        break;
      default:
        throw (0, _rxError.newRxError)('CI2', {
          type: type
        });
    }
  });
  return str;
}
function getStartIndexStringFromUpperBound(schema, index, upperBound) {
  var str = '';
  index.forEach((fieldName, idx) => {
    var schemaPart = (0, _rxSchemaHelper.getSchemaByObjectPath)(schema, fieldName);
    var bound = upperBound[idx];
    var type = schemaPart.type;
    switch (type) {
      case 'string':
        var maxLength = (0, _index.ensureNotFalsy)(schemaPart.maxLength, 'maxLength not set');
        if (typeof bound === 'string' && bound !== _queryPlanner.INDEX_MAX) {
          str += bound.padEnd(maxLength, ' ');
        } else if (bound === _queryPlanner.INDEX_MIN) {
          /**
           * Use the null character (\x00) as the absolute minimum
           * to match the lower bound fix. This ensures that compound
           * index queries with exclusive bounds on earlier fields
           * correctly exclude documents whose later string fields
           * contain characters with codepoints below space.
           */
          str += ''.padEnd(maxLength, '\x00');
        } else {
          str += ''.padEnd(maxLength, _queryPlanner.INDEX_MAX);
        }
        break;
      case 'boolean':
        if (bound === null || bound === _queryPlanner.INDEX_MAX) {
          str += '1';
        } else if (bound === _queryPlanner.INDEX_MIN) {
          str += '0';
        } else {
          var boolToStr = bound ? '1' : '0';
          str += boolToStr;
        }
        break;
      case 'number':
      case 'integer':
        var parsedLengths = getStringLengthOfIndexNumber(schemaPart);
        if (bound === null || bound === _queryPlanner.INDEX_MAX) {
          var fillChar = '9';
          str += fillChar.repeat(parsedLengths.nonDecimals + parsedLengths.decimals);
        } else if (bound === _queryPlanner.INDEX_MIN) {
          var _fillChar = '0';
          str += _fillChar.repeat(parsedLengths.nonDecimals + parsedLengths.decimals);
        } else {
          str += getNumberIndexString(parsedLengths, bound);
        }
        break;
      default:
        throw (0, _rxError.newRxError)('CI2', {
          type: type
        });
    }
  });
  return str;
}

/**
 * Used in storages where it is not possible
 * to define inclusiveEnd/inclusiveStart
 */
function changeIndexableStringByOneQuantum(str, direction) {
  var lastChar = str.slice(-1);
  var charCode = lastChar.charCodeAt(0);
  charCode = charCode + direction;
  var withoutLastChar = str.slice(0, -1);
  return withoutLastChar + String.fromCharCode(charCode);
}
//# sourceMappingURL=custom-index.js.map