"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.checkWriteRows = checkWriteRows;
exports.containsDateInstance = containsDateInstance;
exports.ensurePrimaryKeyValid = ensurePrimaryKeyValid;
var _rxError = require("../../rx-error.js");
var _rxSchemaHelper = require("../../rx-schema-helper.js");
/**
 * Strip Blob data from _attachments so that
 * the structured-clone / JSON check does not fail on Blob objects.
 */
function stripBlobsFromAttachments(attachments) {
  if (!attachments) {
    return {};
  }
  var ret = {};
  for (var [key, val] of Object.entries(attachments)) {
    var clone = Object.assign({}, val);
    delete clone.data;
    ret[key] = clone;
  }
  return ret;
}
function ensurePrimaryKeyValid(primaryKey, docData) {
  if (!primaryKey) {
    throw (0, _rxError.newRxError)('DOC20', {
      primaryKey,
      document: docData
    });
  }

  /**
   * This is required so that we can left-pad
   * the primaryKey and we are still able to de-left-pad
   * it to get again the original key.
   */
  if (primaryKey !== primaryKey.trim()) {
    throw (0, _rxError.newRxError)('DOC21', {
      primaryKey,
      document: docData
    });
  }
  if (primaryKey.includes('\r') || primaryKey.includes('\n')) {
    throw (0, _rxError.newRxError)('DOC22', {
      primaryKey,
      document: docData
    });
  }
  if (primaryKey.includes('"')) {
    throw (0, _rxError.newRxError)('DOC23', {
      primaryKey,
      document: docData
    });
  }
}

/**
 * Deeply checks if the object contains an
 * instance of the JavaScript Date class.
 * @recursive
 */
function containsDateInstance(obj) {
  if (typeof obj !== 'object' || obj === null) {
    return false;
  }
  for (var key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      if (obj[key] instanceof Date) {
        return true;
      }
      if (typeof obj[key] === 'object' && containsDateInstance(obj[key])) {
        return true;
      }
    }
  }
  return false;
}
function checkWriteRows(storageInstance, rows) {
  var primaryPath = (0, _rxSchemaHelper.getPrimaryFieldOfPrimaryKey)(storageInstance.schema.primaryKey);
  var _loop = function (writeRow) {
    // ensure that the primary key has not been changed
    writeRow.document = (0, _rxSchemaHelper.fillPrimaryKey)(primaryPath, storageInstance.schema, writeRow.document);

    /**
     * Ensure that _meta fields have been merged
     * and not replaced.
     * This is important so that when one plugin A
     * sets a _meta field and another plugin B does a write
     * to the document, it must be ensured that the
     * field of plugin A was not removed.
     */
    if (writeRow.previous) {
      Object.keys(writeRow.previous._meta).forEach(metaFieldName => {
        if (!Object.prototype.hasOwnProperty.call(writeRow.document._meta, metaFieldName)) {
          throw (0, _rxError.newRxError)('SNH', {
            dataBefore: writeRow.previous,
            dataAfter: writeRow.document,
            args: {
              metaFieldName
            }
          });
        }
      });
    }

    /**
     * Ensure it can be structured cloned.
     */
    try {
      /**
       * Notice that structuredClone() is not available
       * in ReactNative, so we test for JSON.stringify() instead
       * @link https://github.com/pubkey/rxdb/issues/5046#issuecomment-1827374498
       */
      if (typeof structuredClone === 'function') {
        structuredClone(writeRow);
      } else {
        /**
         * JSON.stringify cannot handle Blob objects,
         * so we must strip them from _attachments before checking.
         */
        var checkRow = Object.assign({}, writeRow, {
          document: Object.assign({}, writeRow.document, {
            _attachments: stripBlobsFromAttachments(writeRow.document._attachments)
          })
        });
        if (writeRow.previous) {
          checkRow.previous = Object.assign({}, writeRow.previous, {
            _attachments: stripBlobsFromAttachments(writeRow.previous._attachments)
          });
        }
        JSON.parse(JSON.stringify(checkRow));
      }
    } catch (err) {
      throw (0, _rxError.newRxError)('DOC24', {
        collection: storageInstance.collectionName,
        document: writeRow.document
      });
    }

    /**
     * Ensure it does not contain a Date() object
     */
    if (containsDateInstance(writeRow.document)) {
      throw (0, _rxError.newRxError)('DOC24', {
        collection: storageInstance.collectionName,
        document: writeRow.document
      });
    }
  };
  for (var writeRow of rows) {
    _loop(writeRow);
  }
}
//# sourceMappingURL=check-document.js.map