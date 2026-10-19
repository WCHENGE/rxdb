"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.attachmentMapKey = attachmentMapKey;
exports.bulkInsertToState = bulkInsertToState;
exports.compareDocsWithIndex = compareDocsWithIndex;
exports.ensureNotRemoved = ensureNotRemoved;
exports.getMemoryCollectionKey = getMemoryCollectionKey;
exports.putWriteRowToState = putWriteRowToState;
exports.removeDocFromState = removeDocFromState;
var _arrayPushAtSortPosition = require("array-push-at-sort-position");
var _rxError = require("../../rx-error.js");
var _binarySearchBounds = require("./binary-search-bounds.js");
function getMemoryCollectionKey(databaseName, collectionName, schemaVersion) {
  return [databaseName, collectionName, schemaVersion].join('--memory--');
}
function ensureNotRemoved(instance) {
  if (instance.internals.removed) {
    throw new Error('removed already ' + instance.databaseName + ' - ' + instance.collectionName + ' - ' + instance.schema.version);
  }
}
function attachmentMapKey(documentId, attachmentId) {
  return documentId + '||' + attachmentId;
}

/**
 * @performance
 * Threshold for using in-place splice vs. full merge-sort when inserting
 * documents into indexes. Below this batch size, in-place binary search + splice
 * is faster because it avoids allocating a new full-size array and copying all elements.
 */
var IN_PLACE_INSERT_THRESHOLD = 64;
function sortByIndexStringComparator(a, b) {
  if (a[0] < b[0]) {
    return -1;
  } else {
    return 1;
  }
}

/**
 * @hotPath
 */
function putWriteRowToState(docId, state, stateByIndex, document, docInState) {
  state.documents.set(docId, document);
  var stateByIndexLength = stateByIndex.length;
  for (var i = 0; i < stateByIndexLength; ++i) {
    var byIndex = stateByIndex[i];
    var docsWithIndex = byIndex.docsWithIndex;
    var getIndexableString = byIndex.getIndexableString;
    var newIndexString = getIndexableString(document);

    /**
     * @performance
     * When updating a document, first compute whether the index changed.
     * If it did not change, we only need to update the document reference
     * in-place without any splice operations.
     */
    if (docInState) {
      var previousIndexString = getIndexableString(docInState);
      if (previousIndexString === newIndexString) {
        /**
         * Performance shortcut.
         * Index did not change, so the old entry is at the same position.
         * We can find it by string-specialized binary search and update in-place.
         */
        var eqPos = (0, _binarySearchBounds.boundEQByIndexString)(docsWithIndex, previousIndexString);
        if (eqPos !== -1) {
          /**
           * There might be multiple entries with the same index string
           * (e.g. different documents). Search around eqPos for ours.
           */
          if (docsWithIndex[eqPos][2] === docId) {
            docsWithIndex[eqPos][1] = document;
            continue;
          }
          // Check neighbors
          var prev = docsWithIndex[eqPos - 1];
          if (prev && prev[0] === previousIndexString && prev[2] === docId) {
            docsWithIndex[eqPos - 1][1] = document;
            continue;
          }
          var next = docsWithIndex[eqPos + 1];
          if (next && next[0] === previousIndexString && next[2] === docId) {
            docsWithIndex[eqPos + 1][1] = document;
            continue;
          }
        }
        // Fallback: use the old insert+remove approach
        var insertPosition = (0, _arrayPushAtSortPosition.pushAtSortPosition)(docsWithIndex, [newIndexString, document, docId], sortByIndexStringComparator, 0);
        var prevEntry = docsWithIndex[insertPosition - 1];
        if (prevEntry && prevEntry[2] === docId) {
          docsWithIndex.splice(insertPosition - 1, 1);
        } else {
          var nextEntry = docsWithIndex[insertPosition + 1];
          if (nextEntry[2] === docId) {
            docsWithIndex.splice(insertPosition + 1, 1);
          } else {
            throw (0, _rxError.newRxError)('SNH', {
              document,
              args: {
                byIndex
              }
            });
          }
        }
        continue;
      } else {
        /**
         * Index changed, we must remove the old entry and insert the new one.
         */
        var indexBefore = (0, _binarySearchBounds.boundEQByIndexString)(docsWithIndex, previousIndexString);
        if (indexBefore !== -1) {
          docsWithIndex.splice(indexBefore, 1);
        }
      }
    }
    (0, _arrayPushAtSortPosition.pushAtSortPosition)(docsWithIndex, [newIndexString, document, docId], sortByIndexStringComparator, 0);
  }
}

/**
 * @hotPath
 * Efficiently inserts multiple documents into the state at once.
 *
 * Uses two strategies based on batch size:
 * - For small batches (relative to existing index size), uses in-place
 *   binary search + splice per document. This avoids allocating a new
 *   full-size array and copying all elements, reducing GC pressure.
 * - For large batches (or empty indexes), pre-computes all index entries,
 *   sorts them, and merges into the existing sorted arrays in a single pass.
 */
function bulkInsertToState(primaryPath, state, stateByIndex, docs) {
  var docsLength = docs.length;
  var stateByIndexLength = stateByIndex.length;

  // Extract documents and docIds once, store in Map
  var documents = new Array(docsLength);
  var docIds = new Array(docsLength);
  for (var i = 0; i < docsLength; ++i) {
    var doc = docs[i].document;
    var docId = doc[primaryPath];
    documents[i] = doc;
    docIds[i] = docId;
    state.documents.set(docId, doc);
  }

  /**
   * @performance
   * For small batch sizes, use in-place binary search + splice
   * instead of creating a full merged array copy. This is faster
   * for serial inserts and small bulk inserts because it avoids:
   * - Allocating a new array of size n+m
   * - Copying all n existing elements
   * - GC pressure from discarding the old array
   *
   * The threshold is based on when the merge approach becomes more
   * efficient than individual splices.
   */
  var useInPlaceInsert = docsLength < IN_PLACE_INSERT_THRESHOLD;
  if (useInPlaceInsert) {
    for (var indexI = 0; indexI < stateByIndexLength; ++indexI) {
      var byIndex = stateByIndex[indexI];
      var docsWithIndex = byIndex.docsWithIndex;
      var getIndexableString = byIndex.getIndexableString;
      if (docsWithIndex.length === 0) {
        for (var _i = 0; _i < docsLength; ++_i) {
          var _doc = documents[_i];
          var indexString = getIndexableString(_doc);
          docsWithIndex.push([indexString, _doc, docIds[_i]]);
        }
        docsWithIndex.sort(sortByIndexStringComparator);
      } else {
        for (var _i2 = 0; _i2 < docsLength; ++_i2) {
          var _doc2 = documents[_i2];
          var _indexString = getIndexableString(_doc2);
          var newEntry = [_indexString, _doc2, docIds[_i2]];
          (0, _arrayPushAtSortPosition.pushAtSortPosition)(docsWithIndex, newEntry, sortByIndexStringComparator, 0);
        }
      }
    }
  } else {
    // For each index, batch-compute entries, sort, and merge
    for (var _indexI = 0; _indexI < stateByIndexLength; ++_indexI) {
      var _byIndex = stateByIndex[_indexI];
      var _docsWithIndex = _byIndex.docsWithIndex;
      var _getIndexableString = _byIndex.getIndexableString;

      // Build new entries
      var newEntries = new Array(docsLength);
      for (var _i3 = 0; _i3 < docsLength; ++_i3) {
        var _doc3 = documents[_i3];
        newEntries[_i3] = [_getIndexableString(_doc3), _doc3, docIds[_i3]];
      }

      // Sort by index string
      newEntries.sort(sortByIndexStringComparator);
      if (_docsWithIndex.length === 0) {
        // Index is empty, just assign sorted entries
        _byIndex.docsWithIndex = newEntries;
      } else {
        // Merge sorted arrays
        _byIndex.docsWithIndex = mergeSortedArrays(_docsWithIndex, newEntries);
      }
    }
  }
}

/**
 * Merges two sorted DocWithIndexString arrays into a single sorted array.
 * Runs in O(n + m) where n and m are the lengths of the input arrays.
 * @performance Comparator is inlined to avoid function call overhead
 * per comparison, which is significant for large arrays.
 */
function mergeSortedArrays(a, b) {
  var aLen = a.length;
  var bLen = b.length;
  var result = new Array(aLen + bLen);
  var ai = 0;
  var bi = 0;
  var ri = 0;
  while (ai < aLen && bi < bLen) {
    if (a[ai][0] <= b[bi][0]) {
      result[ri++] = a[ai++];
    } else {
      result[ri++] = b[bi++];
    }
  }
  while (ai < aLen) {
    result[ri++] = a[ai++];
  }
  while (bi < bLen) {
    result[ri++] = b[bi++];
  }
  return result;
}
function removeDocFromState(primaryPath, schema, state, doc) {
  var docId = doc[primaryPath];
  state.documents.delete(docId);
  var stateByIndex = state.byIndexArray;
  for (var i = 0; i < stateByIndex.length; ++i) {
    var byIndex = stateByIndex[i];
    var docsWithIndex = byIndex.docsWithIndex;
    var indexString = byIndex.getIndexableString(doc);
    var positionInIndex = (0, _binarySearchBounds.boundEQByIndexString)(docsWithIndex, indexString);
    if (positionInIndex !== -1) {
      docsWithIndex.splice(positionInIndex, 1);
    }
  }
}
function compareDocsWithIndex(a, b) {
  var indexStringA = a[0];
  var indexStringB = b[0];
  if (indexStringA < indexStringB) {
    return -1;
  } else if (indexStringA === indexStringB) {
    return 0;
  } else {
    return 1;
  }
}
//# sourceMappingURL=memory-helper.js.map