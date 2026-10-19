import { mapDocumentsDataToCacheDocs } from "./doc-cache.js";
import { overwriteGetterForCaching } from "./plugins/utils/index.js";
import { newRxError } from "./rx-error.js";
/**
 * Counter to create unique result IDs cheaply.
 * Used instead of now() to avoid expensive timestamp computation
 * when we only need uniqueness for change detection.
 */
var _resultIdCounter = 0;

/**
 * RxDB needs the query results in multiple formats.
 * Sometimes as a Map or an array with only the documentData.
 * For better performance we work with this class
 * that initializes stuff lazily so that
 * we can directly work with the query results after RxQuery.exec()
 */
export class RxQuerySingleResult {
  /**
   * Unique identifier for this result state.
   * Used to determine if the result set has changed since X
   * so that we do not emit the same result multiple times on subscription.
   */
  time = ++_resultIdCounter;
  documents;
  constructor(query,
  // only used internally, do not use outside, use this.docsData instead
  docsDataFromStorageInstance,
  // can be overwritten for count-queries
  count) {
    this.query = query;
    this.count = count;
    /**
     * @performance Skip document mapping for count queries
     * since they only need the count number and pass an empty array.
     */
    if (query.op === 'count') {
      this.documents = [];
    } else {
      this.documents = mapDocumentsDataToCacheDocs(this.query.collection._docCache, docsDataFromStorageInstance);
    }
  }

  /**
   * Instead of using the newResultData in the result cache,
   * we directly use the objects that are stored in the RxDocument
   * to ensure we do not store the same data twice and fill up the memory.
   * @overwrites itself with the actual value
   */
  get docsData() {
    return overwriteGetterForCaching(this, 'docsData', this.documents.map(d => d._data));
  }

  // A key->document map, used in the event reduce optimization.
  get docsDataMap() {
    var map = new Map();
    this.documents.forEach(d => {
      map.set(d.primary, d._data);
    });
    return overwriteGetterForCaching(this, 'docsDataMap', map);
  }
  get docsMap() {
    var map = new Map();
    var documents = this.documents;
    for (var i = 0; i < documents.length; i++) {
      var doc = documents[i];
      map.set(doc.primary, doc);
    }
    return overwriteGetterForCaching(this, 'docsMap', map);
  }
  getValue(throwIfMissing) {
    var op = this.query.op;
    if (op === 'count') {
      return this.count;
    } else if (op === 'findOne') {
      // findOne()-queries emit RxDocument or null
      var doc = this.documents.length === 0 ? null : this.documents[0];
      if (!doc && throwIfMissing) {
        throw newRxError('QU10', {
          collection: this.query.collection.name,
          query: this.query.mangoQuery,
          op
        });
      } else {
        return doc;
      }
    } else if (op === 'findByIds') {
      return this.docsMap;
    } else {
      // find()-queries emit RxDocument[]
      // Flat copy the array so it won't matter if the user modifies it.
      return this.documents.slice(0);
    }
  }
}
//# sourceMappingURL=rx-query-single-result.js.map