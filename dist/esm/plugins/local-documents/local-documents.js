import { getDefaultRevision, getDefaultRxDocumentMeta } from "../../plugins/utils/index.js";
import { filter, map, startWith, mergeMap } from 'rxjs';
import { getLocalDocStateByParent } from "./local-documents-helper.js";
import { getSingleDocument, writeSingle } from "../../rx-storage-helper.js";

/**
 * save the local-document-data
 * throws if already exists
 */
export async function insertLocal(id, data) {
  var state = await getLocalDocStateByParent(this);

  // create new one
  var docData = {
    id: id,
    data,
    _deleted: false,
    _meta: getDefaultRxDocumentMeta(),
    _rev: getDefaultRevision(),
    _attachments: {}
  };
  return writeSingle(state.storageInstance, {
    document: docData
  }, 'local-document-insert').then(newDocData => state.docCache.getCachedRxDocument(newDocData));
}

/**
 * save the local-document-data
 * overwrites existing if exists
 */
export async function upsertLocal(id, data) {
  var state = await getLocalDocStateByParent(this);
  while (true) {
    try {
      var docDataFromCache = state.docCache.getLatestDocumentDataIfExists(id);
      if (!docDataFromCache) {
        // create new one
        return await this.insertLocal(id, data);
      } else if (docDataFromCache._deleted) {
        // document was deleted before, un-delete it via the write queue
        var writeResult = await state.incrementalWriteQueue.addWrite(docDataFromCache, docData => {
          docData.data = data;
          docData._deleted = false;
          return docData;
        });
        return state.docCache.getCachedRxDocument(writeResult);
      } else {
        // update existing
        var existing = state.docCache.getCachedRxDocument(docDataFromCache);
        return await existing.incrementalModify(() => {
          return data;
        });
      }
    } catch (err) {
      if (err && err.status === 409) {
        if (err.documentInDb) {
          state.docCache.getCachedRxDocument(err.documentInDb);
        } else {
          var latestDoc = await getSingleDocument(state.storageInstance, id);
          if (latestDoc) {
            state.docCache.getCachedRxDocument(latestDoc);
          }
        }
        continue;
      }
      throw err;
    }
  }
}
export async function getLocal(id) {
  var state = await getLocalDocStateByParent(this);
  var docCache = state.docCache;

  // check in doc-cache
  var found = docCache.getLatestDocumentDataIfExists(id);
  if (found) {
    if (found._deleted) {
      return null;
    }
    return Promise.resolve(docCache.getCachedRxDocument(found));
  }

  // if not found, check in storage instance
  return getSingleDocument(state.storageInstance, id).then(docData => {
    if (!docData) {
      return null;
    }
    return state.docCache.getCachedRxDocument(docData);
  });
}
export function getLocal$(id) {
  return this.$.pipe(startWith(null), mergeMap(async cE => {
    if (cE) {
      return {
        changeEvent: cE
      };
    } else {
      var doc = await this.getLocal(id);
      return {
        doc: doc
      };
    }
  }), mergeMap(async changeEventOrDoc => {
    if (changeEventOrDoc.changeEvent) {
      var cE = changeEventOrDoc.changeEvent;
      if (!cE.isLocal || cE.documentId !== id) {
        return {
          use: false
        };
      } else {
        var doc = await this.getLocal(id);
        return {
          use: true,
          doc: doc
        };
      }
    } else {
      return {
        use: true,
        doc: changeEventOrDoc.doc
      };
    }
  }), filter(filterFlagged => filterFlagged.use), map(filterFlagged => {
    return filterFlagged.doc;
  }));
}
//# sourceMappingURL=local-documents.js.map