/**
 * this plugin adds the json export/import capabilities to RxDB
 */
import { createRxQuery, queryCollection, _getDefaultQuery } from "../../rx-query.js";
import { newRxError } from "../../rx-error.js";
import { blobToBase64String, createBlobFromBase64, flatClone, getDefaultRevision, now } from "../../plugins/utils/index.js";
function dumpRxDatabase(collectionsOrOptions, options) {
  var collections = Array.isArray(collectionsOrOptions) ? collectionsOrOptions : undefined;
  var useOptions = Array.isArray(collectionsOrOptions) ? options : collectionsOrOptions;
  var json = {
    name: this.name,
    instanceToken: this.token,
    collections: []
  };
  var useCollections = Object.keys(this.collections).filter(colName => !collections || collections.includes(colName)).filter(colName => colName.charAt(0) !== '_').map(colName => this.collections[colName]);
  return Promise.all(useCollections.map(col => col.exportJSON(useOptions))).then(cols => {
    json.collections = cols;
    return json;
  });
}
var importDumpRxDatabase = function (dump) {
  /**
   * collections must be created before the import
   * because we do not know about the other collection-settings here
   */
  var missingCollections = dump.collections.filter(col => !this.collections[col.name]).map(col => col.name);
  if (missingCollections.length > 0) {
    throw newRxError('JD1', {
      missingCollections
    });
  }
  return Promise.all(dump.collections.map(colDump => this.collections[colDump.name].importJSON(colDump)));
};

/**
 * Loads the data of all attachments of a single document
 * and returns them in a JSON friendly format where the
 * attachments data is stored as a base64 string.
 */
async function exportAttachmentsOfDocument(collection, documentId, attachments) {
  var ret = {};
  await Promise.all(Object.entries(attachments).map(async ([attachmentId, attachmentData]) => {
    var blob = await collection.storageInstance.getAttachmentData(documentId, attachmentId, attachmentData.digest);
    ret[attachmentId] = {
      type: attachmentData.type,
      length: blob.size,
      data: await blobToBase64String(blob)
    };
  }));
  return ret;
}
var dumpRxCollection = async function (options) {
  var withAttachments = !!(options && options.attachments);
  var primaryPath = this.schema.primaryPath;
  var json = {
    name: this.name,
    schemaHash: await this.schema.hash,
    docs: []
  };
  var query = createRxQuery('find', _getDefaultQuery(), this);
  var result = await queryCollection(query);
  json.docs = await Promise.all(result.docs.map(async docData => {
    var attachments = docData._attachments;
    docData = flatClone(docData);
    delete docData._rev;
    delete docData._attachments;
    if (withAttachments && attachments && Object.keys(attachments).length > 0) {
      docData._attachments = await exportAttachmentsOfDocument(this, docData[primaryPath], attachments);
    }
    return docData;
  }));
  return json;
};

/**
 * Transforms the base64 attachments of an exported document
 * back into the write format that the RxStorage expects.
 */
async function importAttachmentsOfDocument(collection, attachments) {
  var ret = {};
  await Promise.all(Object.entries(attachments).map(async ([attachmentId, attachmentData]) => {
    var blob = await createBlobFromBase64(attachmentData.data, attachmentData.type);
    ret[attachmentId] = {
      type: attachmentData.type,
      length: blob.size,
      data: blob,
      /**
       * The digest is recalculated because the hashFunction
       * of the importing database can be a different one.
       */
      digest: await collection.database.hashFunction(blob)
    };
  }));
  return ret;
}
async function importDumpRxCollection(exportedJSON) {
  // check schemaHash
  if (exportedJSON.schemaHash !== (await this.schema.hash)) {
    throw newRxError('JD2', {
      schemaHash: exportedJSON.schemaHash,
      own: this.schema.hash
    });
  }
  var docs = exportedJSON.docs;
  var rows = await Promise.all(docs.map(async docData => {
    var attachments = {};
    if (docData._attachments) {
      var attachmentsJson = docData._attachments;
      docData = flatClone(docData);
      delete docData._attachments;
      attachments = await importAttachmentsOfDocument(this, attachmentsJson);
    }
    var document = Object.assign({}, docData, {
      _meta: {
        lwt: now()
      },
      _rev: getDefaultRevision(),
      _attachments: attachments,
      _deleted: false
    });
    return {
      document
    };
  }));
  return this.storageInstance.bulkWrite(rows, 'json-dump-import');
}
export var RxDBJsonDumpPlugin = {
  name: 'json-dump',
  rxdb: true,
  prototypes: {
    RxDatabase: proto => {
      proto.exportJSON = dumpRxDatabase;
      proto.importJSON = importDumpRxDatabase;
    },
    RxCollection: proto => {
      proto.exportJSON = dumpRxCollection;
      proto.importJSON = importDumpRxCollection;
    }
  },
  overwritable: {}
};
//# sourceMappingURL=index.js.map