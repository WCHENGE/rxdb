"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.deserializeDocAttachments = deserializeDocAttachments;
exports.docStateToWriteDoc = docStateToWriteDoc;
exports.getUnderlyingPersistentStorage = getUnderlyingPersistentStorage;
exports.serializeDocAttachments = serializeDocAttachments;
exports.stripAllAttachmentDataForComparison = stripAllAttachmentDataForComparison;
exports.stripAttachmentsDataFromMetaWriteRows = stripAttachmentsDataFromMetaWriteRows;
exports.writeDocToDocState = writeDocToDocState;
var _index = require("../plugins/utils/index.js");
var _rxStorageHelper = require("../rx-storage-helper.js");
function docStateToWriteDoc(databaseInstanceToken, hasAttachments, keepMeta, docState, previous) {
  var docData = Object.assign({}, docState, {
    _attachments: hasAttachments && docState._attachments ? docState._attachments : {},
    _meta: keepMeta ? docState._meta : Object.assign({}, previous ? previous._meta : {}, {
      lwt: (0, _index.now)()
    }),
    _rev: keepMeta ? docState._rev : (0, _index.getDefaultRevision)()
  });
  if (!docData._rev) {
    docData._rev = (0, _index.createRevision)(databaseInstanceToken, previous);
  }
  return docData;
}
function writeDocToDocState(writeDoc, keepAttachments, keepMeta) {
  var ret = (0, _index.flatClone)(writeDoc);
  if (!keepAttachments) {
    delete ret._attachments;
  }
  if (!keepMeta) {
    delete ret._meta;
    delete ret._rev;
  }
  return ret;
}
function stripAttachmentsDataFromMetaWriteRows(state, rows) {
  if (!state.hasAttachments) {
    return rows;
  }
  return rows.map(row => {
    var document = (0, _index.clone)(row.document);
    document.docData = (0, _rxStorageHelper.stripAttachmentsDataFromDocument)(document.docData);
    return {
      document,
      previous: row.previous
    };
  });
}

/**
 * Serialises attachment data in a document clone so it can safely be stored
 * as JSON in a remote storage that does not natively support attachments
 * (e.g. Google Drive, OneDrive).
 *
 * - When `serializeData` is true: Blob values are extracted and stored as
 *   base64 strings in the top-level `_attachments_data` field, while
 *   `_attachments` is stripped to clean stubs via `stripAttachmentsDataFromDocument`.
 * - When `serializeData` is false: `_attachments` is set to `{}` so that
 *   attachment stubs (without binary data) are never persisted. This
 *   prevents the downstream replication protocol from trying to write
 *   attachment data it does not have when a peer pulls the document.
 *
 * The function returns a NEW document object; the original is not mutated.
 */
async function serializeDocAttachments(doc, serializeData) {
  var d = doc;
  if (!d?._attachments) {
    return doc;
  }
  if (!serializeData) {
    return {
      ...d,
      _attachments: {}
    };
  }
  var attachmentData = {};
  await Promise.all(Object.entries(d._attachments).map(async ([id, att]) => {
    if (att.data instanceof Blob) {
      attachmentData[id] = await (0, _index.blobToBase64String)(att.data);
    }
  }));
  var stripped = (0, _rxStorageHelper.stripAttachmentsDataFromDocument)(d);
  if (Object.keys(attachmentData).length > 0) {
    return {
      ...stripped,
      _attachments_data: attachmentData
    };
  }
  return stripped;
}

/**
 * Strips both `_attachments` data and the serialised `_attachments_data` field
 * from a document, leaving only the structural attachment metadata
 * (digest / length / type). Used to compare two document states without
 * being affected by attachment binary data that lives in different shapes on
 * each side (Blobs in memory vs base64 in the serialised JSON file).
 */
function stripAllAttachmentDataForComparison(doc) {
  var stripped = (0, _rxStorageHelper.stripAttachmentsDataFromDocument)(doc);
  delete stripped._attachments_data;
  return stripped;
}

/**
 * Converts attachment data stored in `_attachments_data` back to Blobs in the
 * document, so that the downstream replication protocol can write them to the
 * fork storage instance correctly.
 *
 * Mutates the document in place and removes the `_attachments_data` field.
 */
async function deserializeDocAttachments(doc) {
  var attachmentData = doc._attachments_data;
  if (!attachmentData || !doc._attachments) {
    return;
  }
  await Promise.all(Object.entries(attachmentData).map(async ([id, base64]) => {
    if (doc._attachments[id]) {
      doc._attachments[id] = {
        ...doc._attachments[id],
        data: await (0, _index.createBlobFromBase64)(base64, doc._attachments[id].type)
      };
    }
  }));
  delete doc._attachments_data;
}
function getUnderlyingPersistentStorage(instance) {
  while (true) {
    if (instance.underlyingPersistentStorage) {
      instance = instance.underlyingPersistentStorage;
    } else {
      return instance;
    }
  }
}
//# sourceMappingURL=helper.js.map