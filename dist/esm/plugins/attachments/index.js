import { distinctUntilChanged, map } from 'rxjs';
import { blobToBase64String, blobToString, createBlobFromBase64, deepEqual, flatClone, PROMISE_RESOLVE_VOID } from "../../plugins/utils/index.js";
import { assignMethodsToAttachment, ensureSchemaSupportsAttachments } from "./attachments-utils.js";
import { isWriteAllowed } from "../../rx-collection-helper.js";

/**
 * an RxAttachment is basically just the attachment-stub
 * wrapped so that you can access the attachment-data
 */
export class RxAttachment {
  doc;
  id;
  type;
  length;
  digest;
  constructor({
    doc,
    id,
    type,
    length,
    digest
  }) {
    this.doc = doc;
    this.id = id;
    this.type = type;
    this.length = length;
    this.digest = digest;
    assignMethodsToAttachment(this);
  }
  remove() {
    isWriteAllowed(this.doc.collection);
    return this.doc.collection.incrementalWriteQueue.addWrite(this.doc._data, docWriteData => {
      delete docWriteData._attachments[this.id];
      return docWriteData;
    }).then(() => {});
  }

  /**
   * returns the data for the attachment
   */
  async getData() {
    var blob = await this.doc.collection.storageInstance.getAttachmentData(this.doc.primary, this.id, this.digest);
    // Some storage layers return blobs without the original MIME type.
    // Ensure the returned Blob has the attachment's MIME type.
    if (blob && blob.type !== this.type) {
      return blob.slice(0, blob.size, this.type);
    }
    return blob;
  }
  async getStringData() {
    var data = await this.getData();
    var asString = await blobToString(data);
    return asString;
  }
  async getDataBase64() {
    var blob = await this.getData();
    return blobToBase64String(blob);
  }
}
export function fromStorageInstanceResult(id, attachmentData, rxDocument) {
  return new RxAttachment({
    doc: rxDocument,
    id,
    type: attachmentData.type,
    length: attachmentData.length,
    digest: attachmentData.digest
  });
}
async function _putAttachmentsImpl(doc, attachments) {
  ensureSchemaSupportsAttachments(doc);
  isWriteAllowed(doc.collection);
  if (attachments.length === 0) {
    return [];
  }
  var prepared = await Promise.all(attachments.map(async att => ({
    id: att.id,
    type: att.type,
    data: att.data,
    digest: await doc.collection.database.hashFunction(att.data)
  })));
  var writeResult = await doc.collection.incrementalWriteQueue.addWrite(doc._data, docWriteData => {
    docWriteData = flatClone(docWriteData);
    docWriteData._attachments = flatClone(docWriteData._attachments);
    for (var att of prepared) {
      docWriteData._attachments[att.id] = {
        length: att.data.size,
        type: att.type,
        data: att.data,
        digest: att.digest
      };
    }
    return docWriteData;
  });
  var newDocument = doc.collection._docCache.getCachedRxDocument(writeResult);
  return prepared.map(att => fromStorageInstanceResult(att.id, writeResult._attachments[att.id], newDocument));
}
export async function putAttachment(attachmentData) {
  var results = await _putAttachmentsImpl(this, [attachmentData]);
  return results[0];
}
export async function putAttachmentBase64(attachmentData) {
  ensureSchemaSupportsAttachments(this);
  var blob = await createBlobFromBase64(attachmentData.data, attachmentData.type);
  return this.putAttachment({
    id: attachmentData.id,
    type: attachmentData.type,
    data: blob
  });
}

/**
 * Write multiple attachments in a single atomic operation.
 */
export function putAttachments(attachments) {
  return _putAttachmentsImpl(this, attachments);
}

/**
 * get an attachment of the document by its id
 */
export function getAttachment(id) {
  ensureSchemaSupportsAttachments(this);
  var docData = this._data;
  if (!docData._attachments || !docData._attachments[id]) return null;
  var attachmentData = docData._attachments[id];
  var attachment = fromStorageInstanceResult(id, attachmentData, this);
  return attachment;
}

/**
 * returns all attachments of the document
 */
export function allAttachments() {
  ensureSchemaSupportsAttachments(this);
  var docData = this._data;

  // if there are no attachments, the field is missing
  if (!docData._attachments) {
    return [];
  }
  return Object.keys(docData._attachments).map(id => {
    return fromStorageInstanceResult(id, docData._attachments[id], this);
  });
}
export async function preMigrateDocument(data) {
  var attachments = data.docData._attachments;
  if (attachments) {
    var newAttachments = {};
    await Promise.all(Object.keys(attachments).map(async attachmentId => {
      var attachment = attachments[attachmentId];
      var docPrimary = data.docData[data.oldCollection.schema.primaryPath];
      var rawAttachmentBlob = await data.oldCollection.storageInstance.getAttachmentData(docPrimary, attachmentId, attachment.digest);
      var digest = await data.oldCollection.database.hashFunction(rawAttachmentBlob);
      newAttachments[attachmentId] = {
        length: rawAttachmentBlob.size,
        type: attachment.type,
        data: rawAttachmentBlob,
        digest
      };
    }));

    /**
     * Hooks mutate the input
     * instead of returning stuff
     */
    data.docData._attachments = newAttachments;
  }
}
export function postMigrateDocument(_action) {
  /**
   * No longer needed because
   * we store the attachments data buffers directly in the document.
   */
  return PROMISE_RESOLVE_VOID;
}
export var RxDBAttachmentsPlugin = {
  name: 'attachments',
  rxdb: true,
  prototypes: {
    RxDocument: proto => {
      proto.putAttachment = putAttachment;
      proto.putAttachments = putAttachments;
      proto.putAttachmentBase64 = putAttachmentBase64;
      proto.getAttachment = getAttachment;
      proto.allAttachments = allAttachments;
      Object.defineProperty(proto, 'allAttachments$', {
        get: function allAttachments$() {
          return this.$.pipe(
          /**
           * Only emit when the set of attachments has actually changed.
           * Without this filter, any unrelated document revision
           * (e.g. a field update) would cause a new emission, which is
           * both wasteful and surprising for consumers that only care
           * about attachment changes.
           */
          distinctUntilChanged((prev, curr) => deepEqual(Object.keys(prev._data._attachments || {}), Object.keys(curr._data._attachments || {}))), map(rxDocument => {
            return Object.entries(rxDocument.toJSON(true)._attachments).map(([id, attachmentData]) => {
              return fromStorageInstanceResult(id, attachmentData, rxDocument);
            });
          }));
        }
      });
    }
  },
  overwritable: {},
  hooks: {
    preMigrateDocument: {
      after: preMigrateDocument
    },
    postMigrateDocument: {
      after: postMigrateDocument
    }
  }
};
export * from "./attachments-utils.js";
//# sourceMappingURL=index.js.map