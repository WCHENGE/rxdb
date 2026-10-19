"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
var _exportNames = {
  RxAttachment: true,
  fromStorageInstanceResult: true,
  putAttachment: true,
  putAttachmentBase64: true,
  putAttachments: true,
  getAttachment: true,
  allAttachments: true,
  preMigrateDocument: true,
  postMigrateDocument: true,
  RxDBAttachmentsPlugin: true
};
exports.RxDBAttachmentsPlugin = exports.RxAttachment = void 0;
exports.allAttachments = allAttachments;
exports.fromStorageInstanceResult = fromStorageInstanceResult;
exports.getAttachment = getAttachment;
exports.postMigrateDocument = postMigrateDocument;
exports.preMigrateDocument = preMigrateDocument;
exports.putAttachment = putAttachment;
exports.putAttachmentBase64 = putAttachmentBase64;
exports.putAttachments = putAttachments;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _rxjs = require("rxjs");
var _index = require("../../plugins/utils/index.js");
var _attachmentsUtils = require("./attachments-utils.js");
Object.keys(_attachmentsUtils).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _attachmentsUtils[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _attachmentsUtils[key];
    }
  });
});
var _rxCollectionHelper = require("../../rx-collection-helper.js");
/**
 * an RxAttachment is basically just the attachment-stub
 * wrapped so that you can access the attachment-data
 */
var RxAttachment = exports.RxAttachment = /*#__PURE__*/function () {
  function RxAttachment({
    doc,
    id,
    type,
    length,
    digest
  }) {
    (0, _defineProperty2.default)(this, "doc", void 0);
    (0, _defineProperty2.default)(this, "id", void 0);
    (0, _defineProperty2.default)(this, "type", void 0);
    (0, _defineProperty2.default)(this, "length", void 0);
    (0, _defineProperty2.default)(this, "digest", void 0);
    this.doc = doc;
    this.id = id;
    this.type = type;
    this.length = length;
    this.digest = digest;
    (0, _attachmentsUtils.assignMethodsToAttachment)(this);
  }
  var _proto = RxAttachment.prototype;
  _proto.remove = function remove() {
    (0, _rxCollectionHelper.isWriteAllowed)(this.doc.collection);
    return this.doc.collection.incrementalWriteQueue.addWrite(this.doc._data, docWriteData => {
      delete docWriteData._attachments[this.id];
      return docWriteData;
    }).then(() => {});
  }

  /**
   * returns the data for the attachment
   */;
  _proto.getData = async function getData() {
    var blob = await this.doc.collection.storageInstance.getAttachmentData(this.doc.primary, this.id, this.digest);
    // Some storage layers return blobs without the original MIME type.
    // Ensure the returned Blob has the attachment's MIME type.
    if (blob && blob.type !== this.type) {
      return blob.slice(0, blob.size, this.type);
    }
    return blob;
  };
  _proto.getStringData = async function getStringData() {
    var data = await this.getData();
    var asString = await (0, _index.blobToString)(data);
    return asString;
  };
  _proto.getDataBase64 = async function getDataBase64() {
    var blob = await this.getData();
    return (0, _index.blobToBase64String)(blob);
  };
  return RxAttachment;
}();
function fromStorageInstanceResult(id, attachmentData, rxDocument) {
  return new RxAttachment({
    doc: rxDocument,
    id,
    type: attachmentData.type,
    length: attachmentData.length,
    digest: attachmentData.digest
  });
}
async function _putAttachmentsImpl(doc, attachments) {
  (0, _attachmentsUtils.ensureSchemaSupportsAttachments)(doc);
  (0, _rxCollectionHelper.isWriteAllowed)(doc.collection);
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
    docWriteData = (0, _index.flatClone)(docWriteData);
    docWriteData._attachments = (0, _index.flatClone)(docWriteData._attachments);
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
async function putAttachment(attachmentData) {
  var results = await _putAttachmentsImpl(this, [attachmentData]);
  return results[0];
}
async function putAttachmentBase64(attachmentData) {
  (0, _attachmentsUtils.ensureSchemaSupportsAttachments)(this);
  var blob = await (0, _index.createBlobFromBase64)(attachmentData.data, attachmentData.type);
  return this.putAttachment({
    id: attachmentData.id,
    type: attachmentData.type,
    data: blob
  });
}

/**
 * Write multiple attachments in a single atomic operation.
 */
function putAttachments(attachments) {
  return _putAttachmentsImpl(this, attachments);
}

/**
 * get an attachment of the document by its id
 */
function getAttachment(id) {
  (0, _attachmentsUtils.ensureSchemaSupportsAttachments)(this);
  var docData = this._data;
  if (!docData._attachments || !docData._attachments[id]) return null;
  var attachmentData = docData._attachments[id];
  var attachment = fromStorageInstanceResult(id, attachmentData, this);
  return attachment;
}

/**
 * returns all attachments of the document
 */
function allAttachments() {
  (0, _attachmentsUtils.ensureSchemaSupportsAttachments)(this);
  var docData = this._data;

  // if there are no attachments, the field is missing
  if (!docData._attachments) {
    return [];
  }
  return Object.keys(docData._attachments).map(id => {
    return fromStorageInstanceResult(id, docData._attachments[id], this);
  });
}
async function preMigrateDocument(data) {
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
function postMigrateDocument(_action) {
  /**
   * No longer needed because
   * we store the attachments data buffers directly in the document.
   */
  return _index.PROMISE_RESOLVE_VOID;
}
var RxDBAttachmentsPlugin = exports.RxDBAttachmentsPlugin = {
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
          (0, _rxjs.distinctUntilChanged)((prev, curr) => (0, _index.deepEqual)(Object.keys(prev._data._attachments || {}), Object.keys(curr._data._attachments || {}))), (0, _rxjs.map)(rxDocument => {
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
//# sourceMappingURL=index.js.map