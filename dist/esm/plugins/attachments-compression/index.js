import { wrapRxStorageInstance } from "../../plugin-helpers.js";
import { ensureNotFalsy, flatClone } from "../utils/index.js";

/**
 * Default MIME type patterns that benefit from compression.
 * Types like images (JPEG, PNG, WebP), videos, and audio
 * are already compressed and should NOT be re-compressed.
 */
export var DEFAULT_COMPRESSIBLE_TYPES = ['text/*', 'application/json', 'application/xml', 'application/xhtml+xml', 'application/javascript', 'application/x-javascript', 'application/ecmascript', 'application/rss+xml', 'application/atom+xml', 'application/soap+xml', 'application/wasm', 'application/x-yaml', 'application/sql', 'application/graphql', 'application/ld+json', 'application/manifest+json', 'application/schema+json', 'application/vnd.api+json', 'image/svg+xml', 'image/bmp', 'font/ttf', 'font/otf', 'application/x-font-ttf', 'application/x-font-otf', 'application/pdf', 'application/rtf', 'application/x-sh', 'application/x-csh', 'application/x-httpd-php'];

/**
 * Normalizes a MIME type by lowercasing it and stripping any
 * RFC 2045 parameters (everything after the first ';'), e.g.
 * 'application/json; charset=utf-8' -> 'application/json'.
 */
function normalizeMimeType(mimeType) {
  var semicolonIndex = mimeType.indexOf(';');
  var withoutParams = semicolonIndex === -1 ? mimeType : mimeType.slice(0, semicolonIndex);
  return withoutParams.trim().toLowerCase();
}

/**
 * Checks if a given MIME type should be compressed,
 * based on a list of type patterns. Supports wildcard suffix matching
 * (e.g., 'text/*' matches 'text/plain', 'text/html', etc.).
 *
 * RFC 2045 parameters on the MIME type (e.g. '; charset=utf-8') are
 * stripped before matching, so 'application/json; charset=utf-8'
 * matches the 'application/json' pattern.
 *
 * Deterministic: same type + same pattern list = same answer.
 * No byte-level inspection needed.
 */
export function isCompressibleType(mimeType, compressibleTypes) {
  var lower = normalizeMimeType(mimeType);
  for (var pattern of compressibleTypes) {
    var lowerPattern = normalizeMimeType(pattern);
    if (lowerPattern.endsWith('/*')) {
      // 'text/*' -> 'text/', so startsWith matches all subtypes
      var prefix = lowerPattern.slice(0, -1);
      if (lower.startsWith(prefix)) {
        return true;
      }
    } else if (lower === lowerPattern) {
      return true;
    }
  }
  return false;
}

/**
 * Compress a Blob using streaming CompressionStream API.
 * @link https://github.com/WICG/compression/blob/main/explainer.md
 */
export async function compressBlob(mode, blob) {
  var stream = blob.stream().pipeThrough(new CompressionStream(mode));
  return new Response(stream).blob();
}

/**
 * Decompress a Blob using streaming DecompressionStream API.
 */
export async function decompressBlob(mode, blob) {
  var stream = blob.stream().pipeThrough(new DecompressionStream(mode));
  return new Response(stream).blob();
}

/**
 * Digest prefix that marks an attachment as having been
 * stored with compression. Used by getAttachmentData to
 * determine whether decompression is needed, without
 * having to fetch the full document to inspect the MIME type.
 */
var COMPRESSED_DIGEST_PREFIX = 'c-';

/**
 * A RxStorage wrapper that compresses attachment data on writes
 * and decompresses the data on reads.
 *
 * Only compresses attachments whose MIME type is in the compressible list.
 * Already-compressed formats (JPEG, PNG, MP4, etc.) are passed through as-is.
 *
 * This is using the CompressionStream API,
 * @link https://caniuse.com/?search=compressionstream
 */
export function wrappedAttachmentsCompressionStorage(args) {
  return Object.assign({}, args.storage, {
    async createStorageInstance(params) {
      if (!params.schema.attachments || !params.schema.attachments.compression) {
        return args.storage.createStorageInstance(params);
      }
      var mode = params.schema.attachments.compression;
      var compressibleTypes = params.schema.attachments.compressibleTypes || DEFAULT_COMPRESSIBLE_TYPES;
      async function modifyToStorage(docData) {
        await Promise.all(Object.values(docData._attachments).map(async attachment => {
          if (!attachment.data) {
            return;
          }
          var attachmentWriteData = attachment;
          if (isCompressibleType(attachmentWriteData.type, compressibleTypes)) {
            attachmentWriteData.data = await compressBlob(mode, attachmentWriteData.data);
            /**
             * Prefix the digest to signal that this attachment is compressed.
             * This is intentional: the stored digest is 'c-' + hash(originalData),
             * NOT hash(compressedData). RxDB does not use digests for integrity
             * verification — only for change detection (comparing before vs after).
             * Change detection remains correct because both sides of any comparison
             * consistently carry the 'c-' prefix for compressed attachments.
             * All write paths flow through this modifyToStorage wrapper so the
             * prefix is always applied, regardless of which API was used to write.
             */
            if (!attachmentWriteData.digest.startsWith(COMPRESSED_DIGEST_PREFIX)) {
              attachmentWriteData.digest = COMPRESSED_DIGEST_PREFIX + attachmentWriteData.digest;
            }
          }
        }));
        return docData;
      }

      /**
       * Because this wrapper resolves the attachments.compression,
       * we have to remove it before sending it to the underlying RxStorage.
       * which allows underlying storages to detect wrong configurations
       * like when compression is set to false but no attachment-compression module is used.
       */
      var childSchema = flatClone(params.schema);
      childSchema.attachments = flatClone(childSchema.attachments);
      delete ensureNotFalsy(childSchema.attachments).compression;
      var instance = await args.storage.createStorageInstance(Object.assign({}, params, {
        schema: childSchema
      }));
      var wrappedInstance = wrapRxStorageInstance(params.schema, instance, modifyToStorage, d => d);

      /**
       * Override getAttachmentData to decompress based on the
       * digest prefix rather than looking up the document's MIME type.
       */
      wrappedInstance.getAttachmentData = async (documentId, attachmentId, digest) => {
        var data = await instance.getAttachmentData(documentId, attachmentId, digest);
        if (digest.startsWith(COMPRESSED_DIGEST_PREFIX)) {
          return decompressBlob(mode, data);
        }
        return data;
      };
      return wrappedInstance;
    }
  });
}
//# sourceMappingURL=index.js.map