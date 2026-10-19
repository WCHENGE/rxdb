import type { RxStorage, CompressionMode } from '../../types/index.js';
/**
 * Default MIME type patterns that benefit from compression.
 * Types like images (JPEG, PNG, WebP), videos, and audio
 * are already compressed and should NOT be re-compressed.
 */
export declare const DEFAULT_COMPRESSIBLE_TYPES: string[];
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
export declare function isCompressibleType(mimeType: string, compressibleTypes: string[]): boolean;
/**
 * Compress a Blob using streaming CompressionStream API.
 * @link https://github.com/WICG/compression/blob/main/explainer.md
 */
export declare function compressBlob(mode: CompressionMode, blob: Blob): Promise<Blob>;
/**
 * Decompress a Blob using streaming DecompressionStream API.
 */
export declare function decompressBlob(mode: CompressionMode, blob: Blob): Promise<Blob>;
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
export declare function wrappedAttachmentsCompressionStorage<Internals, InstanceCreationOptions>(args: {
    storage: RxStorage<Internals, InstanceCreationOptions>;
}): RxStorage<Internals, InstanceCreationOptions>;
