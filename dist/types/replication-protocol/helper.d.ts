import type { BulkWriteRow, RxDocumentData, RxDocumentWriteData, RxStorageInstance, RxStorageInstanceReplicationState, RxStorageReplicationMeta, WithDeletedAndAttachments } from '../types/index.js';
export declare function docStateToWriteDoc<RxDocType>(databaseInstanceToken: string, hasAttachments: boolean, keepMeta: boolean, docState: WithDeletedAndAttachments<RxDocType>, previous?: RxDocumentData<RxDocType>): RxDocumentWriteData<RxDocType>;
export declare function writeDocToDocState<RxDocType>(writeDoc: RxDocumentData<RxDocType>, keepAttachments: boolean, keepMeta: boolean): WithDeletedAndAttachments<RxDocType>;
export declare function stripAttachmentsDataFromMetaWriteRows<RxDocType>(state: RxStorageInstanceReplicationState<any>, rows: BulkWriteRow<RxStorageReplicationMeta<RxDocType, any>>[]): BulkWriteRow<RxStorageReplicationMeta<RxDocType, any>>[];
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
export declare function serializeDocAttachments<T>(doc: T, serializeData: boolean): Promise<T>;
/**
 * Strips both `_attachments` data and the serialised `_attachments_data` field
 * from a document, leaving only the structural attachment metadata
 * (digest / length / type). Used to compare two document states without
 * being affected by attachment binary data that lives in different shapes on
 * each side (Blobs in memory vs base64 in the serialised JSON file).
 */
export declare function stripAllAttachmentDataForComparison<T>(doc: T): T;
/**
 * Converts attachment data stored in `_attachments_data` back to Blobs in the
 * document, so that the downstream replication protocol can write them to the
 * fork storage instance correctly.
 *
 * Mutates the document in place and removes the `_attachments_data` field.
 */
export declare function deserializeDocAttachments(doc: any): Promise<void>;
export declare function getUnderlyingPersistentStorage<RxDocType>(instance: RxStorageInstance<RxDocType, any, any, any>): RxStorageInstance<RxDocType, any, any, any>;
