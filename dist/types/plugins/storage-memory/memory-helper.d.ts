import type { RxDocumentData, RxJsonSchema } from '../../types/index.js';
import type { DocWithIndexString, MemoryStorageInternals, MemoryStorageInternalsByIndex } from './memory-types.js';
import type { RxStorageInstanceMemory } from './rx-storage-instance-memory.js';
export declare function getMemoryCollectionKey(databaseName: string, collectionName: string, schemaVersion: number): string;
export declare function ensureNotRemoved(instance: RxStorageInstanceMemory<any>): void;
export declare function attachmentMapKey(documentId: string, attachmentId: string): string;
/**
 * @hotPath
 */
export declare function putWriteRowToState<RxDocType>(docId: string, state: MemoryStorageInternals<RxDocType>, stateByIndex: MemoryStorageInternalsByIndex<RxDocType>[], document: RxDocumentData<RxDocType>, docInState?: RxDocumentData<RxDocType>): void;
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
export declare function bulkInsertToState<RxDocType>(primaryPath: string, state: MemoryStorageInternals<RxDocType>, stateByIndex: MemoryStorageInternalsByIndex<RxDocType>[], docs: {
    document: RxDocumentData<RxDocType>;
}[]): void;
export declare function removeDocFromState<RxDocType>(primaryPath: string, schema: RxJsonSchema<RxDocumentData<RxDocType>>, state: MemoryStorageInternals<RxDocType>, doc: RxDocumentData<RxDocType>): void;
export declare function compareDocsWithIndex<RxDocType>(a: DocWithIndexString<RxDocType>, b: DocWithIndexString<RxDocType>): 1 | 0 | -1;
