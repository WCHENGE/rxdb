import type { RxCollection, RxDocument } from '../../../types/index.js';
export type UseRxDocumentResult<RxDocumentType = any, OrmMethods = {}> = {
    result: RxDocument<RxDocumentType, OrmMethods> | null;
    loading: boolean;
    error: string | null;
};
/**
 * React hook to subscribe to a single RxDB document by its primary key.
 * Returns the document with live updates when it changes.
 *
 * @param collection - The RxCollection instance to query from.
 * @param primaryKey - The primary key value of the document to subscribe to.
 * @returns The document, loading state, and error.
 */
export declare function useRxDocument<RxDocumentType = any, OrmMethods = {}>(collection: RxCollection<RxDocumentType, OrmMethods, any, any, any> | null | undefined, primaryKey: string | undefined): UseRxDocumentResult<RxDocumentType, OrmMethods>;
