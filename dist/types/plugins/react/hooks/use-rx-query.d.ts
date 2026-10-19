import type { MangoQuery, RxCollection, RxDocument } from '../../../types/index.js';
export type UseRxQueryOptions<RxDocumentType = any, OrmMethods = {}, StaticMethods = {}, InstanceCreationOptions = {}, Reactivity = unknown> = {
    collection: string | RxCollection<RxDocumentType, OrmMethods, StaticMethods, InstanceCreationOptions, Reactivity>;
    query: MangoQuery<RxDocumentType>;
};
export type UseRxQueryResult<RxDocumentType = any, OrmMethods = {}> = {
    results: RxDocument<RxDocumentType, OrmMethods>[];
    loading: boolean;
    error: string | null;
};
/**
 * React hook to query an RxDB collection with Mango queries.
 *
 * @param {UseRxQueryOptions<RxDocumentType, OrmMethods, StaticMethods, InstanceCreationOptions, Reactivity>} options - Options for the query.
 * @param {string|RxCollection} options.collection - The collection name or instance to query.
 * @param {MangoQuery<RxDocumentType>} options.query - The Mango query to execute.
 * @param {boolean} [options.live] - Whether to subscribe to live query updates.
 *
 * @returns {UseRxQueryResult<RxDocumentType, OrmMethods>} The query result, loading state, and error.
 */
export declare function useRxQueryBase<RxDocumentType = any, OrmMethods = {}, StaticMethods = {}, InstanceCreationOptions = {}, Reactivity = unknown>({ collection, query, live, }: UseRxQueryOptions<RxDocumentType, OrmMethods, StaticMethods, InstanceCreationOptions, Reactivity> & {
    live: boolean;
}): UseRxQueryResult<RxDocumentType, OrmMethods>;
export declare function useRxQuery<RxDocumentType = any, OrmMethods = {}, StaticMethods = {}, InstanceCreationOptions = {}, Reactivity = unknown>({ collection, query, }: UseRxQueryOptions<RxDocumentType, OrmMethods, StaticMethods, InstanceCreationOptions, Reactivity>): UseRxQueryResult<RxDocumentType, OrmMethods>;
