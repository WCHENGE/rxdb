import type { RxCollection } from '../../../types/index.js';
/**
 * The `useRxCollection` hook retrieves an RxDB collection by name and maintains
 * its state as the collection lifecycle changes.
 *
 * @param {string} name The name of the collection to retrieve.
 * @returns The RxCollection instance or null
 * if the collection does not exist or has been closed.
 */
export declare function useRxCollection<RxDocumentType = any, OrmMethods = {}, StaticMethods = {}, InstanceCreationOptions = {}, Reactivity = unknown>(name: string): RxCollection<RxDocumentType, OrmMethods, StaticMethods, InstanceCreationOptions, Reactivity> | null;
