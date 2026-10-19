import type { CollectionsOfDatabase, RxDatabase } from '../../../types/index.js';
/**
 * The `useRxDatabase` hook retrieves the RxDB database instance from context.
 *
 * @returns The RxDB database instance.
 * @throws {Error} Throws an error if the component is not wrapped in a <RxDatabaseProvider>.
 * This ensures the database context is properly initialized before use.
 */
export declare function useRxDatabase<Collections = CollectionsOfDatabase, Internals = any, InstanceCreationOptions = any, Reactivity = any>(): RxDatabase<Collections, Internals, InstanceCreationOptions, Reactivity>;
