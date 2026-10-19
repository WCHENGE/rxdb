import type { RxPlugin, RxCollection } from '../../types/index.js';
import { RxMigrationState } from './rx-migration-state.js';
export declare const DATA_MIGRATOR_BY_COLLECTION: WeakMap<RxCollection, RxMigrationState>;
export declare const RxDBMigrationPlugin: RxPlugin;
export declare const RxDBMigrationSchemaPlugin: RxPlugin;
export * from './rx-migration-state.js';
export * from './migration-helpers.js';
export * from './migration-types.js';
