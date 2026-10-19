import { RxStorage, RxStorageInstanceCreationParams } from '../../index.js';
import { RxStorageInstanceSQLite } from './sqlite-storage-instance.js';
import type { SQLiteInternals, SQLiteInstanceCreationOptions, SQLiteStorageSettings } from './sqlite-types.js';
export * from './sqlite-helpers.js';
export * from './sqlite-types.js';
export * from './sqlite-storage-instance.js';
export * from './sqlite-basics-helpers.js';
export declare class RxStorageSQLiteTrial implements RxStorage<SQLiteInternals, SQLiteInstanceCreationOptions> {
    settings: SQLiteStorageSettings;
    name: string;
    readonly rxdbVersion = "17.6.0";
    constructor(settings: SQLiteStorageSettings);
    createStorageInstance<RxDocType>(params: RxStorageInstanceCreationParams<RxDocType, SQLiteInstanceCreationOptions>): Promise<RxStorageInstanceSQLite<RxDocType>>;
}
export declare function getRxStorageSQLiteTrial(settings: SQLiteStorageSettings): RxStorageSQLiteTrial;
