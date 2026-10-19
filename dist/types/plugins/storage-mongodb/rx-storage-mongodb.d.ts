import type { RxStorage, RxStorageInstanceCreationParams } from '../../types/index.js';
import type { MongoDBDatabaseSettings, MongoDBSettings, MongoDBStorageInternals } from './mongodb-types.js';
import { RxStorageInstanceMongoDB } from './rx-storage-instance-mongodb.js';
export declare class RxStorageMongoDB implements RxStorage<MongoDBStorageInternals, MongoDBSettings> {
    databaseSettings: MongoDBDatabaseSettings;
    name: string;
    readonly rxdbVersion = "17.6.0";
    constructor(databaseSettings: MongoDBDatabaseSettings);
    createStorageInstance<RxDocType>(params: RxStorageInstanceCreationParams<RxDocType, MongoDBSettings>): Promise<RxStorageInstanceMongoDB<RxDocType>>;
}
export declare function getRxStorageMongoDB(databaseSettings: MongoDBDatabaseSettings): RxStorageMongoDB;
