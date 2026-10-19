import type { RxCollection, ReplicationPullOptions, ReplicationPushOptions, RxDocumentData } from '../../types/index.js';
import { RxReplicationState } from '../replication/index.js';
import type { MongoDbCheckpointType, SyncOptionsMongoDB } from './mongodb-types.js';
import { Db as MongoDatabase, Collection as MongoCollection, MongoClient } from 'mongodb';
export * from './mongodb-helper.js';
export * from './mongodb-checkpoint.js';
export type * from './mongodb-types.js';
export declare class RxMongoDBReplicationState<RxDocType> extends RxReplicationState<RxDocType, MongoDbCheckpointType> {
    readonly mongoClient: MongoClient;
    readonly mongoDatabase: MongoDatabase;
    readonly mongoCollection: MongoCollection<RxDocumentData<RxDocType> | any>;
    readonly options: SyncOptionsMongoDB<RxDocType>;
    readonly replicationIdentifier: string;
    readonly collection: RxCollection<RxDocType, any, any>;
    readonly pull?: ReplicationPullOptions<RxDocType, MongoDbCheckpointType> | undefined;
    readonly push?: ReplicationPushOptions<RxDocType> | undefined;
    readonly live: boolean;
    retryTime: number;
    autoStart: boolean;
    constructor(mongoClient: MongoClient, mongoDatabase: MongoDatabase, mongoCollection: MongoCollection<RxDocumentData<RxDocType> | any>, options: SyncOptionsMongoDB<RxDocType>, replicationIdentifier: string, collection: RxCollection<RxDocType, any, any>, pull?: ReplicationPullOptions<RxDocType, MongoDbCheckpointType> | undefined, push?: ReplicationPushOptions<RxDocType> | undefined, live?: boolean, retryTime?: number, autoStart?: boolean);
}
export declare function replicateMongoDB<RxDocType>(options: SyncOptionsMongoDB<RxDocType>): RxMongoDBReplicationState<RxDocType>;
