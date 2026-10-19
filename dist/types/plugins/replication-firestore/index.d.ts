import type { RxCollection, ReplicationPullOptions, ReplicationPushOptions } from '../../types/index.js';
import { RxReplicationState } from '../replication/index.js';
import type { FirestoreCheckpointType, FirestoreOptions, SyncOptionsFirestore } from './firestore-types.js';
export * from './firestore-helper.js';
export * from './firestore-types.js';
export declare class RxFirestoreReplicationState<RxDocType> extends RxReplicationState<RxDocType, FirestoreCheckpointType> {
    readonly firestore: FirestoreOptions<RxDocType>;
    readonly replicationIdentifierHash: string;
    readonly collection: RxCollection<RxDocType>;
    readonly pull?: ReplicationPullOptions<RxDocType, FirestoreCheckpointType> | undefined;
    readonly push?: ReplicationPushOptions<RxDocType> | undefined;
    readonly live: boolean;
    retryTime: number;
    autoStart: boolean;
    constructor(firestore: FirestoreOptions<RxDocType>, replicationIdentifierHash: string, collection: RxCollection<RxDocType>, pull?: ReplicationPullOptions<RxDocType, FirestoreCheckpointType> | undefined, push?: ReplicationPushOptions<RxDocType> | undefined, live?: boolean, retryTime?: number, autoStart?: boolean);
}
export declare function replicateFirestore<RxDocType>(options: SyncOptionsFirestore<RxDocType>): RxFirestoreReplicationState<RxDocType>;
