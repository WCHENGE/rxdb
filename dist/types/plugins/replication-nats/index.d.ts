import type { RxCollection, ReplicationPullOptions, ReplicationPushOptions } from '../../types/index.js';
import { RxReplicationState } from '../replication/index.js';
import type { NatsCheckpointType, NatsSyncOptions } from './nats-types.js';
export * from './nats-types.js';
export * from './nats-helper.js';
export declare class RxNatsReplicationState<RxDocType> extends RxReplicationState<RxDocType, NatsCheckpointType> {
    readonly replicationIdentifier: string;
    readonly collection: RxCollection<RxDocType>;
    readonly pull?: ReplicationPullOptions<RxDocType, NatsCheckpointType> | undefined;
    readonly push?: ReplicationPushOptions<RxDocType> | undefined;
    readonly live: boolean;
    retryTime: number;
    autoStart: boolean;
    constructor(replicationIdentifier: string, collection: RxCollection<RxDocType>, pull?: ReplicationPullOptions<RxDocType, NatsCheckpointType> | undefined, push?: ReplicationPushOptions<RxDocType> | undefined, live?: boolean, retryTime?: number, autoStart?: boolean);
}
export declare function replicateNats<RxDocType>(options: NatsSyncOptions<RxDocType>): RxNatsReplicationState<RxDocType>;
