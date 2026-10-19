import type { RxCollection, ReplicationPullOptions, ReplicationPushOptions } from '../../types/index.js';
import { RxReplicationState } from '../replication/index.js';
import type { OneDriveCheckpointType, OneDriveState, SyncOptionsOneDrive } from './microsoft-onedrive-types.js';
import { DriveStructure } from './init.js';
import { SignalingOptions, SignalingState } from './signaling.js';
export * from './microsoft-onedrive-types.js';
export * from './microsoft-onedrive-helper.js';
export * from './transaction.js';
export * from './init.js';
export * from './document-handling.js';
export * from './downstream.js';
export * from './upstream.js';
export * from './signaling.js';
export declare const DEFAULT_TRANSACTION_TIMEOUT: number;
export declare class RxOneDriveReplicationState<RxDocType> extends RxReplicationState<RxDocType, OneDriveCheckpointType> {
    readonly oneDrive: OneDriveState;
    readonly driveStructure: DriveStructure;
    readonly replicationIdentifierHash: string;
    readonly collection: RxCollection<RxDocType, any>;
    readonly pull?: ReplicationPullOptions<RxDocType, OneDriveCheckpointType> | undefined;
    readonly push?: ReplicationPushOptions<RxDocType> | undefined;
    readonly signalingOptions?: SignalingOptions | undefined;
    readonly live: boolean;
    retryTime: number;
    autoStart: boolean;
    /**
     * Only exists on live replication
     */
    signalingState?: SignalingState;
    constructor(oneDrive: OneDriveState, driveStructure: DriveStructure, replicationIdentifierHash: string, collection: RxCollection<RxDocType, any>, pull?: ReplicationPullOptions<RxDocType, OneDriveCheckpointType> | undefined, push?: ReplicationPushOptions<RxDocType> | undefined, signalingOptions?: SignalingOptions | undefined, live?: boolean, retryTime?: number, autoStart?: boolean);
    /**
     * Notify other peers that something
     * has or might have changed so that
     * they can pull from their checkpoints.
     */
    notifyPeers(): Promise<void>;
}
export declare function replicateMicrosoftOneDrive<RxDocType>(options: SyncOptionsOneDrive<RxDocType>): Promise<RxOneDriveReplicationState<RxDocType>>;
