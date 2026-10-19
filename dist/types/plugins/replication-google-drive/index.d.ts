import type { RxCollection, ReplicationPullOptions, ReplicationPushOptions } from '../../types/index.js';
import { RxReplicationState } from '../replication/index.js';
import type { GoogleDriveCheckpointType, GoogleDriveOptionsWithDefaults, SyncOptionsGoogleDrive } from './google-drive-types.js';
import { DriveStructure } from './init.js';
import { SignalingOptions, SignalingState } from './signaling.js';
export * from './google-drive-types.js';
export * from './google-drive-helper.js';
export * from './transaction.js';
export * from './init.js';
export * from './document-handling.js';
export * from './multipart.js';
export * from './downstream.js';
export * from './upstream.js';
export * from './signaling.js';
export declare const DEFAULT_TRANSACTION_TIMEOUT: number;
export declare class RxGoogleDriveReplicationState<RxDocType> extends RxReplicationState<RxDocType, GoogleDriveCheckpointType> {
    readonly googleDrive: GoogleDriveOptionsWithDefaults;
    readonly driveStructure: DriveStructure;
    readonly replicationIdentifierHash: string;
    readonly collection: RxCollection<RxDocType, any, any, any>;
    readonly pull?: ReplicationPullOptions<RxDocType, GoogleDriveCheckpointType> | undefined;
    readonly push?: ReplicationPushOptions<RxDocType> | undefined;
    readonly signalingOptions?: SignalingOptions | undefined;
    readonly live: boolean;
    retryTime: number;
    autoStart: boolean;
    /**
     * Only exists on live replication
     */
    signalingState?: SignalingState;
    constructor(googleDrive: GoogleDriveOptionsWithDefaults, driveStructure: DriveStructure, replicationIdentifierHash: string, collection: RxCollection<RxDocType, any, any, any>, pull?: ReplicationPullOptions<RxDocType, GoogleDriveCheckpointType> | undefined, push?: ReplicationPushOptions<RxDocType> | undefined, signalingOptions?: SignalingOptions | undefined, live?: boolean, retryTime?: number, autoStart?: boolean);
    /**
     * Notify other peers that something
     * has or might have changed so that
     * they can pull from their checkpoints.
     */
    notifyPeers(): Promise<void>;
}
export declare function replicateGoogleDrive<RxDocType>(options: SyncOptionsGoogleDrive<RxDocType>): Promise<RxGoogleDriveReplicationState<RxDocType>>;
