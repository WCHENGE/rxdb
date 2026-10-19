/**
 * These files contain the replication protocol.
 * It can be used to replicated RxStorageInstances or RxCollections
 * or even to do a client(s)-server replication.
 */
import type { RxConflictHandler, RxReplicationHandler, RxStorageInstance, RxStorageInstanceReplicationInput, RxStorageInstanceReplicationState } from '../types/index.js';
export * from './checkpoint.js';
export * from './downstream.js';
export * from './upstream.js';
export * from './meta-instance.js';
export * from './conflicts.js';
export * from './helper.js';
export * from './default-conflict-handler.js';
/**
 * Used in tests to ensure all states are cleaned up
 * and we have no memory leak.
 */
export declare const OPEN_REPLICATION_STATES: Set<RxStorageInstanceReplicationState<any>>;
export declare function replicateRxStorageInstance<RxDocType>(input: RxStorageInstanceReplicationInput<RxDocType>): RxStorageInstanceReplicationState<RxDocType>;
export declare function awaitRxStorageReplicationFirstInSync(state: RxStorageInstanceReplicationState<any>): Promise<void>;
export declare function awaitRxStorageReplicationInSync(replicationState: RxStorageInstanceReplicationState<any>): Promise<[any, any, any]>;
export declare function awaitRxStorageReplicationIdle(state: RxStorageInstanceReplicationState<any>): Promise<void>;
export declare function rxStorageInstanceToReplicationHandler<RxDocType, MasterCheckpointType>(instance: RxStorageInstance<RxDocType, any, any, MasterCheckpointType>, conflictHandler: RxConflictHandler<RxDocType>, databaseInstanceToken: string, 
/**
 * If set to true,
 * the _meta.lwt from the pushed documents is kept.
 * (Used in the migration to ensure checkpoints are still valid)
 */
keepMeta?: boolean): RxReplicationHandler<RxDocType, MasterCheckpointType>;
export declare function cancelRxStorageReplication(replicationState: RxStorageInstanceReplicationState<any>): Promise<void>;
