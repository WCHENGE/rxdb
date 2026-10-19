import type { RxError, RxTypeError } from '../../../types/index.js';
import type { RxReplicationState } from '../../replication/index.js';
export type UseReplicationStatusResult = {
    /**
     * Whether the replication is currently active (syncing).
     */
    syncing: boolean;
    /**
     * The most recent replication error, or null if no error.
     */
    error: RxError | RxTypeError | null;
    /**
     * Timestamp (ms) of the last successful sync event, or null if never synced.
     */
    lastSyncedAt: number | null;
    /**
     * Whether the replication has been canceled.
     */
    canceled: boolean;
};
/**
 * React hook that subscribes to an RxReplicationState and exposes
 * its status as React state. Provides syncing, error, lastSyncedAt,
 * and canceled fields.
 *
 * @param replicationState - The RxReplicationState to observe, or null/undefined.
 * @returns The current replication status.
 */
export declare function useReplicationStatus(replicationState: RxReplicationState<any, any> | null | undefined): UseReplicationStatusResult;
