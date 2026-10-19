import { Observable } from 'rxjs';
import type { NumberFunctionMap, RxCollection, RxDatabase, RxStorageInstance, RxStorageInstanceReplicationState } from '../../types/index.js';
import { getOldCollectionMeta, mustMigrate } from './migration-helpers.js';
import type { MigrationStatusUpdate, RxMigrationStatus } from './migration-types.js';
import { BroadcastChannel } from 'broadcast-channel';
export declare class RxMigrationState {
    readonly collection: RxCollection;
    readonly migrationStrategies: NumberFunctionMap;
    readonly statusDocKey: string;
    database: RxDatabase;
    private started;
    readonly oldCollectionMeta: ReturnType<typeof getOldCollectionMeta>;
    readonly mustMigrate: ReturnType<typeof mustMigrate>;
    readonly statusDocId: string;
    readonly $: Observable<RxMigrationStatus>;
    /**
     * Contains ALL replication states
     * that are ever used in this migration state.
     */
    replicationStates: Set<RxStorageInstanceReplicationState<any>>;
    /**
     * All storage instances that are opened by the migration itself.
     * They have to be closed when the migration is canceled,
     * otherwise they stay open forever when the migration is interrupted,
     * for example when the database is closed while the migration is running.
     */
    openStorageInstances: Set<RxStorageInstance<any, any, any, any>>;
    canceled: boolean;
    broadcastChannel?: BroadcastChannel;
    heartbeatInterval?: ReturnType<typeof setInterval>;
    constructor(collection: RxCollection, migrationStrategies: NumberFunctionMap, statusDocKey?: string);
    getStatus(): Promise<RxMigrationStatus>;
    /**
     * Starts the migration.
     * Returns void so that people to not get the idea to await
     * this function.
     * Instead use migratePromise() if you want to await
     * the migration. This ensures it works even if the migration
     * is run on a different browser tab.
     */
    startMigration(batchSize?: number): Promise<void>;
    private runMigration;
    /**
     * Waits until this instance is the leader of the migration.
     * Returns 'STALLED' when the current leader did not write
     * the status document for longer than MIGRATION_LEADER_TIMING.timeout,
     * because then the leader is likely frozen, throttled or discarded
     * by the browser and this instance takes over the migration.
     * Returns 'DONE' when another instance has finished the migration.
     * @link https://github.com/pubkey/rxdb/issues/9191
     */
    awaitLeadershipOrStalledLeader(awaitLeadership: () => Promise<any>): Promise<'LEADER' | 'STALLED' | 'DONE'>;
    /**
     * Regularly rewrites the status document while this instance
     * runs the migration, so that other instances can see that
     * the leader is still alive.
     * Frozen or discarded browser tabs do not run timers,
     * so the heartbeat stops when the leader stops working.
     */
    startHeartbeat(): void;
    stopHeartbeat(): void;
    updateStatusHandlers: MigrationStatusUpdate[];
    updateStatusQueue: Promise<any>;
    updateStatus(handler: MigrationStatusUpdate): Promise<any>;
    migrateStorage(oldStorage: RxStorageInstance<any, any, any>, newStorage: RxStorageInstance<any, any, any>, batchSize: number): Promise<void>;
    /**
     * Stops the migration.
     * Mostly used in tests to simulate what happens
     * when the user reloads the page during a migration.
     */
    cancel(): Promise<void>;
    countAllDocuments(storageInstances: RxStorageInstance<any, any, any>[]): Promise<number>;
    getConnectedStorageInstances(): Promise<{
        oldStorage: RxStorageInstance<any, any, any>;
        newStorage: RxStorageInstance<any, any, any>;
    }[]>;
    migratePromise(batchSize?: number): Promise<RxMigrationStatus>;
}
