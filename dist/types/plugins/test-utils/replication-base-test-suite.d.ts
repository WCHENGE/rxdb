import type { RxCollection } from '../../types/index.js';
export type ReplicationBaseTestSuiteConfig = {
    /**
     * Start a live replication for a collection.
     * Must target a shared server endpoint so that multiple
     * collections can replicate to the same backend.
     * May return a Promise to support plugins where the replication factory is async.
     */
    startReplication(collection: RxCollection<any>): any;
    /**
     * Run a one-shot (non-live) sync and wait for completion.
     * Must target the same server endpoint as startReplication.
     */
    syncOnce(collection: RxCollection<any>): Promise<void>;
    /**
     * Get all documents from the server,
     * including soft-deleted ones when the backend uses soft deletes.
     */
    getAllServerDocs(): Promise<any[]>;
    /**
     * Remove all documents from the server.
     */
    cleanUpServer(): Promise<void>;
    /**
     * Whether the server uses soft deletes
     * (marks docs as deleted instead of removing them).
     */
    softDeletes?: boolean;
    /**
     * Check if a server document is marked as deleted.
     * Required when softDeletes is true.
     */
    isDeleted?(serverDoc: any): boolean;
    /**
     * Get the client primary key (passportId) from a server document.
     * Different backends store the primary key in different fields.
     */
    getPrimaryOfServerDoc(serverDoc: any): string;
    /**
     * Additional wait time for eventually consistent backends (ms).
     */
    waitTime?: number;
    /**
     * When provided, attachment replication tests are run.
     * The callback should perform a one-shot sync for a collection whose
     * schema has `attachments: {}` defined, targeting the same server endpoint.
     */
    syncOnceWithAttachments?(collection: RxCollection<any>): Promise<void>;
    /**
     * When provided, a test is run that verifies attachment binary data is NOT
     * replicated when attachments are disabled (e.g. `attachments: false`).
     * The callback should perform a one-shot sync for a collection whose schema
     * has `attachments: {}` defined, but with attachment replication turned off.
     */
    syncOnceWithAttachmentsDisabled?(collection: RxCollection<any>): Promise<void>;
};
/**
 * Runs the base test suite for a replication plugin.
 * Call this inside a describe() block in your replication test file.
 *
 * The config callbacks should target a shared server endpoint.
 * Each test calls cleanUpServer() at the start to ensure a clean state.
 *
 * @example
 * ```ts
 * describe('replication-nats.test.js', () => {
 *     runReplicationBaseTestSuite({
 *         startReplication: (collection) => syncNats(collection, natsName),
 *         syncOnce: (collection) => syncOnceNats(collection, natsName),
 *         getAllServerDocs: () => getAllDocsOfServer(natsName),
 *         cleanUpServer: async () => { ... },
 *         softDeletes: true,
 *         isDeleted: (doc) => doc._deleted,
 *         getPrimaryOfServerDoc: (doc) => doc.passportId,
 *     });
 * });
 * ```
 */
export declare function runReplicationBaseTestSuite(config: ReplicationBaseTestSuiteConfig): void;
