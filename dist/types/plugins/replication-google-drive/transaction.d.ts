import type { GoogleDriveOptionsWithDefaults } from './google-drive-types.js';
import { DriveStructure } from './init.js';
export type DriveTransaction = {
    etag: string;
};
export type TransactionFileContent = {
    createdAtClientTime: number;
};
export declare const TRANSACTION_FILE_NAME = "transaction.json";
export declare const TRANSACTION_BLOCKED_FLAG: {
    readonly retry: true;
};
export declare function startTransactionTryOnce(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure): Promise<typeof TRANSACTION_BLOCKED_FLAG | DriveTransaction>;
export declare function startTransaction(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure): Promise<DriveTransaction>;
export declare function isTransactionTimedOut(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure): Promise<{
    timeLeft: number;
    transactionAge: number;
    expired: boolean;
    etag: string;
}>;
export declare function commitTransaction(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure, transactionInput: DriveTransaction | typeof TRANSACTION_BLOCKED_FLAG): Promise<void>;
export declare function runInTransaction<T>(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure, primaryPath: string, fn: () => Promise<T>, runAfter?: () => any): Promise<T>;
