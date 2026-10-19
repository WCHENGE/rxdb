import type { OneDriveState } from './microsoft-onedrive-types.js';
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
export declare function startTransactionTryOnce(oneDriveState: OneDriveState, init: DriveStructure): Promise<typeof TRANSACTION_BLOCKED_FLAG | DriveTransaction>;
export declare function startTransaction(oneDriveState: OneDriveState, init: DriveStructure): Promise<DriveTransaction>;
export declare function isTransactionTimedOut(oneDriveState: OneDriveState, init: DriveStructure): Promise<{
    timeLeft: number;
    transactionAge: number;
    expired: boolean;
    etag: any;
}>;
export declare function commitTransaction(oneDriveState: OneDriveState, init: DriveStructure, transactionInput: DriveTransaction | typeof TRANSACTION_BLOCKED_FLAG): Promise<void>;
export declare function runInTransaction<T>(oneDriveState: OneDriveState, init: DriveStructure, primaryPath: string, fn: () => Promise<T>, runAfter?: () => any): Promise<T>;
