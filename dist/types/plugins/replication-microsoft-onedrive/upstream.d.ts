import { RxReplicationWriteToMasterRow, WithDeletedAndAttachments } from '../../index.js';
import type { OneDriveState } from './microsoft-onedrive-types.js';
import { DriveStructure } from './init.js';
export declare const WAL_FILE_NAME = "rxdb-wal.json";
export declare const DRIVE_MAX_BULK_SIZE = 250;
export declare function fetchConflicts<RxDocType>(oneDriveState: OneDriveState, init: DriveStructure, primaryPath: keyof WithDeletedAndAttachments<RxDocType>, writeRows: RxReplicationWriteToMasterRow<RxDocType>[]): Promise<{
    conflicts: WithDeletedAndAttachments<RxDocType>[];
    nonConflicts: RxReplicationWriteToMasterRow<RxDocType>[];
}>;
export declare function writeToWal<RxDocType>(oneDriveState: OneDriveState, init: DriveStructure, writeRows?: RxReplicationWriteToMasterRow<RxDocType>[]): Promise<void>;
export declare function readWalContent<RxDocType>(oneDriveState: OneDriveState, init: DriveStructure): Promise<{
    etag: string;
    rows: RxReplicationWriteToMasterRow<RxDocType>[] | undefined;
}>;
/**
 * Here we read the WAL file content
 * and sort the content into the actual
 * document files.
 * Notice that when the JavaScript process
 * exists at any point here, we need to have
 * a recoverable state on the next run. So this
 * must be idempotent.
 */
export declare function processWalFile<RxDocType>(oneDriveState: OneDriveState, init: DriveStructure, primaryPath: keyof RxDocType): Promise<void>;
export declare function handleUpstreamBatch<RxDocType>(oneDriveState: OneDriveState, init: DriveStructure, primaryPath: keyof WithDeletedAndAttachments<RxDocType>, writeRows: RxReplicationWriteToMasterRow<RxDocType>[]): Promise<WithDeletedAndAttachments<RxDocType>[]>;
