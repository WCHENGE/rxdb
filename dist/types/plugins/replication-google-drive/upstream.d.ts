import { RxReplicationWriteToMasterRow, WithDeletedAndAttachments } from '../../index.js';
import type { GoogleDriveOptionsWithDefaults } from './google-drive-types';
import { DriveStructure } from './init.js';
export declare const WAL_FILE_NAME = "rxdb-wal.json";
export declare function fetchConflicts<RxDocType>(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure, primaryPath: keyof WithDeletedAndAttachments<RxDocType>, writeRows: RxReplicationWriteToMasterRow<RxDocType>[]): Promise<{
    conflicts: WithDeletedAndAttachments<RxDocType>[];
    nonConflicts: RxReplicationWriteToMasterRow<RxDocType>[];
}>;
export declare function writeToWal<RxDocType>(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure, writeRows?: RxReplicationWriteToMasterRow<RxDocType>[]): Promise<void>;
export declare function readWalContent<RxDocType>(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure): Promise<{
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
export declare function processWalFile<RxDocType>(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure, primaryPath: keyof RxDocType): Promise<void>;
export declare function handleUpstreamBatch<RxDocType>(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure, primaryPath: keyof WithDeletedAndAttachments<RxDocType>, writeRows: RxReplicationWriteToMasterRow<RxDocType>[]): Promise<WithDeletedAndAttachments<RxDocType>[]>;
