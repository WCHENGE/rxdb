import type { DriveFileMetadata, GoogleDriveCheckpointType, GoogleDriveOptionsWithDefaults } from './google-drive-types';
import { DriveStructure } from './init.js';
export declare function fetchChanges<DocType>(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure, checkpoint?: GoogleDriveCheckpointType, batchSize?: number): Promise<{
    checkpoint: GoogleDriveCheckpointType | undefined;
    documents: DocType[];
}>;
export declare function fetchChangesFiles(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure, checkpoint?: GoogleDriveCheckpointType, batchSize?: number): Promise<{
    checkpoint?: GoogleDriveCheckpointType;
    files: DriveFileMetadata[];
}>;
