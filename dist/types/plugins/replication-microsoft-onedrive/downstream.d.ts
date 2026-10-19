import type { OneDriveItem, OneDriveCheckpointType, OneDriveState } from './microsoft-onedrive-types.js';
import { DriveStructure } from './init.js';
export declare function fetchChanges<DocType>(oneDriveState: OneDriveState, init: DriveStructure, checkpoint?: OneDriveCheckpointType, batchSize?: number): Promise<{
    checkpoint: OneDriveCheckpointType | undefined;
    documents: DocType[];
}>;
export declare function fetchChangesFiles(oneDriveState: OneDriveState, init: DriveStructure, checkpoint?: OneDriveCheckpointType, batchSize?: number): Promise<{
    checkpoint?: OneDriveCheckpointType;
    files: OneDriveItem[];
}>;
