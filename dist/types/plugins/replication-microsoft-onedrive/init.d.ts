import type { OneDriveState } from './microsoft-onedrive-types.js';
export declare function initDriveStructure(oneDriveState: OneDriveState): Promise<{
    rootFolderId: string;
    docsFolderId: string;
    signalingFolderId: string;
    replicationIdentifier: string;
    rxdbJson: {
        status: number;
        etag: string;
        createdTime: string;
        fileId: string;
        size: number;
    };
    transactionFile: {
        status: number;
        etag: string;
        createdTime: string;
        fileId: string;
        size: number;
    };
    walFile: {
        status: number;
        etag: string;
        createdTime: string;
        fileId: string;
        size: number;
    };
}>;
export type DriveStructure = Awaited<ReturnType<typeof initDriveStructure>>;
