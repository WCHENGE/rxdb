import type { GoogleDriveOptionsWithDefaults } from './google-drive-types.js';
export declare function initDriveStructure(googleDriveOptions: GoogleDriveOptionsWithDefaults): Promise<{
    rootFolderId: string;
    docsFolderId: string;
    signalingFolderId: string;
    replicationIdentifier: string;
    rxdbJson: {
        status: number;
        etag: string;
        createdTime: string;
        fileId: any;
        size: number;
    };
    transactionFile: {
        status: number;
        etag: string;
        createdTime: string;
        fileId: any;
        size: number;
    };
    walFile: {
        status: number;
        etag: string;
        createdTime: string;
        fileId: any;
        size: number;
    };
}>;
export type DriveStructure = Awaited<ReturnType<typeof initDriveStructure>>;
