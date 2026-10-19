import type { OneDriveState, OneDriveItem } from './microsoft-onedrive-types.js';
/**
 * Returns the base URL for Graph API calls up to the drive root.
 * Examples:
 *   - "https://graph.microsoft.com/v1.0/me/drive" (personal / default)
 *   - "https://graph.microsoft.com/v1.0/drives/12345"
 */
export declare function getDriveBaseUrl(oneDriveState: OneDriveState): string;
export declare function createFolder(oneDriveState: OneDriveState, parentId: string | undefined, folderName: string): Promise<string>;
export declare function findFolder(oneDriveState: OneDriveState, parentId: string | undefined, folderName: string): Promise<string | undefined>;
export declare function ensureFolderExists(oneDriveState: OneDriveState, folderPath: string): Promise<string>;
export declare function createEmptyFile(oneDriveState: OneDriveState, parentId: string, fileName: string): Promise<{
    status: number;
    etag: string;
    createdTime: string;
    fileId: string;
    size: number;
}>;
export declare function fillFileIfEtagMatches<T = any>(oneDriveState: OneDriveState, fileId: string, etag: string, jsonContent?: any): Promise<{
    status: number;
    etag: string;
    content: T | undefined;
}>;
export declare function deleteIfEtagMatches(oneDriveState: OneDriveState, fileId: string, etag: string): Promise<void>;
export declare function deleteFile(oneDriveState: OneDriveState, fileId: string): Promise<void>;
export declare function readJsonFileContent<T>(oneDriveState: OneDriveState, fileId: string): Promise<{
    etag: string;
    content: T | undefined;
}>;
export declare function listFilesInFolder(oneDriveState: OneDriveState, folderId: string): Promise<OneDriveItem[]>;
