import type { GoogleDriveOptionsWithDefaults, DriveFileMetadata } from './google-drive-types.js';
export declare const DRIVE_API_VERSION = "v3";
export declare const DRIVE_MAX_PAGE_SIZE = 1000;
export declare const DRIVE_MAX_BULK_SIZE: number;
export declare const FOLDER_MIME_TYPE = "application/vnd.google-apps.folder";
/**
 * The id of the top level parent for the configured space.
 * For 'appDataFolder' this is the literal 'appDataFolder' which Google Drive
 * accepts as a parent id and in queries. For 'drive' it is the regular 'root'.
 */
export declare function driveRootParentId(googleDriveOptions: GoogleDriveOptionsWithDefaults): string;
/**
 * files.list only returns appDataFolder contents when spaces=appDataFolder
 * is set, even when querying by an explicit parent id.
 */
export declare function applyDriveSpace(googleDriveOptions: GoogleDriveOptionsWithDefaults, params: URLSearchParams): void;
/**
 * Same as applyDriveSpace() but for URLs that are built via string concatenation.
 * Returns an empty string for the default 'drive' space.
 */
export declare function driveSpaceQuerySuffix(googleDriveOptions: GoogleDriveOptionsWithDefaults): string;
export declare function createFolder(googleDriveOptions: GoogleDriveOptionsWithDefaults, parentId: string | undefined, folderName: string): Promise<string>;
export declare function findFolder(googleDriveOptions: GoogleDriveOptionsWithDefaults, parentId: string | undefined, folderName: string): Promise<string | undefined>;
export declare function ensureFolderExists(googleDriveOptions: GoogleDriveOptionsWithDefaults, folderPath: string): Promise<string>;
export declare function createEmptyFile(googleDriveOptions: GoogleDriveOptionsWithDefaults, parentId: string, fileName: string): Promise<{
    status: number;
    etag: string;
    createdTime: string;
    fileId: any;
    size: number;
}>;
export declare function getFileEtag(googleDriveOptions: GoogleDriveOptionsWithDefaults, fileId: string): Promise<string>;
export declare function fillFileIfEtagMatches<T = any>(googleDriveOptions: GoogleDriveOptionsWithDefaults, fileId: string, etag: string, jsonContent?: any): Promise<{
    status: number;
    etag: string;
    content: T | undefined;
    serverTime: number;
}>;
export declare function deleteIfEtagMatches(googleDriveOptions: GoogleDriveOptionsWithDefaults, fileId: string, etag: string): Promise<void>;
export declare function deleteFile(googleDriveOptions: GoogleDriveOptionsWithDefaults, fileId: string): Promise<void>;
export declare function readJsonFileContent<T>(googleDriveOptions: GoogleDriveOptionsWithDefaults, fileId: string): Promise<{
    etag: string;
    content: T | undefined;
    serverTime: number;
}>;
export declare function readFolder(googleDriveOptions: GoogleDriveOptionsWithDefaults, folderPath: string): Promise<DriveFileMetadata[]>;
export declare function insertMultipartFile<T>(googleDriveOptions: GoogleDriveOptionsWithDefaults, folderId: string, filename: string, jsonData: T): Promise<void>;
export declare function createMultipartBody(metadata: Record<string, unknown>, content: string): {
    body: string;
    boundary: string;
};
export declare function listFilesInFolder(googleDriveOptions: GoogleDriveOptionsWithDefaults, folderId: string): Promise<DriveFileMetadata[]>;
