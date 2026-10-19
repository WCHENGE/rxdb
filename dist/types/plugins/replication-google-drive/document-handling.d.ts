import { ById } from '../../types/util';
import type { DriveFileListResponse, GoogleDriveOptionsWithDefaults } from './google-drive-types.js';
import { DriveStructure } from './init.js';
export { serializeDocAttachments, deserializeDocAttachments } from '../../replication-protocol/helper.js';
export declare function getDocumentFiles(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure, docIds: string[]): Promise<DriveFileListResponse>;
export declare function batchGetFilesMetadata(googleDriveOptions: GoogleDriveOptionsWithDefaults, fileIds: string[]): Promise<string>;
export declare function insertDocumentFiles<RxDocType>(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure, primaryPath: keyof RxDocType, docs: RxDocType[]): Promise<void>;
export declare function updateDocumentFiles<DocType>(googleDriveOptions: GoogleDriveOptionsWithDefaults, primaryPath: keyof DocType, docs: DocType[], fileMetaByDocId: Record<string, {
    fileId: string;
    etag: string;
}>, concurrency?: number): Promise<Record<string, {
    id: string;
}>>;
export declare function fetchDocumentContents<DocType>(googleDriveOptions: GoogleDriveOptionsWithDefaults, fileIds: string[], concurrency?: number): Promise<{
    byId: ById<DocType>;
    ordered: (DocType)[];
}>;
