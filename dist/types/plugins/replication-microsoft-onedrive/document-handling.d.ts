import { ById } from '../../types/util';
import type { OneDriveState, OneDriveItem } from './microsoft-onedrive-types.js';
import { DriveStructure } from './init.js';
export { serializeDocAttachments, deserializeDocAttachments } from '../../replication-protocol/helper.js';
export declare function getDocumentFiles(oneDriveState: OneDriveState, init: DriveStructure, docIds: string[]): Promise<{
    files: OneDriveItem[];
}>;
export declare function insertDocumentFiles<RxDocType>(oneDriveState: OneDriveState, init: DriveStructure, primaryPath: keyof RxDocType, docs: RxDocType[]): Promise<void>;
export declare function updateDocumentFiles<DocType>(oneDriveState: OneDriveState, primaryPath: keyof DocType, docs: DocType[], fileIdByDocId: Record<string, string>, concurrency?: number): Promise<Record<string, {
    id: string;
}>>;
export declare function fetchDocumentContents<DocType>(oneDriveState: OneDriveState, fileIds: string[], concurrency?: number): Promise<{
    byId: ById<DocType>;
    ordered: (DocType)[];
}>;
