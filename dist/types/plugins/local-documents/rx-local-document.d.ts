import type { LocalDocumentParent, RxCollection, RxDatabase, RxDocumentData, RxLocalDocument, RxLocalDocumentData } from '../../types/';
declare const RxDocumentParent: any;
declare class RxLocalDocumentClass<DocData = any> extends RxDocumentParent {
    readonly id: string;
    readonly parent: RxCollection | RxDatabase;
    constructor(id: string, jsonData: DocData, parent: RxCollection | RxDatabase);
}
export declare function createRxLocalDocument<DocData>(data: RxDocumentData<RxLocalDocumentData<DocData>>, parent: any): RxLocalDocument<DocData>;
export declare function getRxDatabaseFromLocalDocument(doc: RxLocalDocument<any> | RxLocalDocumentClass): RxDatabase<import("../../index.js").CollectionsOfDatabase, any, {}, unknown> | import("../../rx-database.js").RxDatabaseBase<any, any, any, any>;
export declare function createLocalDocStateByParent(parent: LocalDocumentParent): void;
export {};
