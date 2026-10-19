import type { RxStorage, RxStorageInstanceCreationParams } from '../../types/index';
import { LocalstorageInstanceCreationOptions, LocalstorageStorageInternals, LocalstorageStorageSettings, RxStorageInstanceLocalstorage } from './rx-storage-instance-localstorage.js';
export * from './localstorage-mock.js';
export declare class RxStorageLocalstorage implements RxStorage<LocalstorageStorageInternals, LocalstorageInstanceCreationOptions> {
    settings: LocalstorageStorageSettings;
    name: string;
    readonly rxdbVersion = "17.6.0";
    constructor(settings: LocalstorageStorageSettings);
    createStorageInstance<RxDocType>(params: RxStorageInstanceCreationParams<RxDocType, LocalstorageInstanceCreationOptions>): Promise<RxStorageInstanceLocalstorage<RxDocType>>;
}
export declare function getRxStorageLocalstorage(settings?: Partial<LocalstorageStorageSettings>): RxStorageLocalstorage;
