import type { RxStorage, RxStorageInstanceCreationParams } from '../../types/index.js';
import type { DexieSettings, DexieStorageInternals } from '../../types/plugins/dexie.js';
import { RxStorageInstanceDexie } from './rx-storage-instance-dexie.js';
export declare class RxStorageDexie implements RxStorage<DexieStorageInternals, DexieSettings> {
    settings: DexieSettings;
    name: string;
    readonly rxdbVersion = "17.6.0";
    constructor(settings: DexieSettings);
    createStorageInstance<RxDocType>(params: RxStorageInstanceCreationParams<RxDocType, DexieSettings>): Promise<RxStorageInstanceDexie<RxDocType>>;
}
export declare function getRxStorageDexie(settings?: DexieSettings): RxStorageDexie;
