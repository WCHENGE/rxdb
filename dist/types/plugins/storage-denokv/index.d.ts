import type { RxStorage, RxStorageInstanceCreationParams } from '../../types/index.js';
import type { DenoKVSettings, DenoKVStorageInternals } from './denokv-types.js';
import { RxStorageInstanceDenoKV } from "./rx-storage-instance-denokv.js";
export declare class RxStorageDenoKV implements RxStorage<DenoKVStorageInternals<any>, DenoKVSettings> {
    settings: DenoKVSettings;
    name: string;
    readonly rxdbVersion = "17.6.0";
    constructor(settings: DenoKVSettings);
    createStorageInstance<RxDocType>(params: RxStorageInstanceCreationParams<RxDocType, DenoKVSettings>): Promise<RxStorageInstanceDenoKV<RxDocType>>;
}
export declare function getRxStorageDenoKV(settings?: DenoKVSettings): RxStorageDenoKV;
