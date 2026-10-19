import type { PreparedQuery, RxStorageQueryResult } from '../../types/index.js';
import { RxStorageInstanceDenoKV } from "./rx-storage-instance-denokv.js";
export declare function queryDenoKV<RxDocType>(instance: RxStorageInstanceDenoKV<RxDocType>, preparedQuery: PreparedQuery<RxDocType>): Promise<RxStorageQueryResult<RxDocType>>;
