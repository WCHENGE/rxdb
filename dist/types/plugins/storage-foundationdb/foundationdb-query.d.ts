import type { PreparedQuery, RxStorageQueryResult } from '../../types/index.js';
import { RxStorageInstanceFoundationDB } from './rx-storage-instance-foundationdb.js';
export declare function queryFoundationDB<RxDocType>(instance: RxStorageInstanceFoundationDB<RxDocType>, preparedQuery: PreparedQuery<RxDocType>): Promise<RxStorageQueryResult<RxDocType>>;
