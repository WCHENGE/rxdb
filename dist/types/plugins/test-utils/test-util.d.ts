import type { Func } from 'mocha';
import type { RxCollection } from '../../types';
import type { RxReplicationState } from '../replication/index.js';
export declare function testMultipleTimes(times: number, title: string, test: Func): void;
export declare function ensureCollectionsHaveEqualState<RxDocType>(c1: RxCollection<RxDocType>, c2: RxCollection<RxDocType>, logContext?: string): Promise<void>;
/**
 * Waits until the collections have the equal state.
 */
export declare function awaitCollectionsHaveEqualState<RxDocType>(c1: RxCollection<RxDocType>, c2: RxCollection<RxDocType>, logContext?: string, timeout?: number): Promise<void>;
/**
 * Deletes all locally stored IndexedDB databases.
 * Noop if IndexedDB is not available (e.g. in Node.js)
 * or if the .databases() method is not supported.
 */
export declare function clearAllLocalIndexedDB(): Promise<void>;
/**
 * Deletes all files and directories stored in the
 * Origin Private File System (OPFS).
 * Noop if OPFS is not available (e.g. in Node.js).
 */
export declare function clearAllLocalOPFS(maxRetries?: number, delayMs?: number): Promise<void>;
/**
 * Clears all localStorage data.
 * Noop if localStorage is not available (e.g. in Node.js).
 */
export declare function clearAllLocalStorage(): Promise<void>;
export declare function ensureReplicationHasNoErrors(replicationState: RxReplicationState<any, any>): void;
