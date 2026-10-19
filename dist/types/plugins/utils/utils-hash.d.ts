import type { HashFunction } from '../../types/index.js';
export declare function nativeSha256(input: string | ArrayBuffer | Blob): Promise<string>;
export declare const defaultHashSha256: HashFunction;
export declare function hashStringToNumber(str: string): number;
