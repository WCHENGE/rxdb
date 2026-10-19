/**
 * Optimized binary search functions for the memory storage.
 * Based on https://github.com/mikolalysenko/binary-search-bounds
 * but with performance improvements:
 * - Removed unnecessary undefined checks for comparator
 * - Inlined the norm() wrapper to avoid extra function calls
 * - Added string-specialized variants to avoid temporary array allocations
 */
type Compare<T> = ((a: T, b: T) => number | null | undefined);
export declare function boundGE<T>(a: T[], y: T, c: Compare<T>, lo?: any, hi?: any): number;
export declare function boundGT<T>(a: T[], y: T, c: Compare<T>, lo?: any, hi?: any): number;
export declare function boundLT<T>(a: T[], y: T, c: Compare<T>, lo?: any, hi?: any): number;
export declare function boundLE<T>(a: T[], y: T, c: Compare<T>, lo?: any, hi?: any): number;
export declare function boundEQ<T>(a: T[], y: T, c: Compare<T>, lo?: any, hi?: any): number;
/**
 * Specialized binary search functions that compare DocWithIndexString
 * entries directly against an index string, avoiding temporary array allocations.
 * Used in query() and count() hot paths.
 */
export declare function boundGEByIndexString<T extends [string, ...any[]]>(a: T[], indexString: string): number;
export declare function boundGTByIndexString<T extends [string, ...any[]]>(a: T[], indexString: string): number;
export declare function boundEQByIndexString<T extends [string, ...any[]]>(a: T[], indexString: string): number;
export declare function boundLTByIndexString<T extends [string, ...any[]]>(a: T[], indexString: string): number;
export declare function boundLEByIndexString<T extends [string, ...any[]]>(a: T[], indexString: string): number;
export {};
