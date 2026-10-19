import type { FilledMangoQuery, MangoQuerySelector, RxDocumentData, RxJsonSchema, RxQueryPlan, RxQueryPlanKey, RxQueryPlanerOpts } from './types/index.js';
export declare const INDEX_MAX: string;
/**
 * Do not use -Infinity here because it would be
 * transformed to null on JSON.stringify() which can break things
 * when the query plan is sent to the storage as json.
 * @link https://stackoverflow.com/a/16644751
 * Notice that for IndexedDB IDBKeyRange we have
 * to transform the value back to -Infinity
 * before we can use it in IDBKeyRange.bound.
 */
export declare const INDEX_MIN: number;
/**
 * Returns the query plan which contains
 * information about how to run the query
 * and which indexes to use.
 *
 * This is used in some storage like Memory, dexie.js and IndexedDB.
 */
export declare function getQueryPlan<RxDocType>(schema: RxJsonSchema<RxDocumentData<RxDocType>>, query: FilledMangoQuery<RxDocType>): RxQueryPlan;
export declare const LOGICAL_OPERATORS: Set<string>;
export declare const LOWER_BOUND_LOGICAL_OPERATORS: Set<string>;
export declare const UPPER_BOUND_LOGICAL_OPERATORS: Set<string>;
export declare function isSelectorSatisfiedByIndex(index: string[], selector: MangoQuerySelector<any>, startKeys: RxQueryPlanKey[], endKeys: RxQueryPlanKey[]): boolean;
/**
 * $in can use an index by scanning the range between the
 * smallest and the largest of the given values.
 * That range can contain non-matching documents, so the
 * selector is never satisfied by the index alone and the
 * query matcher must still filter the results.
 * Only homogeneous arrays of strings or finite numbers are
 * used because min/max is not defined across mixed types.
 * @link https://github.com/pubkey/rxdb/issues/8631
 */
export declare function getInQueryRangeOpts(operatorValue: any): Partial<RxQueryPlanerOpts> | undefined;
export declare function getMatcherQueryOpts(operator: string, operatorValue: any): Partial<RxQueryPlanerOpts>;
/**
 * Returns a number that determines the quality of the query plan.
 * Higher number means better query plan.
 */
export declare function rateQueryPlan<RxDocType>(schema: RxJsonSchema<RxDocumentData<RxDocType>>, query: FilledMangoQuery<RxDocType>, queryPlan: RxQueryPlan): number;
