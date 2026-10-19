import { RxStorage } from '../../index.js';
export type PerformanceTestConfig = {
    /**
     * How many times the test loop is run.
     * More runs give more stable averages but take longer.
     * @default 40
     */
    runs?: number;
    /**
     * Number of collections created per database.
     * @default 4
     */
    collectionsAmount?: number;
    /**
     * Number of documents inserted in bulk per run.
     * @default 3000
     */
    docsAmount?: number;
    /**
     * Number of documents inserted one-by-one (serial) per run.
     * @default 50
     */
    serialDocsAmount?: number;
    /**
     * Number of parallel queries executed per run.
     * @default 4
     */
    parallelQueryAmount?: number;
    /**
     * Number of batches used when doing bulk inserts.
     * @default 6
     */
    insertBatches?: number;
    /**
     * Milliseconds to wait between test operations.
     * Set to 0 to disable waiting (useful for smoke tests).
     * @default 100
     */
    waitBetweenTests?: number;
    /**
     * Whether to log progress and results to the console.
     * @default true
     */
    log?: boolean;
    /**
     * If set, the database will be created with encryption
     * and the schema will mark applicable fields as encrypted.
     */
    password?: any;
    /**
     * Whether to run the bulk find-by-ids test.
     * @default true
     */
    testBulkFindByIds?: boolean;
    /**
     * Whether to run the serial find-by-id test.
     * @default true
     */
    testSerialFindById?: boolean;
    /**
     * Whether to run the find-by-query test.
     * @default true
     */
    testFindByQuery?: boolean;
    /**
     * Whether to run the find-by-query-parallel test.
     * @default true
     */
    testFindByQueryParallel?: boolean;
    /**
     * Whether to run the count query test.
     * @default true
     */
    testCount?: boolean;
    /**
     * Whether to run the property access test.
     * Requires testFindByQuery to also be enabled.
     * @default true
     */
    testPropertyAccess?: boolean;
};
export type PerformanceTestResult = {
    description: string;
    collectionsAmount: number;
    docsAmount: number;
    [timingKey: string]: number | string;
};
/**
 * Runs a performance benchmark against the given RxStorage.
 * Useful for comparing different RxStorage implementations.
 *
 * @param storage - The RxStorage to benchmark.
 * @param storageDescription - A human-readable description of the storage (used in results).
 * @param config - Optional configuration to override the defaults.
 * @returns An object with averaged timing values for each measured operation.
 */
export declare function runPerformanceTests(storage: RxStorage<any, any>, storageDescription: string, config?: PerformanceTestConfig): Promise<PerformanceTestResult>;
export declare function averageOfTimeValues(times: number[], 
/**
 * To better account for anomalies
 * during time measurements,
 * we strip the highest x percent.
 */
striphighestXPercent: number): number;
