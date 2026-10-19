import type { RxDatabase, RxPlugin, RxState } from '../../types/index.js';
export * from './helpers.js';
export declare function addState<T>(this: RxDatabase, namespace?: string): Promise<RxState<T>>;
export declare const RxDBStatePlugin: RxPlugin;
