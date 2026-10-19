import { Signal } from "@preact/signals-core";
import type { Subscription } from 'rxjs';
import type { RxReactivityFactory, ReactivityLambda } from '../../types/index.js';
/**
 * Type-level function (ReactivityLambda) for Preact signals.
 * Use this as the Reactivity type parameter for properly typed signals.
 *
 * @example
 * const db = await createRxDatabase<MyCollections, any, any, PreactSignalReactivityLambda>({
 *     reactivity: PreactSignalsRxReactivityFactory
 * });
 * const signal = doc.age$$; // Signal<number>
 */
export interface PreactSignalReactivityLambda extends ReactivityLambda {
    readonly _result: Signal<this['_data']>;
}
export type PreactSignal<T = any> = Signal<T>;
/**
 * Exported to debug and test
 * the behavior
 */
export declare const PREACT_SIGNAL_STATE: {
    subscribeCount: number;
    signalBySubscription: WeakMap<Subscription, PreactSignal<any>>;
    aliveSubscription: Set<Subscription>;
};
export declare const PreactSignalsRxReactivityFactory: RxReactivityFactory<PreactSignalReactivityLambda>;
