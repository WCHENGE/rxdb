import { Signal, Injector } from '@angular/core';
import type { RxReactivityFactory, ReactivityLambda } from '../../types';
/**
 * Type-level function (ReactivityLambda) for Angular signals.
 * Use this as the Reactivity type parameter for properly typed signals.
 *
 * @example
 * const db = await createRxDatabase<MyCollections, any, any, AngularSignalReactivityLambda>({
 *     reactivity: createReactivityFactory(injector)
 * });
 * const signal = doc.age$$; // Signal<number>
 */
export interface AngularSignalReactivityLambda extends ReactivityLambda {
    readonly _result: Signal<this['_data']>;
}
export declare function createReactivityFactory(injector: Injector): RxReactivityFactory<AngularSignalReactivityLambda>;
